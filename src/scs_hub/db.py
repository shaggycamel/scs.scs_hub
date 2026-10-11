import logging
import configparser
import re
import sqlalchemy
import polars as pl
from collections import defaultdict

from scs_hub.config import credentials_path, ensure_credentials_file

logger = logging.getLogger(__name__)

TIMEOUT = 3 * 60  # connection timeout, seconds

# Schemas carrying readable data. Read-only roles are granted SELECT on these,
# never on Cockroach's own crdb_internal/pg_* schemas.
READ_SCHEMAS = ("nba", "fty", "statyx", "util")

# Identifiers are interpolated into GRANT/DDL, which cannot take bind
# parameters, so they are validated rather than quoted.
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

class Database:
    """
    Owns connection credentials and a single SQLAlchemy engine for one
    database platform (identified by a section in credentials.ini).
    Every component (NBA, fty, statyx) reads/writes through this one
    object rather than opening its own connections.
    """

    def __init__(self, db_con: str | None = None, ini_path: str | None = None):
        self.ini_path = credentials_path(ini_path)
        self.engine: sqlalchemy.Engine | None = None
        if db_con:
            self.connect(db_con)
        else:
            msg = "Database: no connection specified — call db.connect(db_con) to connect"
            logger.warning(msg)
            self.engine = None
            

    def connect(self, db_con: str) -> None:
        """Build and store the engine for a credentials.ini section."""
        ensure_credentials_file(self.ini_path)
        parser = configparser.ConfigParser()
        parser.read(self.ini_path)
        if not parser.has_section(db_con):
            raise ValueError(
                f"No [{db_con}] section in {self.ini_path} "
                f"(found: {parser.sections()})"
            )

        db_creds = dict(parser.items(db_con))
        sql_url = "dialect://user:password@host:port/database"
        for key, value in db_creds.items():
            sql_url = sql_url.replace(key, value)

        self.engine = sqlalchemy.create_engine(sql_url, connect_args={"connect_timeout": TIMEOUT})

    def read(self, query: str, **kwargs) -> pl.DataFrame:
        """Run a SQL query, get a Polars DataFrame back.
        Extra kwargs (e.g. schema_overrides) pass through to pl.read_database."""
        return pl.read_database(query, self.engine, infer_schema_length=None, **kwargs)

    def write(
        self,
        df: pl.DataFrame,
        table: str,
        schema: str,
        if_exists: str = "append",
    ) -> None:
        """Write a Polars DataFrame to a table."""
        df.write_database(f"{schema}.{table}", self.engine, if_table_exists=if_exists)

    def conform(self, df: pl.DataFrame, table: str, schema: str) -> pl.DataFrame:
        """
        Shape a frame to a registration in util.table_column_order: rename source
        columns via origin_name, then select in column_order.

        Rows with a null origin_name are left alone, which covers columns derived
        in code rather than read from the source (nba box scores build player_name
        from two source fields). An unregistered table is returned untouched, so a
        new table can land before its ordering has been curated.
        """
        reg = self.read(
            "SELECT column_name, origin_name FROM util.table_column_order "
            f"WHERE schema = '{schema}' AND table_name = '{table}' "
            "ORDER BY column_order",
        )

        if reg.is_empty():
            logger.info(
                "%s.%s has no util.table_column_order rows — keeping all %d columns as-is",
                schema, table, df.width,
            )
            return df

        mapped = reg.drop_nulls()
        renames = dict(zip(mapped["origin_name"], mapped["column_name"]))
        return df.rename(renames).select(reg["column_name"].to_list())

    def write_ordered(self, df: pl.DataFrame, table: str, schema: str, order_key: str | None = None) -> None:
        """
        Conform a frame to its registration, then write to <schema>.<table>.

        order_key overrides which registration to read when one table is fed by
        more than one method (nba.league_game_schedule has a /past and a /future
        column set), and otherwise defaults to the table's own name.
        """
        df = self.conform(df, order_key or table, schema)
        self.write(df, table, schema=schema)
        logger.info("%s.%s has been updated (%d rows)", schema, table, len(df))

    def delete_where(self, table: str, schema: str, where: str) -> None:
        """
        Delete matching rows, skipping when the table doesn't exist yet.

        The replace-then-write methods issue their delete before the first write
        has created the table, so on a fresh database there is nothing to clear.
        """
        if self.read(f"SELECT to_regclass('{schema}.{table}') AS t")[0, 0] is None:
            return
        self.execute(f"DELETE FROM {schema}.{table} WHERE {where}")

    def execute(self, statement: str) -> None:
        """Run a non-SELECT statement (e.g. DELETE) and commit."""
        with self.engine.connect() as conn:
            conn.execute(sqlalchemy.sql.text(statement))
            conn.commit()

    def regrant_read(
        self,
        role: str | list[str] | tuple[str, ...] = "kobe_public",
        schemas: tuple[str, ...] = READ_SCHEMAS,
    ) -> list[str]:
        """
        Re-grant SELECT to one or more read-only roles on every table and view in
        the data schemas. role is a name or a list/tuple of names, granted in a
        single statement (GRANT ... TO a, b).

        Dropping and recreating a view or table discards the grants attached to
        it, so a role such as kobe_public (the Cockroach read-only user) loses
        access to exactly the objects that were rebuilt. This sweeps the schemas
        and grants SELECT again; it is idempotent, so it is safe to run after any
        rebuild.

        Base tables and views are both enumerated from information_schema.tables
        rather than trusting GRANT ... ON ALL TABLES IN SCHEMA, which Cockroach's
        docs describe as covering tables and sequences and do not promise views.
        Schemas absent from this connection (e.g. Cockroach has no fty) are
        skipped and logged. Returns the schemas that were granted.
        """
        roles = (role,) if isinstance(role, str) else tuple(role)
        if not roles:
            raise ValueError("regrant_read: role must be a name or a non-empty list/tuple")
        for ident in (*roles, *schemas):
            if not _IDENT.match(ident):
                raise ValueError(f"regrant_read: unsafe identifier {ident!r}")
        role_list = ", ".join(roles)

        objects = self.read(
            "SELECT table_schema, table_name FROM information_schema.tables "
            f"WHERE table_schema IN ({', '.join(repr(s) for s in schemas)}) "
            "ORDER BY table_schema, table_name"
        )

        by_schema: dict[str, list[str]] = defaultdict(list)
        for r in objects.iter_rows(named=True):
            by_schema[r["table_schema"]].append(r["table_name"])

        granted = []
        for schema in schemas:
            names = by_schema.get(schema)
            if not names:
                logger.warning("regrant_read: no tables/views in %s — skipped", schema)
                continue
            targets = ", ".join(f'"{schema}"."{name}"' for name in names)
            self.execute(f"GRANT SELECT ON TABLE {targets} TO {role_list}")
            granted.append(schema)
            logger.info(
                "regrant_read: SELECT on %d object(s) in %s granted to %s",
                len(names), schema, role_list,
            )

        return granted

    def deduplicate_tables(self, season: str, dry_run: bool = False) -> list[str]:
        """
        Remove exact duplicate rows, one season at a time, from every table on
        this connection that has a season column.

        Schema-agnostic maintenance rather than a util.* concern: it rewrites any
        table carrying a season column, whatever its schema, which is why it lives
        on the connection rather than a domain component. season is required — the
        Database has no Context — so callers pass ctx.cur_season.

        Scoped to a season because that is the only partition every candidate
        table shares, and it keeps each rewrite small: the method reads one
        season, drops duplicate rows in Polars, then replaces just that season
        inside a transaction, so a failure leaves the table as it was.

        Only tables carrying a season column are considered, so anything keyed
        differently — util.player, util.player_source_id, util.table_column_order
        — is skipped rather than silently rewritten. Views and _RETIRED tables are
        excluded by name.

        Beware what "duplicate" means here: it is an exact match across every
        column, and the table is rewritten from the deduplicated frame. A table
        where identical rows are legitimately distinct records would lose data, so
        dry_run=True first — it logs and returns what would change without
        writing anything.

        Returns the list of tables affected (or that would be, under dry_run).
        """
        tables = self.read(
            "SELECT DISTINCT table_schema, table_name "
            "FROM information_schema.columns "
            "WHERE column_name = 'season' "
            "  AND table_name NOT LIKE '%\\_vw' ESCAPE '\\' "
            "  AND table_name NOT ILIKE '%\\_retired' ESCAPE '\\'"
        )
        names = sorted(
            f"{r['table_schema']}.{r['table_name']}" for r in tables.iter_rows(named=True)
        )
        logger.info(
            "deduplicate_tables: %d candidate table(s) for season %s%s",
            len(names), season, " (dry run)" if dry_run else "",
        )

        changed = []
        for table in names:
            df = self.read(f"SELECT * FROM {table} WHERE season = '{season}'")

            # maintain_order so a rewrite is deterministic rather than reordering
            # the season's rows on every run.
            deduped = df.unique(maintain_order=True)
            if deduped.height == df.height:
                continue

            changed.append(table)
            logger.info(
                "%s: %d row(s) -> %d, dropping %d duplicate(s)%s",
                table, df.height, deduped.height, df.height - deduped.height,
                " (dry run, not written)" if dry_run else "",
            )
            if dry_run:
                continue

            # One transaction: the delete and the rewrite either both land or
            # neither does, so an interrupted run cannot leave a season empty.
            with self.engine.begin() as conn:
                conn.execute(
                    sqlalchemy.sql.text(f"DELETE FROM {table} WHERE season = '{season}'")
                )
                deduped.write_database(table, conn, if_table_exists="append")

        if not changed:
            logger.info("deduplicate_tables: no duplicates found")
        return changed
