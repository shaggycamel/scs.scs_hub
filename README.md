# scs-hub

Shared data and database layer for the SCS NBA platform. Every scheduled job and
model repo reads and writes through this package; it owns the connection, the
season context, and the source-specific components.

## Components

`SportsHub` composes:

- `hub.db` — `Database`: one SQLAlchemy engine, plus Polars `read` / `write`,
  column-order conformance, view grants and Postgres ⇄ Cockroach transport.
- `hub.ctx` — `Context`: current season and active player ids.
- `hub.nba` — official NBA stats and injuries.
- `hub.statyx` — Statyx API data through `statyx_client`.
- `hub.fty` — ESPN and Yahoo fantasy leagues through `fty_handlers`.
- `hub.util` — player identity and cross-domain reference data.

## Install

```bash
uv sync
```

Or install the package from GitHub:

```bash
pip install "scs-hub @ git+https://github.com/shaggycamel/scs.scs_hub.git"
```

`psycopg2` needs `libpq`; `nbainjuries` starts a JVM at import time, so a
headless JRE is required wherever `scs_hub.nba` is imported.

## Use

```python
from scs_hub import SportsHub

hub = SportsHub("cockroach-read")
box = hub.db.read("SELECT * FROM nba.player_box_score_vw LIMIT 10")
```

Credentials resolve from `SCS_HUB_CREDENTIALS` or
`~/.config/scs_hub_credentials.ini`. The file holds the database sections
(`[postgres]`, `[cockroach-read]`, `[cockroach-write]`) and the `[statyx]`,
`[espn_api]` and `[yahoo_api]` sections. Never commit it.

## Development

```bash
uv sync
uv run --with ruff ruff check .
```

`dev/` holds hand-applied SQL and working notes for the identity pipeline and
the `fty_dev` experiments. `known_failures.md` records runtime failures and
their fixes; add an entry after a scheduled-run repair.
