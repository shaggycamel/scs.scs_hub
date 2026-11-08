import datetime as dt
from collections.abc import Callable

import polars as pl
import polars.selectors as cs


def merge_subdicts(*fields: str) -> Callable[[dict], list[dict]]:
    """
    Build a flatten for responses that park a flat stats dict under a wrapper key
    (e.g. NFL advanced-stats {..., stats: {...}}). Splices each named sub-dict
    into the parent and drops the wrapper — one row in, one row out.

    None of the wrapped keys collide with their parent row's, so a plain merge is
    safe. Unnesting is what keeps these loadable: left as dicts, Polars reads them
    as struct columns and locks each field's dtype from the first row, so a later
    row whose value lands on a whole number fails to build.
    """

    def flatten(row: dict) -> list[dict]:
        out = {k: v for k, v in row.items() if k not in fields}
        for field in fields:
            sub = row.get(field)
            if sub:
                out.update(sub)
        return [out]

    return flatten


def flatten_distributions(*fields: str) -> Callable[[dict], list[dict]]:
    """
    Build a flatten for responses carrying probability maps whose *keys are data*
    (game-sim's score_distribution_json {"4-3": 0.06}, inning_distribution_json
    {"1": 0.12}). Merging those up would add a column per scoreline, so expand to
    long form instead: one row per bucket, tagged by which distribution it came
    from. Tagging also avoids a cross product when a row carries several maps.
    """

    def flatten(row: dict) -> list[dict]:
        base = {k: v for k, v in row.items() if k not in fields}
        results = []
        for field in fields:
            for bucket, probability in (row.get(field) or {}).items():
                results.append(
                    {
                        **base,
                        "distribution": field.removesuffix("_json").removesuffix(
                            "_distribution"
                        ),
                        "bucket": bucket,
                        "probability": probability,
                    }
                )
        return results

    return flatten


def flatten_team_shot_locations(row: dict) -> list[dict]:
    """
    Three levels deep: stats.shot_zones.{zone}.{fgm, fga, fg_pct}, where the zone
    name is data rather than a field. Expand to one row per zone.
    """
    base = {k: v for k, v in row.items() if k != "stats"}
    zones = (row.get("stats") or {}).get("shot_zones") or {}
    return [{**base, "shot_zone": zone, **stats} for zone, stats in zones.items()]


def flatten_shotmap(row: dict) -> list[dict]:
    """
    shotmap responses nest an events_data array, one entry per shot attempt.
    Expand to one row per event; a player with no shots contributes no rows.
    """
    base = {k: v for k, v in row.items() if k != "events_data"}
    return [{**base, **event} for event in (row.get("events_data") or [])]


def flatten_usage_shock(row: dict) -> list[dict]:
    """
    usage-shock responses nest a teammates array, each teammate carrying
    {deltas: {...}, withStats: {...}, withoutStats: {...}}.
    Expand into one tidy row per teammate per context. The unnesting is what
    keeps this loadable: left as dicts, Polars reads them as struct columns and
    locks each field's dtype from the first row, so a later teammate whose
    average lands on a whole number fails to build.
    """
    base = {k: v for k, v in row.items() if k != "teammates"}
    results = []
    for teammate in row.get("teammates", []):
        teammate_base = {
            **base,
            **{
                k: v
                for k, v in teammate.items()
                if k not in ("deltas", "withStats", "withoutStats")
            },
            **teammate.get("deltas", {}),
        }
        for context, stats in (
            ("with", teammate.get("withStats")),
            ("without", teammate.get("withoutStats")),
        ):
            if stats:
                results.append({**teammate_base, "context": context, **stats})
    return results


def infer_dtypes(df: pl.DataFrame, threshold: float = 1.0) -> pl.DataFrame:
    """
    Best-guess cast every string column to Int64 -> Float64 -> Date -> Datetime,
    in that order, keeping the first one where enough values parse cleanly.

    threshold: fraction of non-null values that must parse successfully for
    a cast to be accepted (1.0 = every non-null value must parse).
    """
    for name in df.select(cs.string()).columns:
        col = df[name]
        n = col.drop_nulls().len()
        if n == 0:
            continue

        for parse in (
            pl.col(name).cast(pl.Int64, strict=False),
            pl.col(name).cast(pl.Float64, strict=False),
            pl.col(name).str.to_date(strict=False),
            pl.col(name).str.to_datetime(strict=False),
            pl.col(name).str.to_datetime(strict=False, time_zone="UTC"),
        ):
            try:
                casted = df.select(parse)[name]
            except pl.exceptions.ComputeError:
                continue  # format doesn't apply to this column at all

            if casted.null_count() <= col.null_count() + n * (1 - threshold):
                df = df.with_columns(parse)
                break

    # Downcast Datetime columns to Date if every time component is midnight
    for name in df.select(cs.datetime()).columns:
        col = df[name]
        if col.drop_nulls().len() == 0:
            continue
        if (col.dt.time().drop_nulls() == dt.time(0, 0)).all():
            df = df.with_columns(pl.col(name).dt.date().alias(name))

    return df
