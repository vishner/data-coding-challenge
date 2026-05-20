"""
Ingestion assets — load raw files from data/raw/ into ClickHouse.

CSV dimensions: truncate, load initial file, then redelivery overlay when present.
Events: truncate, full reload; last days use redelivery/events/ when available.
"""

import csv
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable

import pyarrow.parquet as pq
from dagster import AssetExecutionContext, MetadataValue, asset

from dagster_project.resources import ClickHouseResource
from dagster_project.source_schemas import (
    AD_EVENTS,
    AD_UNITS,
    CAMPAIGNS,
    CsvSource,
    ParquetEventsSource,
    PUBLISHERS,
    RAW_DATA_DIR,
    REDELIVERY_DIR,
)

RAW_ROOT = Path(RAW_DATA_DIR)
REDELIVERY_ROOT = Path(REDELIVERY_DIR)
INSERT_BATCH_SIZE = 10000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_date(value: str) -> date:
    return datetime.strptime(value.strip(), "%Y-%m-%d").date()


def _parse_datetime(value: str) -> datetime:
    cleaned = value.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unrecognised datetime: {value!r}")


def _parse_event_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.strip().replace("Z", "+00:00"))


def _read_csv(path: Path) -> list[dict[str, str]]:
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _insert_rows(
    client: Any,
    table: str,
    rows: list[tuple[Any, ...]],
    column_names: list[str],
) -> int:
    for offset in range(0, len(rows), INSERT_BATCH_SIZE):
        client.insert(
            table,
            rows[offset : offset + INSERT_BATCH_SIZE],
            column_names=column_names,
        )
    return len(rows)


def _load_csv_source(
    clickhouse: ClickHouseResource,
    source: CsvSource,
    build_rows: Callable[[list[dict[str, str]]], list[tuple[Any, ...]]],
) -> tuple[int, int, bool]:
    initial_path = RAW_ROOT / source.initial_file
    redelivery_path = (
        REDELIVERY_ROOT / source.redelivery_file
        if source.redelivery_file
        else None
    )

    initial_records = _read_csv(initial_path)
    redelivery_records: list[dict[str, str]] = []
    if redelivery_path and redelivery_path.exists():
        redelivery_records = _read_csv(redelivery_path)

    clickhouse.execute(f"TRUNCATE TABLE {source.table}")
    initial_rows = build_rows(initial_records)
    redelivery_rows = build_rows(redelivery_records) if redelivery_records else []
    columns = list(source.insert_columns)

    with clickhouse.get_client() as client:
        if initial_rows:
            _insert_rows(client, source.table, initial_rows, columns)
        if redelivery_rows:
            _insert_rows(client, source.table, redelivery_rows, columns)

    return len(initial_rows), len(redelivery_rows), bool(redelivery_records)


def _resolve_event_files(
    source: ParquetEventsSource,
) -> list[tuple[Path, str]]:
    events_dir = RAW_ROOT / source.events_dir
    redelivery_dir = REDELIVERY_ROOT / source.redelivery_events_dir
    redelivery_dates = (
        {p.stem for p in redelivery_dir.glob("*.parquet")}
        if redelivery_dir.is_dir()
        else set()
    )

    files: list[tuple[Path, str]] = []
    for path in sorted(events_dir.glob("*.parquet")):
        if path.stem in redelivery_dates:
            continue
        files.append((path, f"events/{path.name}"))

    if redelivery_dir.is_dir():
        for path in sorted(redelivery_dir.glob("*.parquet")):
            files.append((path, f"redelivery/events/{path.name}"))

    return files


def _event_row(record: dict[str, Any], source_file: str) -> tuple[Any, ...]:
    campaign_id = record.get("campaign_id")
    advertiser_id = record.get("advertiser_id")
    revenue = record.get("revenue_usd")
    bid_floor = record.get("bid_floor_usd")
    quality = record.get("placement_quality_score")

    return (
        str(record["event_id"]),
        str(record["event_type"]),
        _parse_event_timestamp(str(record["timestamp"])),
        int(record["publisher_id"]),
        str(record["site_domain"]),
        str(record["ad_unit_id"]),
        int(campaign_id) if campaign_id is not None else None,
        int(advertiser_id) if advertiser_id is not None else None,
        str(record["device_type"]),
        str(record["country_code"]),
        str(record.get("region") or ""),
        str(record["browser"]),
        float(quality) if quality is not None else None,
        float(revenue) if revenue is not None else None,
        float(bid_floor) if bid_floor is not None else None,
        1 if record.get("is_filled") else 0,
        source_file,
    )


def _manifest_from_files(files: list[tuple[Path, str]]) -> dict[str, Any]:
    total_rows = 0
    total_revenue = 0.0
    all_ids: set[str] = set()
    per_file: dict[str, int] = {}

    for path, label in files:
        table = pq.read_table(path)
        total_rows += table.num_rows
        per_file[label] = table.num_rows
        all_ids.update(table.column("event_id").to_pylist())
        for value in table.column("revenue_usd").to_pylist():
            if value is not None:
                total_revenue += float(value)

    return {
        "row_count": total_rows,
        "distinct_event_ids": len(all_ids),
        "revenue_sum": total_revenue,
        "per_file": per_file,
    }


# ---------------------------------------------------------------------------
# Row builders (file field → ClickHouse column)
# ---------------------------------------------------------------------------


def _publisher_rows(records: list[dict[str, str]]) -> list[tuple[Any, ...]]:
    return [
        (
            int(r["publisher_id"]),
            r["publisher_name"],
            r["publisher_category"],
            r["primary_domain"],
            r["account_manager"],
            r["country"],
            r.get("timezone", ""),
            _parse_datetime(r["created_at"]),
            _parse_datetime(r["updated_at"]),
        )
        for r in records
    ]


def _campaign_rows(records: list[dict[str, str]]) -> list[tuple[Any, ...]]:
    return [
        (
            int(r["campaign_id"]),
            r["campaign_name"],
            int(r["advertiser_id"]),
            r["advertiser_name"],
            _parse_date(r["start_date"]),
            _parse_date(r["end_date"]),
            float(r["budget_usd"]),
            r["status"],
            r.get("device_targeting", ""),
            r.get("country_targeting", ""),
            _parse_datetime(r["created_at"]),
        )
        for r in records
    ]


def _ad_unit_rows(records: list[dict[str, str]]) -> list[tuple[Any, ...]]:
    return [
        (
            r["ad_unit_id"],
            int(r["publisher_id"]),
            r["ad_unit_name"],
            r["ad_format"],
            r["ad_size"],
            r["placement_type"],
            int(r["is_active"]),
            _parse_datetime(r["created_at"]),
        )
        for r in records
    ]


def _csv_metadata(
    source: CsvSource,
    initial_count: int,
    redelivery_count: int,
    redelivery_applied: bool,
) -> dict[str, MetadataValue]:
    return {
        "table": MetadataValue.text(source.table),
        "initial_row_count": MetadataValue.int(initial_count),
        "redelivery_row_count": MetadataValue.int(redelivery_count),
        "redelivery_overlay": MetadataValue.bool(redelivery_applied),
    }


# ---------------------------------------------------------------------------
# Dimension assets
# ---------------------------------------------------------------------------


@asset(group_name="ingestion")
def raw_publishers(
    context: AssetExecutionContext,
    clickhouse: ClickHouseResource,
) -> None:
    """Load publishers.csv into raw.publishers."""
    initial, redelivery, applied = _load_csv_source(
        clickhouse, PUBLISHERS, _publisher_rows
    )
    context.log.info(
        "publishers: %d initial rows, %d redelivery rows", initial, redelivery
    )
    context.add_output_metadata(_csv_metadata(PUBLISHERS, initial, redelivery, applied))


@asset(group_name="ingestion")
def raw_campaigns(
    context: AssetExecutionContext,
    clickhouse: ClickHouseResource,
) -> None:
    """Load campaigns_export.csv into raw.campaigns."""
    initial, redelivery, applied = _load_csv_source(
        clickhouse, CAMPAIGNS, _campaign_rows
    )
    context.log.info(
        "campaigns: %d initial rows, %d redelivery rows", initial, redelivery
    )
    context.add_output_metadata(_csv_metadata(CAMPAIGNS, initial, redelivery, applied))


@asset(group_name="ingestion")
def raw_ad_units(
    context: AssetExecutionContext,
    clickhouse: ClickHouseResource,
) -> None:
    """Load ad_units.csv into raw.ad_units."""
    initial, redelivery, applied = _load_csv_source(
        clickhouse, AD_UNITS, _ad_unit_rows
    )
    context.log.info(
        "ad_units: %d initial rows, %d redelivery rows", initial, redelivery
    )
    context.add_output_metadata(_csv_metadata(AD_UNITS, initial, redelivery, applied))


# ---------------------------------------------------------------------------
# Events asset
# ---------------------------------------------------------------------------


@asset(
    group_name="ingestion",
    deps=[raw_publishers, raw_ad_units, raw_campaigns],
)
def raw_ad_events(
    context: AssetExecutionContext,
    clickhouse: ClickHouseResource,
) -> None:
    """Load event parquet files into raw.ad_events."""
    files = _resolve_event_files(AD_EVENTS)
    expected = _manifest_from_files(files)

    rows: list[tuple[Any, ...]] = []
    for path, source_label in files:
        for record in pq.read_table(path).to_pylist():
            rows.append(_event_row(record, source_label))

    clickhouse.execute(f"TRUNCATE TABLE {AD_EVENTS.table}")
    with clickhouse.get_client() as client:
        loaded = _insert_rows(client, AD_EVENTS.table, rows, list(AD_EVENTS.insert_columns))

    loaded_stats = clickhouse.query(
        f"""
        SELECT
            count(),
            uniqExact(event_id),
            sumIf(toFloat64(revenue_usd), revenue_usd IS NOT NULL)
        FROM {AD_EVENTS.table}
        """
    )[0]
    row_count, distinct_ids, revenue_sum = loaded_stats

    if (
        int(row_count) != expected["row_count"]
        or int(distinct_ids) != expected["distinct_event_ids"]
        or abs(float(revenue_sum) - expected["revenue_sum"]) > 0.0001
    ):
        raise RuntimeError(
            f"Load validation failed: expected {expected['row_count']} rows, "
            f"{expected['distinct_event_ids']} ids, revenue {expected['revenue_sum']:.6f}; "
            f"got {row_count}, {distinct_ids}, {float(revenue_sum):.6f}"
        )

    context.log.info(
        "ad_events: %d rows, revenue %.6f", row_count, float(revenue_sum)
    )
    context.add_output_metadata({
        "files_loaded": MetadataValue.int(len(files)),
        "row_count": MetadataValue.int(int(row_count)),
        "distinct_event_ids": MetadataValue.int(int(distinct_ids)),
        "revenue_sum": MetadataValue.float(float(revenue_sum)),
        "rows_inserted": MetadataValue.int(loaded),
    })
