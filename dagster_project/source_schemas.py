"""
Source definitions for landing-zone files.

Add new sources here (paths, target table, column list). Row mapping stays in
ingest.py next to each asset — same pattern as the starter code.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class CsvSource:
    table: str
    initial_file: str
    redelivery_file: str | None
    merge_key: str
    insert_columns: tuple[str, ...]


@dataclass(frozen=True)
class ParquetEventsSource:
    table: str
    events_dir: str
    redelivery_events_dir: str
    insert_columns: tuple[str, ...]


RAW_DATA_DIR = "data/raw"
REDELIVERY_DIR = "data/raw/redelivery"

PUBLISHERS = CsvSource(
    table="raw.publishers",
    initial_file="publishers.csv",
    redelivery_file="publishers.csv",
    merge_key="publisher_id",
    insert_columns=(
        "publisher_id",
        "publisher_name",
        "publisher_category",
        "primary_domain",
        "account_manager",
        "country",
        "timezone",
        "created_at",
        "updated_at",
    ),
)

CAMPAIGNS = CsvSource(
    table="raw.campaigns",
    initial_file="campaigns/campaigns_export.csv",
    redelivery_file="campaigns/campaigns_export.csv",
    merge_key="campaign_id",
    insert_columns=(
        "campaign_id",
        "campaign_name",
        "advertiser_id",
        "advertiser_name",
        "campaign_start_date",
        "campaign_end_date",
        "campaign_budget_usd",
        "campaign_status",
        "targeting_device_types",
        "targeting_countries",
        "created_at",
    ),
)

AD_UNITS = CsvSource(
    table="raw.ad_units",
    initial_file="ad_units.csv",
    redelivery_file="ad_units.csv",
    merge_key="ad_unit_id",
    insert_columns=(
        "ad_unit_id",
        "publisher_id",
        "ad_unit_name",
        "ad_format",
        "ad_size",
        "placement_type",
        "is_active",
        "created_at",
    ),
)

AD_EVENTS = ParquetEventsSource(
    table="raw.ad_events",
    events_dir="events",
    redelivery_events_dir="events",
    insert_columns=(
        "event_id",
        "event_type",
        "event_timestamp",
        "publisher_id",
        "site_domain",
        "ad_unit_id",
        "campaign_id",
        "advertiser_id",
        "device_type",
        "country_code",
        "region",
        "browser",
        "placement_quality_score",
        "revenue_usd",
        "bid_floor_usd",
        "is_filled",
        "_source_file",
    ),
)
