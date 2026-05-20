# Design Document

> Replace each section with your own analysis. Keep it concise — quality over quantity.

## 1) Pipeline — What You Changed and Why

For each change to `dagster_project/assets/ingest.py` (or any other infrastructure code):

- What was the original behaviour?  - 
Loaded only from data/raw/ (no redelivery). Dates/times sent as strings (can fail with clickhouse-connect).
Asset only listed parquet files and set metadata STUB — load not implemented. No rows in raw.ad_events. implementation is functional, 
added some option to import configuration / source_schemas.py to allow for easy dynamic csv loading 

- How did you discover the problem? (e.g. row count mismatch, distribution check, value comparison against the raw file)
Inspected data/raw/redelivery/events/ and compared 2026-03-22..24: 
initial files ~6.5k rows each, redelivery ~3.1k, ~3k overlapping event_ids, different revenue sums. 
Loading both would double-count recent days.

- What did you change? 
Implemented parquet load: _resolve_event_files() skips initial files for dates that exist in redelivery, then loads redelivery files. 
TRUNCATE + full insert each run. _manifest_from_files() builds expected row count, distinct event_id, and revenue from disk; 
asset raises if ClickHouse differs. 
Schemas in source_schemas.py (AD_EVENTS).

- How did you verify the fix?
189,972 rows, 189,972 distinct event_ids, revenue 1879.099641 USD (manifest vs query on raw.ad_events). 
Ran all four assets twice; same numbers on second run.


## 2) dbt — Design Choices

Investigation charts and SQL live in `notebooks/revenue_investigation.ipynb`.

### 2a) Materialisation

| Model | Strategy | Why |
|-------|----------|-----|
| `stg_ad_events` | `view` | Always reflects current `raw.ad_events` after ingestion truncate/reload. |
| `stg_*` dimensions | `view` over `raw.* FINAL` | One logical row per key despite ReplacingMergeTree duplicates. |
| `dim_*` | `table` | Snapshot of latest dimension attributes for joins. |
| `fct_ad_events_daily` | `incremental` `delete+insert` on `(event_date, publisher_id)` | Rebuilds recent days when raw is re-ingested; avoids full-table rewrite at scale while staying correct after redelivery. |

**Re-delivered event with corrected revenue:** Ingestion replaces raw rows for affected days; incremental fact re-aggregates those `event_date` + `publisher_id` keys → mart revenue updates. Verified: raw and mart both sum to **1879.099641** USD (`assert_fct_revenue_matches_raw` passes).

**Re-delivered event with `revenue_usd = NULL`:** Included in aggregates as NULL/zero contribution; `fill_rate` still counts the row in `total_events`.

**Ingestion run twice on identical data:** Raw unchanged → dbt re-run replaces same keys → **620** fact rows, **178,229** impressions, **1879.099641** revenue (stable).

### 2b) Daily aggregation boundary

**Grain:** `event_date` × `publisher_id` in `fct_ad_events_daily`.

**`event_date`** = calendar date of `event_timestamp` in the **publisher's IANA timezone** (`stg_publishers.timezone`), implemented with `multiIf` + `toTimeZone` (ClickHouse requires constant timezone literals).

**Why not UTC:** Publishers report and reconcile on local business days; UTC splits evening traffic across dates.

**Publisher 11 (RetroGames Hub, `America/Los_Angeles`) — 2026-03-19 revenue:**

| Bucketing | Revenue USD |
|-----------|-------------|
| UTC date | **5.18** |
| LA date | **181.46** |

Same total publisher revenue over the full range; **~$418** moves between days when re-bucketing UTC → LA (see notebook 3).

### 2c) Dimension handling

**SCD Type 1 (latest wins):** `stg_publishers`, `stg_campaigns`, `stg_ad_units` read `FROM raw.* FINAL` after Dagster redelivery overlay.

**Fields that change (initial → redelivery):**

| Entity | ID | Field | Initial | Redelivery |
|--------|-----|-------|---------|------------|
| Publisher | 4 | `account_manager` | Priya Singh | Hannah Reid |
| Publisher | 12 | `publisher_category` | Lifestyle | Entertainment |
| Campaign | 1003 | `campaign_budget_usd` | 320000 | 360000 |
| Campaign | 1017 | `campaign_status` / end date | completed / 2026-03-05 | active / 2026-03-20 |

**Fact → dimension join:** Uses **latest** attributes (not event-time SCD2). Appropriate for daily publisher revenue reporting and operational dashboards; not for historical “who was the AM when this impression fired?” audits.

**Impact example:** Campaign 1017 — under initial dim, **224** events (Mar 6–20) look out-of-flight; under `FINAL` redelivery they are in-flight.

### 2d) Tests

| Test | Invariant | Failure means |
|------|-----------|---------------|
| `assert_fct_revenue_matches_raw` | Sum of `fct_ad_events_daily.revenue_usd` = sum of raw `revenue_usd` (±$0.01) | Grain, timezone logic, or join fan-out broke revenue conservation. |
| `assert_fill_rate_valid` | `0 <= fill_rate <= 1` | Bad fill_rate definition or divide-by-zero bug. |
| `dbt_utils.unique_combination_of_columns` on `(event_date, publisher_id)` | One fact row per publisher-day | Double-counted daily aggregates. |
| `relationships` on `publisher_id` | Every fact publisher exists in `dim_publishers` | Orphan keys / broken dim load. |
| `accepted_values` on `campaign_status` | Only `active`, `completed` | Unexpected upstream status codes. |

## 3) Revenue Integrity Investigation

Full write-up with detection SQL and result tables: **`notebooks/revenue_integrity_findings.ipynb`**

Run that notebook top-to-bottom (ClickHouse on `localhost:8123`, ingestion materialised), then copy each finding into this section for submission. Summary of findings:

| # | Title | Scale |
|---|--------|--------|
| 1 | Publisher 11 × campaign 1051 CPM spike | 410 events, $1,455.27 (77% of revenue) |

========================================================================
Finding 1: Publisher 11 / Campaign 1051 CPM spike
========================================================================
WHAT: 410 events (0.22% of paid rows) have revenue_usd > $1, totalling $1,455.27 — 77.4% of all revenue ($1,879.10). Publisher 11 on campaign 1051: CPM $1,273 vs $2.38 for all other publishers. Spike starts 2026-03-20 UTC; campaign 1051 total on pub 11 ≈ $1,489.49.
WHY IT MATTERS: Daily and campaign reporting is dominated by ~400 rows. Finance reconciliation and publisher payouts would be wrong without review.
HOW HANDLED: Documented in mart; not excluded in pipeline. Production: quarantine flag, SSP ticket, cap or exclude until confirmed.
event_date_utc	events	high_revenue_events	total_revenue_usd
0	2026-03-15	133	0	3.943587
1	2026-03-16	140	0	4.469430
2	2026-03-17	157	0	4.952640
3	2026-03-18	128	0	3.917502
4	2026-03-19	146	0	4.774192
5	2026-03-20	143	131	464.705200
6	2026-03-21	132	111	401.279700
7	2026-03-22	77	67	233.178719
8	2026-03-23	66	54	196.464073
9	2026-03-24	60	47	171.801029


| 2 | Filled events with $0 revenue | 12,324 events (6.5%) |

========================================================================
Finding 2: Filled events with $0 revenue
========================================================================
WHAT: 12,324 events (6.5% of 189,972) have is_filled = 1 and revenue_usd NULL or 0. $0 direct revenue impact but they inflate filled inventory.
WHY IT MATTERS: Fill rate overstates monetisable inventory; publisher reporting may count serves that never paid.
HOW HANDLED: Included in fct fill_rate denominator (filled/total events). Production: separate metric billable_impressions excluding zero-revenue fills.
| 3 | Dimension join without `FINAL` | 2× row fan-out on publishers |

========================================================================
Finding 3: Join fan-out on raw dimensions
========================================================================
WHAT: 189,972 fact rows become 379,944 when joining raw.publishers without FINAL (+100%). With FINAL: 189,972 rows. Any SUM(revenue) after naive join over-counts by 2× on publishers.
WHY IT MATTERS: Aggregate revenue integrity fails silently in ad-hoc SQL and broken dbt models.
HOW HANDLED: stg_* dimensions use FROM raw.* FINAL; 

| 4 | Campaign 1017 flight / status redelivery | 224 events, $0.48 |

========================================================================
Finding 4: Campaign 1017 flight mismatch
========================================================================
WHAT: Initial export: status completed, end 2026-03-05. Redelivery (FINAL): active, end 2026-03-20. 224 events between 2026-03-06 and 2026-03-20 ($0.48 revenue) are in-flight under redelivery only.
WHY IT MATTERS: Flight filters and budget pacing depend on dimension version; wrong version misclassifies valid traffic.
HOW HANDLED: Dagster loads redelivery overlay; dbt uses FINAL (SCD Type 1 latest). Production: SCD2 if audit trail required.


| 5 | UTC vs publisher-local day (pub 11) | Mar 19: $5.18 UTC vs $181.46 LA |

========================================================================
Finding 5: UTC vs publisher-local event day
========================================================================
WHAT: Publisher 11 (America/Los_Angeles): 2026-03-19 revenue = $5.18 (UTC bucket) vs $181.46 (LA bucket). Total publisher revenue unchanged; ~$418 reallocated across days when rebucketing.
WHY IT MATTERS: Publisher-facing daily reports must use publisher timezone; UTC breaks evening traffic allocation.
HOW HANDLED: fct_ad_events_daily uses event_date in publisher timezone (see stg_ad_events / DESIGN §2b).

| 6 | Dimension attribute drift | Pubs 4, 12; campaign 1003 budget |

========================================================================
Finding 6: Dimension attribute drift
========================================================================
WHAT: Publishers: id 4 account_manager Priya Singh → Hannah Reid; id 12 category Lifestyle → Entertainment. Campaign 1003 budget $320,000 → $360,000. Campaign 1017 per Finding 4.
WHY IT MATTERS: Account management and categorisation reports shift when redelivery lands.
HOW HANDLED: Ingestion inserts initial then redelivery; dbt reads FINAL / Type 1 dims.


| 7 | site_domain ≠ primary_domain | 657 events, $1.50 |

========================================================================
Finding 7: site_domain vs primary_domain mismatch
========================================================================
WHAT: 657 events ($1.50 revenue) where site_domain ≠ publishers.primary_domain (FINAL join).
WHY IT MATTERS: Small $ impact but signals syndication / network serving worth monitoring.
HOW HANDLED: No filter in mart; flag in QA dashboard in production.

Exploratory charts: `notebooks/revenue_investigation.ipynb`

## 4) Trade-offs

- **Not implemented:** SCD2 history dimensions, event-level dedup in dbt (relies on ingestion), dynamic timezone map (hard-coded `multiIf` for 10 zones). No exclusion rules for revenue spike rows in marts yet.
- **Shortcuts:** `multiIf` timezone list must be updated when publishers add zones; incremental fact uses 7-day lookback not tuned to SLA; investigation findings in notebook not yet fully written into Section 3.
- **Next ~4 hours:** Copy notebook findings into this doc for final submit; flag/exclude pub 11 × campaign 1051 spike in mart; `OPTIMIZE FINAL` after dim ingest.
