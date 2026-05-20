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

### 2a) Materialisation

Which strategy did you pick for `fct_ad_events_daily` and the staging events model? Why?

- What does your choice do when an event is re-delivered with a corrected revenue value?
- What does your choice do when an event is re-delivered with `revenue_usd = NULL`?
- What does your choice do when ingestion is run twice on identical data?

Show the numbers (sums, counts) that prove your choice produces the right behaviour.

### 2b) Daily aggregation boundary

How does your `fct_ad_events_daily` define "day"? Why?

If your `event_date` differs by publisher timezone, show one publisher where it changes the daily total vs. the naive UTC choice.

### 2c) Dimension handling

How are you handling publisher and campaign attribute changes between the initial and redelivery dimension files?

- Which fields change?
- Does your join from facts to dimensions reflect the value at event-time, or the latest value?
- Why is that the right choice for daily ad-revenue reporting?

### 2d) Tests

For each test you wrote that isn't a basic `unique` / `not_null`: what business invariant does it assert, and what would a failure mean?

## 3) Revenue Integrity Investigation

For each anomaly you found, fill in:

### Finding N: [short title]

- **What:** Quantified description. Specific IDs, date ranges, counts, revenue impact.
- **Why it matters:** Business / commercial implication.
- **How you handled it:** What did you do in the pipeline / what would you do in production.
- **Detection query:**

```sql
-- the actual SQL you ran
```

- **Result table / distribution that confirms it:** (paste the rows or a summary)

Repeat for each finding. There is no fixed number of findings expected — but each one must be quantified, evidenced, and traced to a query. Anything described in vague terms or without numbers will not be credited.

## 4) Trade-offs

- What did you intentionally not implement, and why?
- Where did you take shortcuts that you would not take in production?
- With another four hours, what would you do next?
