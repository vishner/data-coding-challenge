{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['event_date', 'publisher_id'],
        engine='MergeTree()',
        order_by='(event_date, publisher_id)',
    )
}}

/*
    Fact: daily ad metrics by publisher calendar day.

    Grain: event_date (publisher timezone) × publisher_id
    - event_date uses each publisher's IANA timezone so daily totals align with
      how publishers report; UTC date is available as event_date_utc for audit.

    Materialisation: incremental delete+insert on (event_date, publisher_id).
    Re-ingestion updates raw.ad_events; this model rebuilds affected days so
    redelivery corrections flow through without duplicating history.

    fill_rate = filled events / all events (impressions + clicks + viewable).
    ctr = clicks / impressions (0 when no impressions).
*/

with daily as (
    select
        event_date_publisher as event_date,
        publisher_id,
        countIf(event_type = 'impression') as impressions,
        countIf(event_type = 'click') as clicks,
        countIf(event_type = 'viewable_impression') as viewable_impressions,
        sum(toFloat64(revenue_usd)) as revenue_usd,
        countIf(is_filled = 1) as filled_events,
        count() as total_events
    from {{ ref('stg_ad_events') }}
    {% if is_incremental() %}
    where event_date_publisher >= (
        select coalesce(max(event_date), toDate('1970-01-01')) - 7
        from {{ this }}
    )
    {% endif %}
    group by 1, 2
)

select
    event_date,
    publisher_id,
    impressions,
    clicks,
    viewable_impressions,
    revenue_usd,
    filled_events / nullIf(total_events, 0) as fill_rate,
    clicks / nullIf(impressions, 0) as ctr,
    viewable_impressions / nullIf(impressions, 0) as viewability_rate,
    total_events
from daily
