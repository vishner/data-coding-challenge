{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(event_date, publisher_id)',
    )
}}

/*
    Publisher-day rollup with dimension attributes (nice-to-have).
    Grain: event_date × publisher_id — same as fct_ad_events_daily plus publisher dims.
*/

select
    f.event_date,
    f.publisher_id,
    p.publisher_name,
    p.publisher_category,
    p.account_manager,
    p.timezone,
    f.impressions,
    f.clicks,
    f.viewable_impressions,
    f.revenue_usd,
    f.fill_rate,
    f.ctr,
    f.viewability_rate,
    f.total_events
from {{ ref('fct_ad_events_daily') }} as f
inner join {{ ref('dim_publishers') }} as p
    on f.publisher_id = p.publisher_id
