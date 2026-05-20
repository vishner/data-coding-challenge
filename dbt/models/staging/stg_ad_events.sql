{{
    config(
        materialized='view'
    )
}}

/*
    Staging: ad_events
    - Cleansed pass-through from raw (ingestion already dedupes event_id).
    - Adds UTC and publisher-local calendar dates for downstream grain choice.
*/

with events as (
    select
        event_id,
        event_type,
        event_timestamp,
        publisher_id,
        site_domain,
        ad_unit_id,
        campaign_id,
        advertiser_id,
        device_type,
        upper(country_code) as country_code,
        region,
        browser,
        placement_quality_score,
        revenue_usd,
        bid_floor_usd,
        is_filled,
        _source_file,
        _loaded_at
    from {{ source('raw', 'ad_events') }}
    where event_id != ''
      and event_type != ''
)

select
    e.event_id,
    e.event_type,
    e.event_timestamp,
    toDate(e.event_timestamp) as event_date_utc,
    toDate(
        multiIf(
            p.timezone = 'America/Chicago', toTimeZone(e.event_timestamp, 'America/Chicago'),
            p.timezone = 'America/Detroit', toTimeZone(e.event_timestamp, 'America/Detroit'),
            p.timezone = 'America/Los_Angeles', toTimeZone(e.event_timestamp, 'America/Los_Angeles'),
            p.timezone = 'America/New_York', toTimeZone(e.event_timestamp, 'America/New_York'),
            p.timezone = 'Asia/Kolkata', toTimeZone(e.event_timestamp, 'Asia/Kolkata'),
            p.timezone = 'Asia/Manila', toTimeZone(e.event_timestamp, 'Asia/Manila'),
            p.timezone = 'Asia/Tokyo', toTimeZone(e.event_timestamp, 'Asia/Tokyo'),
            p.timezone = 'Australia/Sydney', toTimeZone(e.event_timestamp, 'Australia/Sydney'),
            p.timezone = 'Europe/Berlin', toTimeZone(e.event_timestamp, 'Europe/Berlin'),
            p.timezone = 'Europe/London', toTimeZone(e.event_timestamp, 'Europe/London'),
            toTimeZone(e.event_timestamp, 'UTC')
        )
    ) as event_date_publisher,
    e.publisher_id,
    e.site_domain,
    e.ad_unit_id,
    e.campaign_id,
    e.advertiser_id,
    e.device_type,
    e.country_code,
    e.region,
    e.browser,
    e.placement_quality_score,
    e.revenue_usd,
    e.bid_floor_usd,
    e.is_filled,
    e._source_file,
    e._loaded_at
from events as e
left join {{ ref('stg_publishers') }} as p
    on e.publisher_id = p.publisher_id
