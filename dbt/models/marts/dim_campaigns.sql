{{
    config(
        materialized='table'
    )
}}

/*
    Dimension: campaigns (SCD Type 1 — latest flight dates / budget from redelivery).
    Grain: one row per campaign_id.
*/

select
    campaign_id,
    campaign_name,
    advertiser_id,
    advertiser_name,
    campaign_start_date,
    campaign_end_date,
    campaign_budget_usd,
    campaign_status,
    targeting_device_types,
    targeting_countries,
    created_at,
    _loaded_at
from {{ ref('stg_campaigns') }}
