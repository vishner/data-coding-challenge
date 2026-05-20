{{
    config(
        materialized='table'
    )
}}

/*
    Dimension: ad units (SCD Type 1).
    Grain: one row per ad_unit_id.
*/

select
    ad_unit_id,
    publisher_id,
    ad_unit_name,
    ad_format,
    ad_size,
    placement_type,
    is_active,
    created_at,
    _loaded_at
from {{ ref('stg_ad_units') }}
