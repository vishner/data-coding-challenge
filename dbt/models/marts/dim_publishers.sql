{{
    config(
        materialized='table'
    )
}}

/*
    Dimension: publishers (SCD Type 1 — latest attributes from redelivery overlay).
    Grain: one row per publisher_id.
*/

select
    publisher_id,
    publisher_name,
    publisher_category,
    primary_domain,
    account_manager,
    country,
    timezone,
    created_at,
    updated_at,
    _loaded_at
from {{ ref('stg_publishers') }}
