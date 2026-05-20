/*
    Singular test: fill_rate must be between 0 and 1 inclusive.
*/

select
    event_date,
    publisher_id,
    fill_rate
from {{ ref('fct_ad_events_daily') }}
where fill_rate < 0 or fill_rate > 1
