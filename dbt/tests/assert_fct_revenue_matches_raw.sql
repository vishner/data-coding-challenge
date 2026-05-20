/*
    Singular test: mart revenue must reconcile to raw.ad_events (within rounding).
*/

select revenue_diff
from (
    select abs(
        (
            select sum(toFloat64(revenue_usd))
            from {{ source('raw', 'ad_events') }}
            where revenue_usd is not null
        )
        - (
            select sum(revenue_usd)
            from {{ ref('fct_ad_events_daily') }}
        )
    ) as revenue_diff
)
where revenue_diff > 0.01
