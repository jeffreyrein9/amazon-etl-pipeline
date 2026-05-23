/*
agg_shipping.sql
Operational shipping and delivery metrics by location and status.
*/

with orders as (
    select *
    from {{ ref('fact_orders')}}
)

select
    location,
    delivery_status,

    count(*) as total_orders,
    round(avg(shipping_time_days), 2) as avg_shipping_days,
    min(shipping_time_days) as min_shipping_days,
    max(shipping_time_days) as max_shipping_days,
    sum(case when is_returned then 1 else 0 end) as total_returns,
    round(sum(case when is_returned then 1 else 0 end)
        / count(*) * 100, 2) as return_rate_pct

from orders
group by
    location,
    delivery_status
;