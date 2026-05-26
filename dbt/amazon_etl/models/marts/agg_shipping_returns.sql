/*
agg_shipping_returns.sql
Shipping time bands vs return rate.
Answers:
    Do slow deliveries cause more returns?
Key operational insight for the dashboard.
*/

with orders as (
    select *
    from {{ ref('fact_orders')}}
),

banded as (
    select
        *,
        case
            when shipping_time_days <= 3 then '0-3 days (Fast)'
            when shipping_time_days > 3
                and shipping_time_days <= 7 then '4-7 days (Standard)'
            when shipping_time_days > 7
                and shipping_time_days <= 14 then '8-14 days (Slow)'
            else '15+ days (Very Slow)'
        end as shipping_band,

        --used for sorting in Tableau
        case
            when shipping_time_days <= 3 then 1
            when shipping_time_days > 3
                and shipping_time_days <= 7 then 2
            when shipping_time_days > 7
                and shipping_time_days <= 14 then 3
            else 4
        end as shipping_band_sort,

    from orders
)

select 
    shipping_band,
    shipping_band_sort,
    delivery_status,
    location,

    --volume
    count(*) as total_orders,

    --shipping
    round(avg(shipping_time_days), 2) as avg_shipping_days,
    min(shipping_time_days) as min_shipping_days,
    max(shipping_time_days) as max_shipping_days,

    --revenue
    cast(round(sum(final_price), 2) as decimal(15, 2)) as total_revenue,
    cast(round(avg(final_price), 2) as decimal(15, 2)) as avg_order_value,

    --returns
    sum(case when is_returned then 1 else 0 end) as total_returns,
    round(sum(case when is_returned then 1 else 0 end)
        / count(*) * 100, 2) as return_rate_pct,

    --returned revenue
    cast(round(sum(case when is_returned then final_price else 0 end), 2) as decimal(15, 2)) as returned_revenue,

    --customer satisfaction signal
    round(avg(rating), 2) as avg_product_rating

from banded
group by
    shipping_band,
    shipping_band_sort,
    delivery_status,
    location
order by
    shipping_band_sort,
    location
;