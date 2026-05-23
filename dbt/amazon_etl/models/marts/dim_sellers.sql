/*
dim_sellers.sql
One row per seller with performance and risk signals.
*/

with orders as (
    select *
    from {{ ref('fact_orders')}}
)

select
    seller_id,

    --volume
    count(*) as total_orders,
    count(distinct user_id) as unique_customers,
    count(distinct product_id) as unique_products,
    sum(final_price) as total_revenue,
    round(avg(final_price), 2) as avg_order_value,

    --quality
    round(avg(seller_rating), 2) as avg_seller_rating,
    round(avg(rating), 2) as avg_product_rating,

    --risk signals
    sum(case when is_returned then 1 else 0 end) as total_returns,
    round(sum(case when is_returned then 1 else 0 end)
        / count(*) * 100, 2) as return_rate_pct,

    --fulfillment
    round(avg(shipping_time_days), 2) as avg_shipping_days,
    mode(location) as primary_location

from orders
group by seller_id
;