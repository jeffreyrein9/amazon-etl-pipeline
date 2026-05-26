/*
agg_returns.sql
Return rate analysis by category, subcategory, and brand.
Primary model for identifying where the business is losing money to returns.
*/

with orders as (
    select *
    from {{ ref('fact_orders')}}
)

select 
    category,
    subcategory,
    brand,

    --volume
    count(*) as total_orders,
    count(distinct user_id) as unique_customers,

    --revenue
    cast(round(sum(final_price), 2) as decimal(15, 2)) as total_revenue,
    cast(round(avg(final_price), 2) as decimal(15, 2)) as avg_order_value,

    --returns
    sum(case when is_returned then 1 else 0 end) as total_returns,
    round(sum(case when is_returned then 1 else 0 end)
        / count(*) * 100, 2) as return_rate_pct,

    --returned revenue (money lost)
    cast(round(sum(case when is_returned then final_price else 0 end), 2) as decimal(15, 2)) as returned_revenue,

    --avg discount on returned vs non-returned items
    round(avg(case when is_returned then discount_pct end), 2) as avg_discount_pct_returned,
    round(avg(case when not is_returned then discount_pct end), 2) as avg_discount_pct_kept,

    --shipping on returned items
    round(avg(case when is_returned then shipping_time_days end), 2) as avg_shipping_days_returned,
    round(avg(case when not is_returned then shipping_time_days end), 2) as avg_shipping_days_kept

from orders
group by
    category,
    subcategory,
    brand
;