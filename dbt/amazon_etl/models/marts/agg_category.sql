/*
agg_category.sql
Category and subcategory performance for Tableau breakdown charts.
*/

with orders as (
    select *
    from {{ ref('fact_orders')}}
)

select
    category,
    subcategory,

    count(*) as total_orders,
    count(distinct user_id) as unique_customers,
    count(distinct product_id) as unique_products,
    sum(final_price) as total_revenue,
    round(avg(final_price), 2) as avg_order_value,
    round(avg(discount_pct), 2) as avg_discount_pct,
    round(avg(rating), 2) as avg_rating,
    sum(case when is_returned then 1 else 0 end) as total_returns,
    round(sum(case when is_returned then 1 else 0 end)
        / count(*) * 100, 2) as return_rate_pct

from orders
group by
    category,
    subcategory
;