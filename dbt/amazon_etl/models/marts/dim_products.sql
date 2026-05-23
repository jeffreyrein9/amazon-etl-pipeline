/*
dim_products.sql
One row per product with performance summary.
*/

with orders as (
    select *
    from {{ ref('fact_orders')}}
)

select
    product_id,
    category,
    subcategory,
    brand,

    --pricing
    round(avg(price), 2) as avg_list_price,
    round(avg(final_price), 2) as avg_selling_price,
    round(avg(discount_pct), 2) as avg_discount_pct,

    --volume
    count(*) as total_orders,
    round(sum(final_price), 2) as total_revenue,

    --quality signals
    round(avg(rating), 2) as avg_rating,
    sum(review_count) as total_reviews,

    --returns
    sum(case when is_returned then 1 else 0 end) as total_returns,
    round(sum(case when is_returned then 1 else 0 end)
        / count(*) * 100, 2) as return_rate_pct

from orders
group by
    product_id,
    category,
    subcategory,
    brand
;