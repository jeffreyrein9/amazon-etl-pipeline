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

    --volume
    count(*) as total_orders,

    --revenue
    round(sum(final_price), 2) as total_revenue,

    --returns
    sum(case when is_returned then 1 else 0 end) as total_returns

from orders
group by
    category,
    subcategory
;