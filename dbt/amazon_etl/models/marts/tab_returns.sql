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

    --revenue
    cast(round(sum(final_price), 2) as decimal(15, 2)) as total_revenue,

    --returns
    sum(case when is_returned then 1 else 0 end) as total_returns,

    --returned revenue (money lost)
    cast(round(sum(case when is_returned then final_price else 0 end), 2) as decimal(15, 2)) as returned_revenue

from orders
group by
    category,
    subcategory,
    brand
;