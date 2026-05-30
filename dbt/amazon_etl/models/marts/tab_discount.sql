/*
agg_discount.sql
Discount effectiveness analysis.
Answers:
    Are discounts driving volume, or just killing margins?
    Are heavily discounted items returned more?
*/

with orders as (
    select *
    from {{ ref('fact_orders')}}
),

--bucket orders into discount bands for easy Tableau grouping
banded as (
    select
        *,
        case
            when discount_pct = 0 then '0%'
            when discount_pct > 0
                and discount_pct <= 10 then '1-10%'
            when discount_pct > 10
                and discount_pct <= 20 then '11-20%'
            when discount_pct > 20
                and discount_pct <= 30 then '21-30%'
            when discount_pct > 30
                and discount_pct <= 40 then '31-40%'
            when discount_pct > 40
                and discount_pct <= 50 then '41-50%'
            else '50%+'
        end as discount_band,

        --used for sorting in Tableau
        case
            when discount_pct = 0 then 1
            when discount_pct > 0
                and discount_pct <= 10 then 2
            when discount_pct > 10
                and discount_pct <= 20 then 3
            when discount_pct > 20
                and discount_pct <= 30 then 4
            when discount_pct > 30
                and discount_pct <= 40 then 5
            when discount_pct > 40
                and discount_pct <= 50 then 6
            else 7
        end as discount_band_sort
    
    from orders
)

select  
    discount_band,
    discount_band_sort,
    category,

    --volume
    count(*) as total_orders,

    --revenue
    cast(round(sum(final_price), 2) as decimal(15, 2)) as total_revenue,
    sum(case when not is_returned then final_price else 0 end) as net_revenue,

    --returns
    sum(case when is_returned then 1 else 0 end) as total_returns,
    sum(case when is_returned then final_price else 0 end) as returned_revenue,

    --quality signal: do discounted items have lower ratings?
    round(avg(rating), 2) as avg_rating

from banded
group by
    discount_band,
    discount_band_sort,
    category
order by
    discount_band_sort
;