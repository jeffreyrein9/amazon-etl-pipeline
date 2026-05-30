/*
agg_daily_revenue.sql
Daily revenue time series. Primary source for Tableau trend charts.
*/

with orders as (
    select *
    from {{ ref('fact_orders')}}
)

select
    purchase_date,
    purchase_week,
    purchase_month,
    purchase_year,

    --volume
    count(*) as total_orders,

    --revenue
    round(sum(final_price), 2) as total_revenue
    sum(case when not is_returned then final_price else 0 end) as net_revenue

from orders
group by
    purchase_date,
    purchase_week,
    purchase_month,
    purchase_year
order by purchase_date
;