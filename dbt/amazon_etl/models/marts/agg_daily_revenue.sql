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

    count(*) as total_orders,
    count(distinct user_id) as unique_customers,
    sum(final_price) as total_revenue,
    round(avg(final_price), 2) as avg_order_value,
    sum(discount_amount) as total_discount_given,
    sum(case when is_returned then 1 else 0 end) as total_returns,
    sum(case when is_returned then final_price else 0 end) as returned_revenue

from orders
group by
    purchase_date,
    purchase_week,
    purchase_month,
    purchase_year
order by purchase_date
;