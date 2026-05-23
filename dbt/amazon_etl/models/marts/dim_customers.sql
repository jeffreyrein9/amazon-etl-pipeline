/*
dim_customers.sql
One row per customer with lifetime behavioral summary.
*/

with orders as (
    select *
    --dbt automatically resolves the right schema depending on whether you're in dev or prod
    from {{ ref('fact_orders')}}
)

select
    user_id,

    --purchase behavior
    count(*) as total_orders,
    sum(final_price) as lifetime_value,
    round(avg(final_price), 2) as avg_order_value,
    min(purchase_date) as first_purchase_date,
    max(purchase_date) as last_purchase_date,
    datediff(max(purchase_date),
            min(purchase_date)) as customer_tenure_days,

    --preferences
    count(distinct category) as unique_categories,
    count(distinct product_id) as unique_products,

    --returns
    sum(case when is_returned then 1 else 0 end) as total_returns,
    round(sum(case when is_returned then 1 else 0 end)
        / count(*) * 100, 2) as return_rate_pct,
    
    --most frequently used payment and device
    mode(payment_method) as preferred_payment,
    mode(device) as preferred_device,
    mode(location) as primary_location

from orders
group by user_id
;