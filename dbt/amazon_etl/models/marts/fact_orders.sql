/*
fact_orders.sql
One row per transaction. Foundation for all other marts.
Tableau can use this for any custom aggregation or filter.
*/

with source as (
    select *
    from workspace.silver.amazon_clean
)

select
    --keys
    user_id,
    product_id,
    seller_id,

    --time
    purchase_date,
    date_trunc('month', purchase_date) as purchase_month,
    date_trunc('week', purchase_date) as purchase_week,
    year(purchase_date) as purchase_year,

    --product
    category,
    subcategory,
    brand,

    --financials
    price,
    discount,
    final_price,
    round(price - final_price, 2) as discount_amount,
    round((price - final_price)
        / nullif(price, 0) * 100, 2) as discount_pct,

    --customer behavior
    device,
    payment_method,
    location,
    is_returned,

    --fulfillment
    shipping_time_days,
    delivery_status,

    --seller
    seller_rating,

    --product quality signals
    rating,
    review_count,

    --metadata
    row_hash,
    ingest_ts

from source
;