/*
agg_seller_risk.sql
Seller risk analysis: identifies high-volume sellers with high return rates and checks whether seller ratings
accurately reflect actual performance.
*/

with orders as (
    select *
    from {{ ref('fact_orders')}}
),

seller_stats as (
    select
        seller_id,

        --volume
        count(*) as total_orders,
        count(distinct user_id) as unique_customers,
        count(distinct product_id) as unique_products,

        --revenue
        cast(round(sum(final_price), 2) as decimal(15, 2)) as total_revenue,
        cast(round(avg(final_price), 2) as decimal(15, 2)) as avg_order_value,

        --returns
        sum(case when is_returned then 1 else 0 end) as total_returns,
        round(sum(case when is_returned then 1 else 0 end)
            / count(*) * 100, 2) as return_rate_pct,

        --returned revenue (money lost)
        cast(round(sum(case when is_returned then final_price else 0 end), 2) as decimal(15, 2)) as returned_revenue,

        --ratings
        round(avg(seller_rating), 2) as avg_seller_rating,
        round(avg(rating), 2) as avg_product_rating,

        --fulfillment
        round(avg(shipping_time_days), 2) as avg_shipping_days_kept

    from orders
    group by seller_id
),

--calculate revenue rank so Tableau can filter to top N sellers
ranked as (
    select
        *,
        rank() over (order by total_revenue desc) as revenue_rank,
        rank() over (order by return_rate_pct desc) as return_rate_rank,
        rank() over (order by total_returns desc) as total_returns_rank
    from seller_stats
),

--dynamically calculate the max return rate for normalization
return_rate_bounds as (
    select
        max(return_rate_pct) as max_return_rate
    from seller_stats
)

select
    r.*,

    --risk flag: high volume AND high return rate
    --defined as top 25% in both revenue and return rate
    case
        when revenue_rank <= (select count(*) * 0.25 from seller_stats)
            and return_rate_rank <= (select count(*) * 0.25 from seller_stats)
        then true
        else false
    end as high_risk_flag,

    --normalized rating vs return score
    --converts return_rate_pct to 0-5 scale using actual data range
    --negative or near-zero = seller rating is misleading
    round(r.avg_seller_rating - (r.return_rate_pct / nullif(b.max_return_rate, 0) * 5), 2) as rating_vs_return_score

from ranked r
cross join return_rate_bounds b
;