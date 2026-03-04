{{ config(materialized='table') }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
reviews AS (
    SELECT * FROM {{ ref('stg_order_reviews') }}
)

SELECT
    r.review_id,
    r.order_id,
    o.customer_id,
    r.review_score,
    r.review_creation_ts,
    r.review_answer_ts
FROM reviews r
JOIN orders o ON r.order_id = o.order_id