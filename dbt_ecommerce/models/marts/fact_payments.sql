{{ config(materialized='table') }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
payments AS (
    SELECT * FROM {{ ref('stg_order_payments') }}
)

SELECT
    p.order_id,
    o.customer_id,
    p.payment_sequential,
    p.payment_type,
    p.payment_installments,
    p.payment_value,
    o.purchase_ts
FROM payments p
JOIN orders o ON p.order_id = o.order_id