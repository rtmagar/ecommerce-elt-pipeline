
  create view "analytics_warehouse"."public"."stg_orders__dbt_tmp"
    
    
  as (
    SELECT
    order_id,
    customer_id,
    order_status,
    CAST(order_purchase_timestamp AS TIMESTAMP) AS purchase_ts,
    CAST(order_delivered_customer_date AS TIMESTAMP) AS delivered_ts,
    
    -- THE REAL-WORLD TRICK: Synthetic updated_at
    -- Finds the most recent event in the order's lifecycle
    GREATEST(
        CAST(order_purchase_timestamp AS TIMESTAMP),
        COALESCE(CAST(order_approved_at AS TIMESTAMP), CAST(order_purchase_timestamp AS TIMESTAMP)),
        COALESCE(CAST(order_delivered_carrier_date AS TIMESTAMP), CAST(order_purchase_timestamp AS TIMESTAMP)),
        COALESCE(CAST(order_delivered_customer_date AS TIMESTAMP), CAST(order_purchase_timestamp AS TIMESTAMP))
    ) AS updated_at

FROM "analytics_warehouse"."public"."raw_orders"
  );