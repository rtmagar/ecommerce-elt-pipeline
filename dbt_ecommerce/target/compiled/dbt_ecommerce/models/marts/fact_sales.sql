

WITH orders AS (
    SELECT * FROM "analytics_warehouse"."public"."stg_orders"

    -- THE TRUE INCREMENTAL LOGIC
    -- This now correctly pulls ANY order that was modified recently, 
    -- regardless of when it was originally purchased!
    
),
items AS (
    SELECT * FROM "analytics_warehouse"."public"."stg_order_items"
)

SELECT
    i.order_id,
    i.order_item_id,
    o.customer_id,
    i.product_id,
    i.seller_id,
    o.purchase_ts,
    o.delivered_ts,
    o.updated_at, -- Tracking the high-water mark
    i.price,
    i.freight_value,
    i.total_item_value
FROM items i
JOIN orders o ON i.order_id = o.order_id