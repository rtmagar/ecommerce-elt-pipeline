
  create view "analytics_warehouse"."public"."stg_order_items__dbt_tmp"
    
    
  as (
    SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price,
    freight_value,
    (price + freight_value) AS total_item_value
FROM "analytics_warehouse"."public"."raw_order_items"
  );