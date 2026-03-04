
  create view "analytics_warehouse"."public"."stg_order_payments__dbt_tmp"
    
    
  as (
    SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
FROM "analytics_warehouse"."public"."raw_order_payments"
  );