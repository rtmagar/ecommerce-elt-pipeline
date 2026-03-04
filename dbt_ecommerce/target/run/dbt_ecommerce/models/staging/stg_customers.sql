
  create view "analytics_warehouse"."public"."stg_customers__dbt_tmp"
    
    
  as (
    SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix AS zip_code,
    customer_city AS city,
    customer_state AS state
FROM "analytics_warehouse"."public"."raw_customers"
  );