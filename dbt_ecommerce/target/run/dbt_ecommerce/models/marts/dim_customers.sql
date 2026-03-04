
  
    

  create  table "analytics_warehouse"."public"."dim_customers__dbt_tmp"
  
  
    as
  
  (
    

SELECT DISTINCT
    customer_id,
    customer_unique_id,
    zip_code,
    city,
    state
FROM "analytics_warehouse"."public"."stg_customers"
  );
  