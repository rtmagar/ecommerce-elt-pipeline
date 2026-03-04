
  
    

  create  table "analytics_warehouse"."public"."dim_sellers__dbt_tmp"
  
  
    as
  
  (
    

SELECT DISTINCT
    seller_id,
    zip_code,
    city,
    state
FROM "analytics_warehouse"."public"."stg_sellers"
  );
  