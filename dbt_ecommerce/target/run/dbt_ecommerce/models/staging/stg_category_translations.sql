
  create view "analytics_warehouse"."public"."stg_category_translations__dbt_tmp"
    
    
  as (
    SELECT
    product_category_name,
    product_category_name_english
FROM "analytics_warehouse"."public"."raw_product_category_name_translation"
  );