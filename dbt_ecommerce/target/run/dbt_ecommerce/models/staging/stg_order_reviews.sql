
  create view "analytics_warehouse"."public"."stg_order_reviews__dbt_tmp"
    
    
  as (
    SELECT
    review_id,
    order_id,
    review_score,
    CAST(review_creation_date AS TIMESTAMP) AS review_creation_ts,
    CAST(review_answer_timestamp AS TIMESTAMP) AS review_answer_ts
FROM "analytics_warehouse"."public"."raw_order_reviews"
  );