
  
    

  create  table "analytics_warehouse"."public"."fact_reviews__dbt_tmp"
  
  
    as
  
  (
    

WITH orders AS (
    SELECT * FROM "analytics_warehouse"."public"."stg_orders"
),
reviews AS (
    SELECT * FROM "analytics_warehouse"."public"."stg_order_reviews"
)

SELECT
    r.review_id,
    r.order_id,
    o.customer_id,
    r.review_score,
    r.review_creation_ts,
    r.review_answer_ts
FROM reviews r
JOIN orders o ON r.order_id = o.order_id
  );
  