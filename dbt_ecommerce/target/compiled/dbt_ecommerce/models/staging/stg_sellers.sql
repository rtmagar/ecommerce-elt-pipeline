SELECT
    seller_id,
    seller_zip_code_prefix AS zip_code,
    seller_city AS city,
    seller_state AS state
FROM "analytics_warehouse"."public"."raw_sellers"