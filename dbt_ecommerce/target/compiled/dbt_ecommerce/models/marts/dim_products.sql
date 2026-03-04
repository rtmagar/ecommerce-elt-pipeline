

SELECT
    p.product_id,
    COALESCE(t.product_category_name_english, p.product_category_name, 'unknown') AS category_name,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
FROM "analytics_warehouse"."public"."stg_products" p
LEFT JOIN "analytics_warehouse"."public"."stg_category_translations" t
    ON p.product_category_name = t.product_category_name