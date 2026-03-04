SELECT
    geolocation_zip_code_prefix AS zip_code,
    geolocation_lat AS lat,
    geolocation_lng AS lng,
    geolocation_city AS city,
    geolocation_state AS state
FROM "analytics_warehouse"."public"."raw_geolocation"