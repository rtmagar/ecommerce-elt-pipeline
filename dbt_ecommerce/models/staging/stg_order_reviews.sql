SELECT
    review_id,
    order_id,
    review_score,
    CAST(review_creation_date AS TIMESTAMP) AS review_creation_ts,
    CAST(review_answer_timestamp AS TIMESTAMP) AS review_answer_ts
FROM {{ source('olist', 'raw_order_reviews') }}