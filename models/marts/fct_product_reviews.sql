{{ config(materialized='table') }}

select
    review_id,
    product_id,
    rating,
    comment,
    review_date,
    reviewer_name,
    reviewer_email,
    loaded_at,
    review_date::date as review_day,
    to_char(review_date::date, 'YYYYMMDD')::int as date_key
from {{ ref('stg_product_reviews') }}
