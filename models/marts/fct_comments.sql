{{ config(materialized='table') }}

select
    comment_id,
    post_id,
    user_id,
    comment_text,
    likes_count,
    loaded_at,
    loaded_at::date as comment_day,
    to_char(loaded_at::date, 'YYYYMMDD')::int as date_key
from {{ ref('stg_comments') }}
