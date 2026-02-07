{{ config(
    materialized='incremental',
    unique_key='cart_id'
) }}

with base as (
    select
        cart_id,
        user_id,
        cart_total_price,
        cart_discounted_price,
        total_unique_items,
        total_item_quantity,
        loaded_at
    from {{ ref('stg_carts') }}
    {% if is_incremental() %}
    where loaded_at > (select max(loaded_at) from {{ this }})
    {% endif %}
)

select
    cart_id,
    user_id,
    cart_total_price,
    cart_discounted_price,
    total_unique_items,
    total_item_quantity,
    loaded_at,
    loaded_at::date as cart_date,
    to_char(loaded_at::date, 'YYYYMMDD')::int as date_key
from base
