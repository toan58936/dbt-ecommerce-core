{{ config(
    materialized='incremental',
    unique_key='cart_item_id'
) }}

with base as (
    select
        cart_item_id,
        cart_id,
        user_id,
        product_id,
        quantity,
        price_at_purchase,
        line_total,
        line_discount_percentage,
        line_discounted_total,
        loaded_at
    from {{ ref('stg_cart_items') }}
    {% if is_incremental() %}
    where loaded_at > (select max(loaded_at) from {{ this }})
    {% endif %}
)

select
    cart_item_id,
    cart_id,
    user_id,
    product_id,
    quantity,
    price_at_purchase,
    line_total,
    line_discount_percentage,
    line_discounted_total,
    loaded_at,
    loaded_at::date as cart_item_date,
    to_char(loaded_at::date, 'YYYYMMDD')::int as date_key
from base
