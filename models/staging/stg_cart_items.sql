with source as (
    select
        id as cart_id,
        "userId" as user_id,
        cast(products as jsonb) as products_json,
        _airbyte_extracted_at
    from {{ source('ecommerce_source', 'carts') }}
),

flattened as (
    select
        cart_id,
        user_id,
        -- OLD: jsonb_array_elements(coalesce(products_json, '[]'::jsonb)) as item
        -- CHANGED: use LATERAL + WITH ORDINALITY to avoid duplicate cart_item_id
        item.value as item,
        item.idx as idx,
        _airbyte_extracted_at
    from source
    cross join lateral jsonb_array_elements(coalesce(products_json, '[]'::jsonb)) with ordinality as item(value, idx)
)

select
    -- 1. Surrogate Key
    -- OLD: md5(concat(cart_id, '-', item ->> 'id'))
    -- CHANGED: add idx to guarantee uniqueness when same product repeats in a cart
    md5(cast(concat_ws(
        '-',
        coalesce(cast(cart_id as text), ''),
        coalesce(cast(item ->> 'id' as text), ''),
        coalesce(cast(idx as text), '')
    ) as text)) as cart_item_id,

    -- 2. Foreign Keys
    cart_id,
    cast(item ->> 'id' as int) as product_id,
    user_id,

    -- 3. Item Metrics
    cast(item ->> 'quantity' as int) as quantity,
    cast(item ->> 'price' as numeric(16,2)) as price_at_purchase, -- price at purchase time
    cast(item ->> 'total' as numeric(16,2)) as line_total,
    
    cast(item ->> 'discountPercentage' as numeric(5,2)) as line_discount_percentage,
    cast(item ->> 'discountedTotal' as numeric(16,2)) as line_discounted_total,

    -- 4. Metadata
    _airbyte_extracted_at as loaded_at

from flattened
