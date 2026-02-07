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
        -- Hàm nhân bản dòng cart_id cho mỗi sản phẩm
        jsonb_array_elements(coalesce(products_json, '[]'::jsonb)) as item,
        _airbyte_extracted_at
    from source
)

select
    -- 1. Surrogate Key (Nâng cấp với COALESCE)
    -- Đảm bảo nếu id bị null thì thay bằng chuỗi rỗng '', giúp md5 luôn hoạt động
    md5(cast(concat(
        coalesce(cast(cart_id as text), ''), 
        '-', 
        coalesce(cast(item ->> 'id' as text), '')
    ) as text)) as cart_item_id,

    -- 2. Foreign Keys
    cart_id,
    cast(item ->> 'id' as int) as product_id,
    user_id,

    -- 3. Item Metrics
    cast(item ->> 'quantity' as int) as quantity,
    cast(item ->> 'price' as numeric(16,2)) as price_at_purchase, -- Giá chốt tại thời điểm mua
    cast(item ->> 'total' as numeric(16,2)) as line_total,
    
    cast(item ->> 'discountPercentage' as numeric(5,2)) as line_discount_percentage,
    cast(item ->> 'discountedTotal' as numeric(16,2)) as line_discounted_total,

    -- 4. Metadata
    _airbyte_extracted_at as loaded_at

from flattened