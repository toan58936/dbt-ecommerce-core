with source as (
    select * from {{ source('ecommerce_source', 'carts') }}
),

renamed as (
    select
        -- 1. Identifiers
        id as cart_id,
        userId as user_id,

        -- 2. Metrics (Header Level)
        -- Ép kiểu numeric để tính toán tiền tệ chính xác
        cast(total as numeric(16,2)) as cart_total_price,
        cast("discountedTotal" as numeric(16,2)) as cart_discounted_price,
        
        cast("totalProducts" as int) as total_unique_items, -- Số loại sản phẩm (SKUs)
        cast("totalQuantity" as int) as total_item_quantity, -- Tổng số lượng hàng

        -- 3. Metadata
        _airbyte_extracted_at as loaded_at

        -- [LOẠI BỎ]: products (Vì sẽ xử lý ở bảng stg_cart_items)

    from source
)

select * from renamed