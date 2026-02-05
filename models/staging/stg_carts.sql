with source as (
    select * from {{ source('ecommerce_source', 'carts') }}
),

renamed as (
    select
        -- 1. IDs
        id as cart_id,
        userId as user_id,

        -- 2. Metrics (Số liệu tổng quan)
        total as cart_total,
        discountedTotal as discounted_total,
        totalProducts as unique_products_count, -- Số lượng mã sản phẩm khác nhau
        totalQuantity as total_items_quantity,  -- Tổng số lượng hàng trong giỏ

        -- 3. Complex Data (Cột khó nhất - Giữ nguyên dưới dạng JSONB)
        -- Lưu ý: Neon/Postgres hỗ trợ JSONB rất mạnh để truy vấn sau này
        cast(products as jsonb) as cart_items_json,

        -- 4. Metadata
        _airbyte_extracted_at as loaded_at

    from source
)

select * from renamed