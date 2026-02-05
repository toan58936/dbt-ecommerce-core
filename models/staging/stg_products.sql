with source as (
    select * from {{ source('ecommerce_source', 'products') }}
),

renamed as (
    select
        -- 1. Identifiers & Basic Info (GIỮ)
        id as product_id,
        title as product_name,
        sku,
        brand,
        category,
        
        -- 2. Business Metrics (GIỮ & ÉP KIỂU)
        cast(price as numeric(16,2)) as price,
        cast("discountPercentage" as numeric(5,2)) as discount_percentage,
        cast(stock as int) as stock_quantity,
        cast(rating as numeric(3,2)) as rating,
        cast("minimumOrderQuantity" as int) as min_order_qty,
        
        -- 3. Operations Info (GIỮ)
        cast(weight as numeric(10,2)) as weight,
        "availabilityStatus" as availability_status,
        
        -- 4. UI Display (GIỮ Thumbnail, BỎ Images Array)
        thumbnail,

        -- 5. Data Structures
        cast(dimensions as jsonb) as dimensions_info, -- Giữ để macro xử lý
        _airbyte_extracted_at as loaded_at
        
        -- [ĐÃ LOẠI BỎ]: description, images, meta (barcode/qr)

    from source
),

final as (
    select
        product_id,
        product_name,
        sku,
        brand,
        category,
        price,
        discount_percentage,
        stock_quantity,
        rating,
        min_order_qty,
        weight,
        availability_status,
        thumbnail,
        
        -- ÁP DỤNG MACRO (Tự động tách width, height, depth)
        {{ extract_product_dimensions('dimensions_info') }},

        loaded_at

    from renamed
)

select * from final