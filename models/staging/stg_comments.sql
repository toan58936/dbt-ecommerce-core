with source as (
    select * from {{ source('ecommerce_source', 'comments') }}
),

renamed as (
    select
        -- 1. Identifiers
        id as comment_id,
        "postId" as product_id, -- Trong DummyJSON, postId chính là ID của sản phẩm

        -- 2. Content & Metrics
        body as comment_text,
        likes::int as likes_count,

        -- 3. Thông tin người dùng (Lưu ý: "user" phải để trong ngoặc kép vì là từ khóa hệ thống)
        cast("user" as jsonb) as user_info, 

        -- 4. Metadata
        _airbyte_extracted_at as loaded_at

    from source
)

select
    comment_id,
    product_id,
    comment_text,
    likes_count,
    
    {{ extract_comment_user_info('user_info') }},
    loaded_at
from renamed