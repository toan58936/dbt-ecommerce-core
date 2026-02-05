with source as (
    select * from {{ source('ecommerce_source', 'comments') }}
),

renamed as (
    select
        -- 1. Identifiers
        id as comment_id,
        postId as product_id, -- Mapping postId -> product_id

        -- 2. Content & Metrics
        body as comment_body,
        likes,

        -- 3. Complex Data (User info đang nằm trong JSON object)
        -- Postgres yêu cầu ngoặc kép "user" vì trùng tên từ khóa hệ thống
        cast("user" as jsonb) as user_info, 

        -- 4. Metadata
        _airbyte_extracted_at as loaded_at

    from source
)

select * from renamed