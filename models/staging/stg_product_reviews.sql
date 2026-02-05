with source as (
    select 
        id as product_id,
        -- Ép kiểu sang JSONB ngay từ đầu để tối ưu hiệu năng
        cast(reviews as jsonb) as reviews_raw,
        _airbyte_extracted_at
    from {{ source('ecommerce_source', 'products') }}
    where reviews is not null -- Chỉ lấy những sản phẩm có review
),

flattened as (
    select
        product_id,
        -- Hàm 'đập' mảng: Biến 1 dòng sản phẩm thành N dòng review
        jsonb_array_elements(reviews_raw) as review_item,
        _airbyte_extracted_at
    from source
)

select
    -- 1. Tạo Surrogate Key (ID duy nhất cho mỗi review)
    -- Dùng MD5 hash các trường unique để tạo ID cố định
    md5(cast(concat(product_id, '-', review_item ->> 'reviewerEmail', '-', review_item ->> 'date') as text)) as review_id,
    
    -- 2. Khóa ngoại (Foreign Key)
    product_id,
    
    -- 3. Thông tin chi tiết Review
    cast(review_item ->> 'rating' as int) as rating,
    cast(review_item ->> 'comment' as text) as comment,
    cast(review_item ->> 'date' as timestamp) as review_date,
    cast(review_item ->> 'reviewerName' as text) as reviewer_name,
    cast(review_item ->> 'reviewerEmail' as text) as reviewer_email,
    
    -- 4. Metadata
    _airbyte_extracted_at as loaded_at

from flattened