with source as (
    select * from {{ source('ecommerce_source', 'users') }}
),

renamed as (
    select
        -- 1. Identifiers (BỎ Password)
        id as user_id,
        username,
        email,
        "firstName" as first_name,
        "lastName" as last_name,
        
        -- 2. Demographics (GIỮ Age, Gender để phân khúc)
        gender,
        cast(age as int) as age,
        cast("birthDate" as date) as birth_date,
        
        -- 3. Geography & Segmentation (Chuẩn bị JSON)
        cast(address as jsonb) as address_info,
        cast(company as jsonb) as company_info,

        -- 4. Metadata
        _airbyte_extracted_at as loaded_at

        -- [ĐÃ LOẠI BỎ TOÀN BỘ]:
        -- hair, eyeColor, bloodGroup, height, weight (Rác vật lý)
        -- bank, crypto, ssn, ein (Rủi ro bảo mật & Tài chính)
        -- macAddress, ip, userAgent (Rác kỹ thuật - trừ khi cần IP để map location)

    from source
),

final as (
    select
        user_id,
        username,
        email,
        -- Tạo Full Name tiện lợi
        concat(first_name, ' ', last_name) as full_name,
        gender,
        age,
        birth_date,

        -- SỬ DỤNG MACRO: Address (Cực quan trọng cho Map Chart)
        {{ extract_user_address('address_info') }},

        -- SỬ DỤNG MACRO: Company (Lấy Name & Title để phân tích B2B)
        -- (Macro này sẽ lấy cả địa chỉ cty, nhưng không sao, thừa chút không đáng kể)
        {{ extract_company_info('company_info') }},

        loaded_at
    from renamed
)

select 
    user_id,
    full_name,
    email,
    gender,
    age,
    
    -- Chỉ select các cột cần thiết từ kết quả Macro (Flattening triệt để)
    city,
    state,
    country,
    postal_code,
    
    -- Lấy thông tin công ty
    company_name,
    department, -- Dùng để phân tích nhóm ngành nghề khách hàng
    job_title,

    loaded_at
from final