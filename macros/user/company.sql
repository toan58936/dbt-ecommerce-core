{% macro extract_company_info(json_column) %}
    -- 1. Thông tin chung về công ty & vị trí
    {{ json_column }} ->> 'name' as company_name,
    {{ json_column }} ->> 'title' as job_title,
    {{ json_column }} ->> 'department' as department,

    -- 2. Địa chỉ công ty (Lồng bên trong key 'address')
    -- Lưu ý: đi vào key 'address' trước rồi mới lấy các trường con
    {{ json_column }} -> 'address' ->> 'address' as company_street_address,
    {{ json_column }} -> 'address' ->> 'city' as company_city,
    {{ json_column }} -> 'address' ->> 'state' as company_state,
    {{ json_column }} -> 'address' ->> 'country' as company_country,
    
    -- 3. Tọa độ công ty (Lồng 3 lớp: json_column -> address -> coordinates -> lat)
    -- OLD: cast({{ json_column }} -> 'address' -> 'coordinates' ->> 'lat' as float) as company_latitude
    -- CHANGED: nullif để tránh lỗi khi rỗng
    cast(nullif({{ json_column }} -> 'address' -> 'coordinates' ->> 'lat', '') as float) as company_latitude,
    -- OLD: cast({{ json_column }} -> 'address' -> 'coordinates' ->> 'lng' as float) as company_longitude
    -- CHANGED: nullif để tránh lỗi khi rỗng
    cast(nullif({{ json_column }} -> 'address' -> 'coordinates' ->> 'lng', '') as float) as company_longitude

{% endmacro %}
