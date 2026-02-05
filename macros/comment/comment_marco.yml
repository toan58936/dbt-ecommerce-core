{% macro extract_comment_user_info(json_column) %}
    -- Trích xuất thông tin User từ JSON embedded
    cast({{ json_column }} ->> 'id' as int) as user_id,
    cast({{ json_column }} ->> 'username' as text) as user_name,
    cast({{ json_column }} ->> 'fullName' as text) as user_full_name
{% endmacro %}