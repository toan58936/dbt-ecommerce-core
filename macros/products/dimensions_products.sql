{% macro extract_product_dimensions(json_column) %}
    -- Bóc tách kích thước sản phẩm
    cast({{ json_column }} ->> 'width' as float) as width,
    cast({{ json_column }} ->> 'height' as float) as height,
    cast({{ json_column }} ->> 'depth' as float) as depth
{% endmacro %}