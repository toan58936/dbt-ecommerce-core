{% macro extract_user_address(json_column) %}
    -- Bóc tách các trường cơ bản cấp 1
    {{ json_column }} ->> 'address' as street_address,
    {{ json_column }} ->> 'city' as city,
    {{ json_column }} ->> 'state' as state,
    {{ json_column }} ->> 'stateCode' as state_code,
    {{ json_column }} ->> 'postalCode' as postal_code,
    {{ json_column }} ->> 'country' as country,

    -- Bóc tách coordinates (JSON lồng nhau)
    cast({{ json_column }} -> 'coordinates' ->> 'lat' as float) as latitude,
    cast({{ json_column }} -> 'coordinates' ->> 'lng' as float) as longitude
{% endmacro %}