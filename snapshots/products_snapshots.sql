{% snapshot products_snapshot %}
{{
    config(
        target_schema=target.schema,
        target_database=target.database,
    
    unique_key='product_id',
    strategy='check', 
    check_cols=['price','stock_quantity','discount_percentage','rating','availability_status','min_order_qty','weight']
    )
}}
select
  product_id,
  price,
  stock_quantity,
  discount_percentage,
  rating,
  availability_status,
  min_order_qty,
  weight
from {{ ref('stg_products') }}
{% endsnapshot %}