-- ADDED: data quality checks for stg_carts totals and quantities
select *
from {{ ref('stg_carts') }}
where cart_total_price < 0
   or cart_discounted_price < 0
   or cart_discounted_price > cart_total_price
   or total_unique_items < 0
   or total_item_quantity < 0
   or total_item_quantity < total_unique_items
