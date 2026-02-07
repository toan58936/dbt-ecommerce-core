-- ADDED: data quality checks for stg_products numeric ranges
select *
from {{ ref('stg_products') }}
where price < 0
   or stock_quantity < 0
   or rating < 0
   or rating > 5
