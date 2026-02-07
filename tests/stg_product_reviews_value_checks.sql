-- ADDED: data quality checks for stg_product_reviews rating range
select *
from {{ ref('stg_product_reviews') }}
where rating is null
   or rating < 1
   or rating > 5
