{{ config(materialized='table') }}

with snapshots as (
    select * from {{ ref('products_snapshot') }}
)

select
    dbt_scd_id as product_sk,
    product_id,
    product_name,
    category, -- danh mục sản phẩm
    brand, -- thương hiệu sản phẩm
    sku, -- mã định danh duy nhất cho sản phẩm
    price,-- giá niêm yết không phải giá mua thực tế của khách hàng
    discount_percentage, -- phần trăm giảm giá áp dụng cho giá niêm yết
    stock_quantity, -- số lượng hàng tồn kho hiện có
    rating, -- đánh giá trung bình của sản phẩm
    min_order_qty, -- số lượng đặt hàng tối thiểu
    weight, -- trọng lượng sản phẩm tính bằng kg
    availability_status, -- trạng thái sẵn có của sản phẩm (còn hàng, hết hàng, đặt trước, v.v.)
    thumbnail, -- hình thu nhỏ của sản phẩm
    dbt_valid_from, -- thời gian bắt đầu hiệu lực của bản ghi
    dbt_valid_to -- thời gian kết thúc hiệu lực của bản ghi
from snapshots
