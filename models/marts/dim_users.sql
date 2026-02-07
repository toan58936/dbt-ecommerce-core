{{ config(materialized='table') }}

with stg_users as (
    select * from {{ ref('stg_users') }}
)
select
    user_id,
    full_name,
    email,
    gender,
    age,
    city,
    state,
    country,
    postal_code,
    company_name,
    department,
    job_title
from stg_users
