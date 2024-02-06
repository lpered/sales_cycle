{{ config(materialized="table") }}

with source_data as (select * from {{ ref("historical_category_by_country_gtrends") }})

select *
from source_data
