{{
    config(
        materialized='table'
    )
}}

with
    raw as (
        select
            country_simplified as country,
            name as country_full_name,
            iso_2 as country_code_2,
            iso_3 as country_code_3,
            continent_name as continent,
            continent as continent_code,
            country_flag,
            internet as domain_ending,
            telephone as telephone_country_code,
            vehicle as car_code,
            currency_name,
            currency_code,
            currency_symbol,
            gdp_2021_worldbank,
            ranking_gdp_2021_worldbank
        from {{ source("imports", "country_codes") }}
    )
select *
from raw
