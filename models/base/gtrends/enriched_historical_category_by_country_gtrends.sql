with
    historical as (
        select * from {{ ref("historical_category_by_country_gtrends_table") }}
    ),
    country_codes_enrichment as (
        select
            country as country_name,
            country_code_2,
            continent,
            continent_code,
            country_flag,
            domain_ending,
            currency_name,
            currency_symbol
        from {{ ref("raw_country_codes_gsheets") }}
    ),
    final as (
        select historical.*,
        CONCAT("https://trends.google.com/trends/explore?cat=",historical.category,"&geo=",historical.country) AS url,
         country_codes_enrichment.* except (country_code_2), 
        from historical
        left join
            country_codes_enrichment
            on historical.country = country_codes_enrichment.country_code_2
    )

select *
from final
