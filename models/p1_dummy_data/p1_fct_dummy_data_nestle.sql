with 

source as (

    select * from {{ source('source_us', 'dummy_data_nestle') }}

),

final as (

    select
        date,
        site,
        market,
        division,
        campaign,
        insertion_order,
        tactics,
        adset,
        format,
        ad,
        product,
        spend,
        impressions,
        impression_share,
        frequency,
        total_audience_size,
        reached_audience_perc,
        clicks,
        conversions,
        video_p25_watched,
        video_p50_watched,
        video_p75_watched,
        video_p100_watched,
        reached_audience_size,
        link_clicks,
        market_flag,
        market_currency,
        market_currency_symbol

    from source

)

select * from final
