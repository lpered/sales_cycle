with
    raw as (
        select date, source, category, country, sum(hits) as hits
        from {{ ref("raw_category_by_country_gtrends") }}
        group by 1, 2, 3, 4
    ),
    params as (
        select distinct
            source,
            category,
            country,
            max(date) over (partition by source, category, country) as max_date,
            date_add(
                max(date) over (partition by source, category, country),
                interval - 7 day
            ) as max_date_minus_7,
            date_add(
                max(date) over (partition by source, category, country),
                interval - 63 day
            ) as max_date_minus_63,
            extract(
                year from (max(date) over (partition by source, category, country))
            ) as year_max_date,
            extract(year from (max(date) over (partition by source, category, country)))
            - 1 as year_last_year,

        from raw
    ),
    labels as (
        select
            raw.*,
            -- Simple_Moving_average_to_decrease_noise 
            avg(raw.hits) over (
                partition by raw.source, raw.category, raw.country
                order by raw.date asc
                rows between 3 preceding and current row
            -- rows between 6 preceding and current row
            ) as sma7,
            extract(week from date) as week_num,
            extract(year from date) as year,
        from raw
        join params using (source, category, country)
        where date <> max_date  -- Remove_max_date_as_it_is_skewed 
    ),
    segment_last_weeks as (
        select

            date,
            week_num,
            year,
            labels.source,
            labels.category,
            labels.country,
            hits,

            avg(hits) over (
                partition by labels.source, labels.category, labels.country
                order by date asc
                rows between 3 preceding and current row
            ) as sma4,

            avg(hits) over (
                partition by labels.source, labels.category, labels.country
                order by date asc
                rows between 7 preceding and current row

            ) as sma8,

        from labels
        join params using (source, category, country)
        where date between params.max_date_minus_63 and params.max_date
    ),
    segment_last_weeks_filter as (
        select segment_last_weeks.*, (sma4 - sma8) as trend
        from segment_last_weeks
        join params using (source, category, country)
        where date between params.max_date_minus_7 and params.max_date

    ),
    combined_data_with_trend as (
        select labels.*, trend
        from labels
        left join segment_last_weeks_filter using (source, category, country)

    ),  -- group_so_that_we_can_lag
    group_dims_for_lag_yoy as (
        select
            date,
            week_num,
            year,
            source,
            category,
            country,
            sum(hits) as hits,
            sum(sma7) as sma7,
            sum(trend) as trend,
        from combined_data_with_trend
        group by 1, 2, 3, 4, 5, 6
    ),

    yoy as (
        select
            *,
            -- yoy_each_record_is_a_week
            lag(hits, 52, 0) over (
                partition by source, category, country order by date asc
            ) as hits_last_year_abs,

        from group_dims_for_lag_yoy
    ),
    sma_for_last_year as (
        select
            *,
            avg(hits_last_year_abs) over (
                partition by source, category, country
                order by date asc
                rows between 3 preceding and current row
            -- rows between 6 preceding and current row
            ) as last_year_sma7,
        from yoy
    ),
    trend_growth_yoy as (
        select
            date,
            source,
            category,
            country,
            sma7,
            last_year_sma7,
            (sma7 - last_year_sma7) as trend_abs_growth,
            safe_divide((sma7 - last_year_sma7), last_year_sma7) as trend_perc_growth,
        from sma_for_last_year
        left join params using (source, category, country)
        where sma_for_last_year.date = params.max_date_minus_7
    ),
    final as (
        select
            sma_for_last_year.*,
            ifnull(trend_abs_growth, 0) as trend_abs_growth,
            ifnull(trend_perc_growth, 0) as trend_perc_growth
        from sma_for_last_year
        left join trend_growth_yoy using (date, source, category, country)
    )

select *,


from final
