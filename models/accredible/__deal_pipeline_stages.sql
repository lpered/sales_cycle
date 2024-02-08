with
    raw as (
        select
            date(stage_created_at) as stage_pipeline_created_at_date,
            stage_created_at as stage_pipeline_created_at_timestamp,
            cast(deal_id as string) as deal_id,
            cast(pipeline_id as string) as pipeline_id,
            pipeline_name as pipeline,
            deal_stage_id as stage_pipeline_id,
            stage_name as stage_pipeline,
            stage_display_order as stage_pipeline_display_order,
            row_number() over (
                partition by deal_id order by stage_created_at desc
            ) as deal_id_row_number_most_recent,
            date(
                lag(stage_created_at) over (
                    partition by deal_id order by stage_created_at asc
                )
            ) as previous_stage_pipeline_created_at_date,
            lag(stage_name) over (
                partition by deal_id order by stage_created_at asc
            ) as previous_stage_pipeline,

            date(
                nth_value(stage_created_at, 1) over (
                    partition by deal_id
                    order by stage_created_at asc
                    range between unbounded preceding and unbounded following
                )
            ) as stage_pipeline_first_date,

            nth_value(stage_name, 1) over (
                partition by deal_id
                order by stage_created_at asc
                range between unbounded preceding and unbounded following
            ) as stage_pipeline_first,

            date(
                nth_value(stage_created_at, 1) over (
                    partition by deal_id
                    order by stage_created_at desc
                    range between unbounded preceding and unbounded following
                )
            ) as stage_pipeline_last_date,

            nth_value(stage_name, 1) over (
                partition by deal_id
                order by stage_created_at desc
                range between unbounded preceding and unbounded following
            ) as stage_pipeline_last,
        from {{ source("source_us", "deal_pipeline_stages") }}
    ),
    final as (
        select
            *,
            date_diff(
                stage_pipeline_last_date, stage_pipeline_first_date, day
            ) as stage_pipeline_total_days,

            {{ label_stage_sales_cycle("previous_stage_pipeline") }}
            as stage_sales_cycle_previous,
            {{ label_stage_sales_cycle("stage_pipeline_first") }}
            as stage_sales_cycle_first,
            {{ label_stage_sales_cycle("stage_pipeline_last") }}
            as stage_sales_cycle_last,
        from raw
    )
select *
from final
