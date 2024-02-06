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
            ) as deal_id_row_number, 
            date(
                lag(stage_created_at) over (
                    partition by deal_id order by stage_created_at asc
                )
            ) as previous_stage_pipeline_created_at_date,
            lag(stage_name) over (
                partition by deal_id order by stage_created_at asc
            ) as previous_stage_pipeline
        from {{ source("source_us", "deal_pipeline_stages") }}
    ),
    final as (
        select
            *,
            date_diff(
                stage_pipeline_created_at_date,
                previous_stage_pipeline_created_at_date,
                day
            ) as day_absolute_variance_stage_pipeline,
            {{ label_stage_sales_cycle("previous_stage_pipeline") }} as previous_stage_sales_cycle,
        from raw
    )
select *
from final
