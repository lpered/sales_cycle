with
    raw as (
        select
            date(created_at) as created_at_date,
            created_at as created_at,
            cast(deal_id as string) as deal_id,
            cast(pipeline_id as string) as pipeline_id,
            pipeline_name as pipeline,
            stage_name as stage_pipeline,
            {{label_stage_sales_cycle('stage_name')}} as stage_sales_cycle,
            amount_in_home_currency,
            "USD" as currency,
            date(close_date) as close_date,
            close_date as close_date_timestamp,
            deal_source_type as deal_source_type
        from {{ source("source_us", "deals") }}
    ),
    final as (
        select
            *,
            case
                when lower(stage_sales_cycle) = 'discovery'
                then 1
                when lower(stage_sales_cycle) = 'qualified'
                then 2
                when lower(stage_sales_cycle) = 'proof of value'
                then 3
                when lower(stage_sales_cycle) = 'proposal/pricing'
                then 4
                when lower(stage_sales_cycle) = 'procurement/negotiation'
                then 5
                when lower(stage_sales_cycle) = 'closed won / closed lost'
                then 6
                else 0
            end as stage_sales_cycle_display_order

        from raw
    )
select *

from final
