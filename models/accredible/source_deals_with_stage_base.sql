with

    stages_raw as (select * from {{ ref("__deal_pipeline_stages") }}),
    deals_raw as (select * from {{ ref("__deals") }}),

    final as (
        select r.*, l.* except (deal_id, pipeline_id, pipeline, stage_pipeline)
        from stages_raw r
        left join
            deals_raw l
            on lower(r.deal_id) = lower(l.deal_id)
            and lower(r.pipeline_id) = lower(l.pipeline_id)
            and lower(r.pipeline) = lower(l.pipeline)
            and r.deal_id_row_number_most_recent = 1
    )
select
    * except (amount_in_home_currency),
    ifnull(amount_in_home_currency, 0) as amount_in_home_currency
from final
