with
    deals_raw as (select * from {{ ref("__deals") }}),

    stages_raw as (
        select * from {{ ref("__deal_pipeline_stages") }} where deal_id_row_number = 1
    ),
    final as (
        select r.*, l.* except (deal_id, pipeline_id, pipeline, stage_pipeline)
        from deals_raw r
        left join stages_raw l using (deal_id, pipeline_id, pipeline, stage_pipeline)

    )

select *
from final
