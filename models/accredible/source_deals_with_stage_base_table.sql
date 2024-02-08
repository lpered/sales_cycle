{{ config(materialized="table") }} 
select * from {{ ref("source_deals_with_stage_base") }}
