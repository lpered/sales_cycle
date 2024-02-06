{% macro label_stage_sales_cycle(column_name) %}

    case
        when lower(trim({{ column_name }})) in ('stalled', 'discovery')
        then 'Discovery'
        when
            lower(trim({{ column_name }})) in ('lead engaged', 'keep warm', 'qualified')
        then 'Qualified'
        when lower(trim({{ column_name }})) in ('proof of value')
        then 'Proof of Value'
        when lower(trim({{ column_name }})) in ('proposal/pricing')
        then 'Proposal/Pricing'
        when lower(trim({{ column_name }})) in ('procurement/negotiation')
        then 'Procurement/Negotiation'
        when lower(trim({{ column_name }})) like '%closed%'
        then 'Closed Won / Closed Lost'
        else 'NOT LABELLED'
    end

{% endmacro %}
