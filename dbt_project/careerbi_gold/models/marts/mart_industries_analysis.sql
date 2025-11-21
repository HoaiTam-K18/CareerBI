{{ config(materialized='table') }}

{{ analyze_kpis_by_dimension(
    dim_ref=ref('dim_industries'),
    dim_pk='industry_id',
    dim_name_col='industry_name',
    is_direct_join=True
) }}