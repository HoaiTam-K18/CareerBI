{{ config(materialized='table') }}

{{ analyze_kpis_by_dimension(
    dim_ref=ref('dim_group_job_functions'),
    dim_pk='group_job_function_id',
    dim_name_col='group_job_function_name',
    is_direct_join=True
) }}