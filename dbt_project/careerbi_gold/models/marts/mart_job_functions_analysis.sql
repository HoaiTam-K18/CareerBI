{{ analyze_kpis_by_dimension(
    dim_ref=ref('dim_group_job_functions'), 
    bridge_ref=ref('fct_job_group_jobfunctions'), 
    dim_pk='group_job_function_id', 
    dim_name_col='group_job_function_name'
) }}