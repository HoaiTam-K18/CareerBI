{{ analyze_kpis_by_dimension(
    dim_ref=ref('dim_industries'), 
    bridge_ref=ref('fct_job_industries'), 
    dim_pk='industry_id', 
    dim_name_col='industry_name'
) }}