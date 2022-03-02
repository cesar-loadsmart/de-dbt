{{ config(schema='odinprep_dbt_qa', materialized='table', unique_key='id', sort='id', dist='id', tags=["superset"]) }}
SELECT 
    id,
    allow_run_async, 
    cache_timeout, 
    changed_by_fk, 
    convert_timezone('America/New_York', changed_on) as changed_on, 
    configuration_method, 
    created_by_fk, 
    convert_timezone('America/New_York', created_on) as created_on, 
    database_name,
    expose_in_sqllab, 
    extra, 
    force_ctas_schema,
    impersonate_user, 
    select_as_create_table_as, 
    sqlalchemy_uri,
    convert_timezone('America/New_York', getdate())  as dw_loaded_at
from {{ source('superset', 'dbs') }} 
