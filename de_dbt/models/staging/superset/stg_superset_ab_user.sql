{{ config(schema='odinprep_dbt_qa', materialized='table', unique_key='id', sort='id', dist='id', tags=["superset"]) }}
select
    active, 
	convert_timezone('America/New_York', changed_on) as changed_on, 
	convert_timezone('America/New_York', created_on) as created_on, 
	email, 
	fail_login_count, 
	first_name, 
	id, 
	last_login, 
	last_name, 
	login_count, 
	username,
    convert_timezone('America/New_York', getdate())  as dw_loaded_at    
from {{ source('superset', 'ab_user') }} -- superset.ab_user