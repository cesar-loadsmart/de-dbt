{{ config(schema='odinprep_dbt_qa', materialized='table', unique_key='id', sort='id', dist='id', tags=["superset"]) }}
select
	id, 
	changed_by_fk, 
	created_by_fk, 
	last_saved_by_fk,
	convert_timezone('America/New_York', changed_on)   as changed_on, 
	convert_timezone('America/New_York', created_on)   as created_on,
	convert_timezone('America/New_York', last_saved_at)   as last_saved_at,
	datasource_id, 
	datasource_name, 
	datasource_type,
	description, 
	params,
	perm,
	query_context,
	schema_perm,
	slice_name,
	uuid,
	viz_type,
  	convert_timezone('America/New_York', getdate())   as dw_loaded_at
from {{ source('superset', 'slices') }} 