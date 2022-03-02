{{ config(schema='odinprep_dbt_qa', materialized='incremental', unique_key='id', sort='id', dist='id', tags=["superset"]) }}
select
	id,
	user_id,
	database_id,
	convert_timezone('America/New_York', changed_on) as changed_on,
	client_id, 
	ctas_method, 
	lower(replace("sql", '-- Note: Unless you save your query, these tabs will NOT persist if you clear your cookies or change browsers.', '')) as sql,
	lower(executed_sql) as executed_sql, 
	extra_json, 
	limiting_factor,
	"limit", 
	progress, 
	"rows", 
	"schema", 
	select_as_cta, 
	select_as_cta_used, 
	select_sql, 
	sql_editor_id, 
	start_running_time, 
	start_time, 
	end_time, 
	status, 
	error_message,
	tab_name, 
	tmp_schema_name, 
	tmp_table_name,     
    convert_timezone('America/New_York', getdate())   as dw_loaded_at
from {{ source('superset', 'query') }} 

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where id > (select max(id) from {{ this }})
{% endif %}