{{ config(schema='odin_qa', materialized='incremental', tags=["superset"]) }}

with dim_superset_user as (
	select sk_superset_user, nk_superset_user from {{ ref('dim_superset_user') }}
), dim_superset_database as (
	select sk_superset_database, nk_superset_database from {{ ref('dim_superset_database') }}
), dim_superset_query_details as (
	select sk_superset_query_details, nk_superset_query_details from {{ ref('dim_superset_query_details') }}
), dim_time as (
	select sk_time, nk_time from {{ source('odin_qa', 'dim_time') }}
)
select 
      cast(bsq.changed_on as date) as sk_executed_date
    , nvl(dqt.sk_time, 0) as sk_time
    , nvl(du.sk_superset_user, 0) as sk_superset_user
	, nvl(ddb.sk_superset_database, 0) as sk_superset_database
    , nvl(dq.sk_superset_query_details, 0) as sk_superset_query_details
	, sum("rows") as total_rows
	, avg("rows") as average_rows
    , avg(cast((end_time - start_running_time)/1000 as numeric(8,2))) as average_execution_time
	, count(1) as executions
    , convert_timezone('America/New_York', getdate()) as updated_at

from {{ ref('stg_superset_query') }} as  bsq 
	
	left join dim_superset_query_details dq 
            on bsq.id = dq.nk_superset_query_details
      
    left join dim_time dqt 
            on substring(cast(bsq.changed_on as time),0 ,6) = dqt.nk_time

	left join dim_superset_user du 
            on bsq.user_id = du.nk_superset_user
	
    left join dim_superset_database ddb 
            on bsq.database_id = ddb.nk_superset_database
{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where dw_loaded_at > (select nvl(max(updated_at), '1900-01-01 00:00:00') from {{ this }})
{% endif %}

group by
      cast(bsq.changed_on as date)
    , nvl(dqt.sk_time, 0)
	, nvl(du.sk_superset_user, 0)
	, nvl(ddb.sk_superset_database, 0)
    , nvl(dq.sk_superset_query_details, 0)
