{{ config(schema='odin_qa', materialized='incremental', tags=["superset"]) }}

with dim_superset_user as (
	select sk_superset_user, nk_superset_user from {{ ref('dim_superset_user') }}
), dim_superset_database as (
	select sk_superset_database, nk_superset_database from {{ ref('dim_superset_database') }}
), dim_superset_dataset_details as (
	select sk_superset_dataset_details, nk_superset_dataset_details from {{ ref('dim_superset_dataset_details') }}
), dim_superset_chart_details as (
	select sk_superset_chart_details, nk_superset_chart_details from {{ ref('dim_superset_chart_details') }}
), dim_superset_chart_type as (
	select sk_superset_chart_type, nk_superset_chart_type from {{ ref('dim_superset_chart_type') }}    
), dim_time as (
	select sk_time, nk_time from {{ source('odin_qa', 'dim_time') }}
), source as (
    select charts.created_on
    , charts.changed_on
    , charts.created_by_fk
    , charts.changed_by_fk
    , charts.id
    , charts.datasource_id
    , charts.viz_type
    , datasets.database_id
    , charts.dw_loaded_at
    from {{ ref('stg_superset_slices') }} as charts
    left join {{ ref('stg_superset_tables') }} as datasets 
    on charts.datasource_id = datasets.id
)
select 
      cast(ssc.created_on as date) as sk_created_date
    , nvl(dqt_created.sk_time, 0) as sk_created_time
    , cast(ssc.changed_on as date) as sk_changed_date
    , nvl(dqt_changed.sk_time, 0) as sk_changed_time
    , nvl(du_created.sk_superset_user, 0) as sk_created_by_superset_user
    , nvl(du_changed.sk_superset_user, 0) as sk_changed_by_superset_user
	, nvl(ddb.sk_superset_database, 0) as sk_superset_database
    , nvl(dsdd.sk_superset_dataset_details, 0) as sk_superset_dataset_details
    , nvl(dsct.sk_superset_chart_type, 0) as sk_superset_chart_type
    , nvl(dscd.sk_superset_chart_details, 0) as sk_superset_chart_details
	, count(1) as count_of_charts
    , convert_timezone('America/New_York', getdate()) as updated_at

from source as  ssc 
	
	left join dim_superset_chart_details dscd 
            on ssc.id = dscd.nk_superset_chart_details
    
    left join dim_superset_dataset_details dsdd
            on ssc.datasource_id = dsdd.nk_superset_dataset_details

    left join dim_superset_chart_type dsct
            on ssc.viz_type = dsct.nk_superset_chart_type 

	left join dim_superset_user du_created
            on ssc.created_by_fk = du_created.nk_superset_user

    left join dim_superset_user du_changed 
            on ssc.changed_by_fk = du_changed.nk_superset_user
	
    left join dim_superset_database ddb 
            on ssc.database_id = ddb.nk_superset_database

    left join dim_time dqt_created 
            on substring(cast(ssc.created_on as time),0 ,6) = dqt_created.nk_time
    
    left join dim_time dqt_changed
            on substring(cast(ssc.changed_on as time),0 ,6) = dqt_changed.nk_time

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where id > (select nvl(max(nk_superset_chart_details), 0) from {{ this }} src inner join dim_superset_chart_details d  on src.sk_superset_chart_details=d.sk_superset_chart_details)
{% endif %}

group by
      cast(ssc.created_on as date)
    , nvl(dqt_created.sk_time, 0)
    , cast(ssc.changed_on as date)
    , nvl(dqt_changed.sk_time, 0)
    , nvl(du_created.sk_superset_user, 0)
    , nvl(du_changed.sk_superset_user, 0)
	, nvl(ddb.sk_superset_database, 0)
    , nvl(dsdd.sk_superset_dataset_details, 0)
    , nvl(dsct.sk_superset_chart_type, 0)
    , nvl(dscd.sk_superset_chart_details, 0)
