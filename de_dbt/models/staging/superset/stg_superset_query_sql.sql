{{ config(schema='odinprep_dbt_qa', materialized='table', unique_key='id', sort='id', dist='id', tags=["superset"]) }}
select
	id,
	lower(replace("sql", '-- Note: Unless you save your query, these tabs will NOT persist if you clear your cookies or change browsers.', ''))as "sql"
from {{ source('superset', 'query') }} 
where id not in (select id from {{ source('odinprep_dbt_qa', 'stg_superset_tables_scraped_by_query') }} )