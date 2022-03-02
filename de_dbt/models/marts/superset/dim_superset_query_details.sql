{{ config(schema='odin_qa', materialized='incremental', unique_key='nk_superset_query_details', sort='nk_superset_query_details', dist='nk_superset_query_details', tags=["superset"]) }}

 SELECT row_number() over (order by nk_superset_query_details ) as sk_superset_query_details,
        id as nk_superset_query_details, 
	case when regexp_count(executed_sql, 'with\\s{1,}[a-zA-Z0-9*._]{0,}\\s{1,}as', 0, 'p') > 0 then true else false end as has_cte, 
	case when regexp_count(executed_sql, 'count\\s{0,}\\([a-zA-Z0-9*._]{0,}', 0, 'p') > 0 then true else false end as has_count, 
	case when regexp_count(executed_sql, 'avg\\s{0,}\\([a-zA-Z0-9*._]{0,}', 0, 'p') > 0 then true else false end as has_avg, 
	case when regexp_count(executed_sql, 'sum\\s{0,}\\([a-zA-Z0-9*._]{0,}', 0, 'p') > 0 then true else false end as has_sum, 
	case when regexp_count(executed_sql, 'max\\s{0,}\\([a-zA-Z0-9*._]{0,}', 0, 'p') > 0 then true else false end as has_max, 
	case when regexp_count(executed_sql, 'min\\s{0,}\\([a-zA-Z0-9*._]{0,}', 0, 'p') > 0 then true else false end as has_min, 
	case when regexp_count(executed_sql, 'row_number()\\s', 0, 'p') > 0 then true else false end as has_row_number, 
	case when regexp_count(executed_sql, '\\s{1,}join\\s{1,}', 0, 'p') > 0 then true else false end as has_join,
	case when regexp_count(executed_sql, 'group\\s{1,}by\\s', 0, 'p') > 0 then true else false end as has_group_by, 
	case when regexp_count(executed_sql, 'having\\s{1,}', 0, 'p') > 0 then true else false end as has_having, 
	case when regexp_count(executed_sql, 'order\\s{1,}by\\s', 0, 'p') > 0 then true else false end as has_order_by, 
	limiting_factor, 
	"limit" , 
        case when status = 'success' then true else false end as has_succeeded,
	"error_message",
        convert_timezone('America/New_York', getdate())  as updated_at
from {{ ref('stg_superset_query') }} 

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where nk_superset_query_details > (select nvl(max(nk_superset_query_details),0) from {{ this }})
{% endif %}