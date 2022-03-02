{{ config(schema='odin_qa', materialized='incremental', unique_key='nk_superset_dataset_details', sort='nk_superset_dataset_details', dist='nk_superset_dataset_details', tags=["superset"]) }}

 {% if is_incremental() %}
WITH existing_records AS (
    SELECT  nk_superset_dataset_details,
            dataset_type,
            dataset_name, 
            dataset_schema, 
            has_cte, 
            has_count, 
            has_avg, 
            has_sum, 
            has_max, 
            has_min, 
            has_row_number, 
            has_join,
            has_group_by, 
            has_having, 
            has_order_by,
            has_params,
            has_jinja_template,
            is_deleted,
            last_modification_on, 
            updated_at
   FROM {{ this }}
), deleted_records AS  (
    SELECT  nk_superset_dataset_details,
            dataset_type,
            dataset_name, 
            dataset_schema, 
            has_cte, 
            has_count, 
            has_avg, 
            has_sum, 
            has_max, 
            has_min, 
            has_row_number, 
            has_join,
            has_group_by, 
            has_having, 
            has_order_by,
            has_params,
            has_jinja_template,
            true as is_deleted,
            last_modification_on, 
            convert_timezone('America/New_York', getdate()) AS updated_at
   FROM
     (SELECT d.nk_superset_dataset_details,
             d.dataset_type,
             d.dataset_name, 
             d.dataset_schema, 
             d.has_cte, 
             d.has_count, 
             d.has_avg, 
             d.has_sum, 
             d.has_max, 
             d.has_min, 
             d.has_row_number, 
             d.has_join,
             d.has_group_by, 
             d.has_having, 
             d.has_order_by,
             d.has_params,
             d.has_jinja_template,
             d.last_modification_on
      FROM {{ this }} AS d
      LEFT JOIN {{ ref('stg_superset_tables') }} AS src ON d.nk_superset_dataset_details = src.id
      WHERE src.id IS NULL
        AND d.is_deleted=FALSE) AS rows_to_inactive
), updated_records AS ( 
    SELECT nk_superset_dataset_details,
           dataset_type,
           dataset_name, 
           dataset_schema, 
           has_cte, 
           has_count, 
           has_avg, 
           has_sum, 
           has_max, 
           has_min, 
           has_row_number, 
           has_join,
           has_group_by, 
           has_having, 
           has_order_by,
           has_params,
           has_jinja_template,
           is_deleted,
           last_modification_on, 
           convert_timezone('America/New_York', getdate()) AS updated_at
   FROM
     (SELECT src.id AS nk_superset_dataset_details,
             case when src.is_sqllab_view is true then 'Virtual' else 'Physical' end as dataset_type,
             src.table_name as dataset_name, 
             src.schema as dataset_schema, 
             case when regexp_count(src."sql", 'with\\s{1,}[a-zA-Z0-9*._]{0,}\\s{1,}as', 0, 'p') > 0 then true else false end as has_cte, 
             case when regexp_count(src."sql", 'count\\s{0,}\\([a-zA-Z0-9*._]{0,}', 0, 'p') > 0 then true else false end as has_count, 
             case when regexp_count(src."sql", 'avg\\s{0,}\\([a-zA-Z0-9*._]{0,}', 0, 'p') > 0 then true else false end as has_avg, 
             case when regexp_count(src."sql", 'sum\\s{0,}\\([a-zA-Z0-9*._]{0,}', 0, 'p') > 0 then true else false end as has_sum, 
             case when regexp_count(src."sql", 'max\\s{0,}\\([a-zA-Z0-9*._]{0,}', 0, 'p') > 0 then true else false end as has_max, 
             case when regexp_count(src."sql", 'min\\s{0,}\\([a-zA-Z0-9*._]{0,}', 0, 'p') > 0 then true else false end as has_min, 
             case when regexp_count(src."sql", 'row_number()\\s', 0, 'p') > 0 then true else false end as has_row_number, 
             case when regexp_count(src."sql", '\\s{1,}join\\s{1,}', 0, 'p') > 0 then true else false end as has_join,
             case when regexp_count(src."sql", 'group\\s{1,}by\\s', 0, 'p') > 0 then true else false end as has_group_by, 
             case when regexp_count(src."sql", 'having\\s{1,}', 0, 'p') > 0 then true else false end as has_having, 
             case when regexp_count(src."sql", 'order\\s{1,}by\\s', 0, 'p') > 0 then true else false end as has_order_by,
             case when regexp_count(src."sql", '\{\{.{1,}\}\}', 0, 'p') > 0 then true else false end as has_params,
             case when regexp_count(src."sql", '\{\{\\s{0,}.{0,}filter_values.{1,}\}\}', 0, 'p') > 0 then true else false end as has_jinja_template,
             false as is_deleted,
             src.changed_on as last_modification_on
      FROM {{ this}} AS d
      INNER JOIN {{ ref('stg_superset_tables') }} AS src ON d.nk_superset_dataset_details = src.id
      WHERE d.is_deleted=FALSE
        AND (
        case when src.is_sqllab_view is true then 'Virtual' else 'Physical' end != d.dataset_type OR
        src.table_name != nvl(d.dataset_name, '') OR
        src.schema != nvl(dataset_schema, '') OR
        case when regexp_count(src."sql", 'with\\s{1,}[a-zA-Z0-9*._]{0,}\\s{1,}as', 0, 'p') > 0 then true else false end != d.has_cte OR
        case when regexp_count(src."sql", 'count\\s{0,}\\([a-zA-Z0-9*._]{0,}', 0, 'p') > 0 then true else false end != d.has_count OR 
        case when regexp_count(src."sql", 'avg\\s{0,}\\([a-zA-Z0-9*._]{0,}', 0, 'p') > 0 then true else false end != d.has_avg OR 
        case when regexp_count(src."sql", 'sum\\s{0,}\\([a-zA-Z0-9*._]{0,}', 0, 'p') > 0 then true else false end != d.has_sum OR 
        case when regexp_count(src."sql", 'max\\s{0,}\\([a-zA-Z0-9*._]{0,}', 0, 'p') > 0 then true else false end != d.has_max OR 
        case when regexp_count(src."sql", 'min\\s{0,}\\([a-zA-Z0-9*._]{0,}', 0, 'p') > 0 then true else false end != d.has_min OR 
        case when regexp_count(src."sql", 'row_number()\\s', 0, 'p') > 0 then true else false end != d.has_row_number OR 
        case when regexp_count(src."sql", '\\s{1,}join\\s{1,}', 0, 'p') > 0 then true else false end != d.has_join OR
        case when regexp_count(src."sql", 'group\\s{1,}by\\s', 0, 'p') > 0 then true else false end != d.has_group_by OR 
        case when regexp_count(src."sql", 'having\\s{1,}', 0, 'p') > 0 then true else false end != d.has_having OR 
        case when regexp_count(src."sql", 'order\\s{1,}by\\s', 0, 'p') > 0 then true else false end != d.has_order_by OR
        case when regexp_count(src."sql", '\{\{.{1,}\}\}', 0, 'p') > 0 then true else false end != d.has_params OR
        case when regexp_count(src."sql", '\{\{\\s{0,}.{0,}filter_values.{1,}\}\}', 0, 'p') > 0 then true else false end != d.has_jinja_template OR
        src.changed_on != d.last_modification_on )
    )
), new_records AS (
    SELECT 
        id AS nk_superset_dataset_details,
        case when is_sqllab_view is true then 'Virtual' else 'Physical' end as dataset_type,
        table_name as dataset_name, 
        schema as dataset_schema, 
        case when regexp_count("sql", 'with\\s{1,}[a-zA-Z0-9*._]{0,}\\s{1,}as', 0, 'p') > 0 then true else false end as has_cte, 
        case when regexp_count("sql", 'count\\s{0,}\\([a-zA-Z0-9*._]{0,}', 0, 'p') > 0 then true else false end as has_count, 
        case when regexp_count("sql", 'avg\\s{0,}\\([a-zA-Z0-9*._]{0,}', 0, 'p') > 0 then true else false end as has_avg, 
        case when regexp_count("sql", 'sum\\s{0,}\\([a-zA-Z0-9*._]{0,}', 0, 'p') > 0 then true else false end as has_sum, 
        case when regexp_count("sql", 'max\\s{0,}\\([a-zA-Z0-9*._]{0,}', 0, 'p') > 0 then true else false end as has_max, 
        case when regexp_count("sql", 'min\\s{0,}\\([a-zA-Z0-9*._]{0,}', 0, 'p') > 0 then true else false end as has_min, 
        case when regexp_count("sql", 'row_number()\\s', 0, 'p') > 0 then true else false end as has_row_number, 
        case when regexp_count("sql", '\\s{1,}join\\s{1,}', 0, 'p') > 0 then true else false end as has_join,
        case when regexp_count("sql", 'group\\s{1,}by\\s', 0, 'p') > 0 then true else false end as has_group_by, 
        case when regexp_count("sql", 'having\\s{1,}', 0, 'p') > 0 then true else false end as has_having, 
        case when regexp_count("sql", 'order\\s{1,}by\\s', 0, 'p') > 0 then true else false end as has_order_by,
        case when regexp_count("sql", '\{\{.{1,}\}\}', 0, 'p') > 0 then true else false end as has_params,
        case when regexp_count("sql", '\{\{\\s{0,}.{0,}filter_values.{1,}\}\}', 0, 'p') > 0 then true else false end as has_jinja_template,
        false as is_deleted,
        changed_on as last_modification_on, 
        convert_timezone('America/New_York', getdate()) AS updated_at
   FROM {{ ref('stg_superset_tables') }}
   WHERE id >
       (SELECT nvl(max(nk_superset_dataset_details), 0)
        FROM {{ this }})
), summary  AS (
    SELECT e.*
    FROM existing_records AS e
    LEFT JOIN deleted_records AS d ON e.nk_superset_dataset_details = d.nk_superset_dataset_details
    LEFT JOIN updated_records AS u ON e.nk_superset_dataset_details = u.nk_superset_dataset_details
    WHERE d.nk_superset_dataset_details IS NULL
    AND u.nk_superset_dataset_details IS NULL

    UNION

    SELECT d.*
    FROM deleted_records AS d
    LEFT JOIN updated_records AS u ON d.nk_superset_dataset_details = u.nk_superset_dataset_details
    WHERE u.nk_superset_dataset_details IS NULL

    UNION

    SELECT u.*
    FROM updated_records AS u

    UNION

    SELECT n.*
    FROM new_records AS n
)

select  row_number() over (order by nk_superset_dataset_details )  as sk_superset_dataset_details, 
        nk_superset_dataset_details,
        dataset_type,
        dataset_name, 
        dataset_schema, 
        has_cte, 
        has_count, 
        has_avg, 
        has_sum, 
        has_max, 
        has_min, 
        has_row_number, 
        has_join,
        has_group_by, 
        has_having, 
        has_order_by,
        has_params,
        has_jinja_template,
        is_deleted,
        last_modification_on, 
        updated_at
from summary

{% else %}

SELECT  row_number() over (order by nk_superset_dataset_details ) as sk_superset_dataset_details,  
        id AS nk_superset_dataset_details,
        case when is_sqllab_view is true then 'Virtual' else 'Physical' end as dataset_type,
        table_name as dataset_name, 
        schema as dataset_schema, 
        case when regexp_count("sql", 'with\\s{1,}[a-zA-Z0-9*._]{0,}\\s{1,}as', 0, 'p') > 0 then true else false end as has_cte, 
        case when regexp_count("sql", 'count\\s{0,}\\([a-zA-Z0-9*._]{0,}', 0, 'p') > 0 then true else false end as has_count, 
        case when regexp_count("sql", 'avg\\s{0,}\\([a-zA-Z0-9*._]{0,}', 0, 'p') > 0 then true else false end as has_avg, 
        case when regexp_count("sql", 'sum\\s{0,}\\([a-zA-Z0-9*._]{0,}', 0, 'p') > 0 then true else false end as has_sum, 
        case when regexp_count("sql", 'max\\s{0,}\\([a-zA-Z0-9*._]{0,}', 0, 'p') > 0 then true else false end as has_max, 
        case when regexp_count("sql", 'min\\s{0,}\\([a-zA-Z0-9*._]{0,}', 0, 'p') > 0 then true else false end as has_min, 
        case when regexp_count("sql", 'row_number()\\s', 0, 'p') > 0 then true else false end as has_row_number, 
        case when regexp_count("sql", '\\s{1,}join\\s{1,}', 0, 'p') > 0 then true else false end as has_join,
        case when regexp_count("sql", 'group\\s{1,}by\\s', 0, 'p') > 0 then true else false end as has_group_by, 
        case when regexp_count("sql", 'having\\s{1,}', 0, 'p') > 0 then true else false end as has_having, 
        case when regexp_count("sql", 'order\\s{1,}by\\s', 0, 'p') > 0 then true else false end as has_order_by,
        case when regexp_count("sql", '\{\{.{1,}\}\}', 0, 'p') > 0 then true else false end as has_params,
        case when regexp_count("sql", '\{\{\\s{0,}.{0,}filter_values.{1,}\}\}', 0, 'p') > 0 then true else false end as has_jinja_template,
        false as is_deleted,
        changed_on as last_modification_on, 
        convert_timezone('America/New_York', getdate()) AS updated_at
   FROM {{ ref('stg_superset_tables') }}

{% endif %}