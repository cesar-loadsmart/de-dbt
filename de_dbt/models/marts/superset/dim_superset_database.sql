{{ config(schema='odin_qa', materialized='incremental', unique_key='nk_superset_database', sort='nk_superset_database', dist='nk_superset_database', tags=["superset"]) }}

{% if is_incremental() %}

        WITH existing_records AS (
        SELECT 
                nk_superset_database
                , "database_name"
                , is_deleted
                , allow_run_async
                , cache_timeout
                , configuration_method
                , is_available_on_sqllab
                , sqlalchemy_uri
                , default_schema_for_ctas
                , created_on 
                , updated_at 
        FROM {{ this }}
        ), deleted_records AS (
                SELECT 
                nk_superset_database
                , "database_name"
                , TRUE as is_deleted
                , allow_run_async
                , cache_timeout
                , configuration_method
                , is_available_on_sqllab
                , sqlalchemy_uri
                , default_schema_for_ctas
                , created_on 
                , updated_at
        FROM
        (SELECT d.nk_superset_database
                , d."database_name"
                , d.allow_run_async
                , d.cache_timeout
                , d.configuration_method
                , d.is_available_on_sqllab
                , d.sqlalchemy_uri
                , d.default_schema_for_ctas
                , d.created_on 
                , d.updated_at
        FROM {{ this }} AS d
        LEFT JOIN {{ ref('stg_superset_dbs') }} AS src ON d.nk_superset_database = src.id
        WHERE src.id IS NULL
                AND d.is_deleted=FALSE) AS rows_to_inactive
        ), updated_records AS
        (SELECT nk_superset_database
                , "database_name"
                , is_deleted
                , allow_run_async
                , cache_timeout
                , configuration_method
                , is_available_on_sqllab
                , sqlalchemy_uri
                , default_schema_for_ctas
                , created_on 
                , convert_timezone('America/New_York', getdate()) AS updated_at
        FROM
        (SELECT src.id as nk_superset_database
                , src."database_name"
                , false as is_deleted
                , src.allow_run_async 
                , src.cache_timeout
                , src.configuration_method     
                , src.expose_in_sqllab as is_available_on_sqllab
                , src.sqlalchemy_uri
                , src.force_ctas_schema as default_schema_for_ctas
                , src.created_on
        FROM {{ this}} AS d
        INNER JOIN {{ ref('stg_superset_dbs') }} AS src ON d.nk_superset_database = src.id
        WHERE d.is_deleted=FALSE
                AND (
                        src."database_name" != d."database_name"
                        OR src.allow_run_async != d.allow_run_async
                        OR src.cache_timeout != d.cache_timeout
                        OR src.configuration_method != d.configuration_method
                        OR src.expose_in_sqllab != d.is_available_on_sqllab
                        OR src.sqlalchemy_uri != d.sqlalchemy_uri
                        OR src.force_ctas_schema != d.default_schema_for_ctas) 
                ) AS rows_to_update
        ),     new_records AS
        (SELECT id as nk_superset_database
                , "database_name"
                , false as is_deleted
                , allow_run_async 
                , cache_timeout
                , configuration_method     
                , expose_in_sqllab as is_available_on_sqllab
                , sqlalchemy_uri
                , force_ctas_schema as default_schema_for_ctas
                , created_on
                , convert_timezone('America/New_York', getdate())    as updated_at
        FROM {{ ref('stg_superset_dbs') }}
        WHERE id >
        (SELECT nvl(max(nk_superset_database), 0)
                FROM {{ this }}))
        , summary  AS (
        SELECT e.*
        FROM existing_records AS e
        LEFT JOIN deleted_records AS d ON e.nk_superset_database = d.nk_superset_database
        LEFT JOIN updated_records AS u ON e.nk_superset_database = u.nk_superset_database
        WHERE d.nk_superset_database IS NULL
        AND u.nk_superset_database IS NULL

        UNION

        SELECT d.*
        FROM deleted_records AS d
        LEFT JOIN updated_records AS u ON d.nk_superset_database = u.nk_superset_database
        WHERE u.nk_superset_database IS NULL

        UNION

        SELECT u.*
        FROM updated_records AS u

        UNION

        SELECT n.*
        FROM new_records AS n
        )

        select row_number() over (order by nk_superset_database ) as sk_superset_database 
                , nk_superset_database
                , "database_name"
                , is_deleted
                , allow_run_async
                , cache_timeout
                , configuration_method
                , is_available_on_sqllab
                , sqlalchemy_uri
                , default_schema_for_ctas
                , created_on 
                , updated_at
        from summary


{% else %}
        SELECT     row_number() over (order by nk_superset_database ) as sk_superset_database 
                ,  id as nk_superset_database
                , "database_name"
                , false as is_deleted
                , allow_run_async 
                , cache_timeout
                , configuration_method     
                , expose_in_sqllab as is_available_on_sqllab
                , sqlalchemy_uri
                , force_ctas_schema as default_schema_for_ctas
                , created_on
                , convert_timezone('America/New_York', getdate())    as updated_at
        FROM {{ ref('stg_superset_dbs') }}

{% endif %}
