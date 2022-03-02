{{ config(schema='odin_qa', materialized='incremental', unique_key='nk_superset_chart_details', sort='nk_superset_chart_details', dist='nk_superset_chart_details', tags=["superset"]) }} 

{% if is_incremental() %}

    WITH existing_records AS 
        (SELECT nk_superset_chart_details ,
            chart_name ,
            chart_description ,
            is_deleted ,
            updated_at
        FROM {{ this }} ), deleted_records AS 
        (SELECT nk_superset_chart_details ,
            chart_name ,
            chart_description ,
            true AS is_deleted ,
            convert_timezone('America/New_York', getdate()) AS updated_at
        FROM 
            (SELECT d.nk_superset_chart_details ,
            d.chart_name ,
            d.chart_description ,
            d.updated_at
            FROM {{ this }} AS d
            LEFT JOIN {{ ref('stg_superset_slices') }} AS src
                ON d.nk_superset_chart_details = src.id
            WHERE src.id IS NULL
                    AND d.is_deleted=FALSE) AS rows_to_inactive 
            ), updated_records AS (
            SELECT nk_superset_chart_details ,
            chart_name ,
            chart_description ,
            is_deleted ,
            convert_timezone('America/New_York', getdate()) AS updated_at
            FROM 
                (SELECT src.id AS nk_superset_chart_details ,
                src.slice_name AS chart_name ,
                src.description AS chart_description ,
                false AS is_deleted
                FROM {{ this}} AS d
                INNER JOIN {{ ref('stg_superset_slices') }} AS src
                    ON d.nk_superset_chart_details = src.id
                WHERE d.is_deleted=FALSE
                        AND ( src.slice_name != d.chart_name
                        OR src.description != nvl(d.chart_description, ''))
                ) AS rows_to_UPDATE 
            ), new_records AS  (
            SELECT id AS nk_superset_chart_details ,
            slice_name AS chart_name ,
            description AS chart_description ,
            false AS is_deleted ,
            convert_timezone('America/New_York', getdate()) AS updated_at
                FROM {{ ref('stg_superset_slices') }}
                WHERE id > 
                    (SELECT nvl(max(nk_superset_chart_details),
            0)
                    FROM {{ this }})) , summary AS 
                    (SELECT e.*
                    FROM existing_records AS e
                    LEFT JOIN deleted_records AS d
                        ON e.nk_superset_chart_details = d.nk_superset_chart_details
                    LEFT JOIN updated_records AS u
                        ON e.nk_superset_chart_details = u.nk_superset_chart_details
                    WHERE d.nk_superset_chart_details IS NULL
                            AND u.nk_superset_chart_details IS NULL
                    UNION
                    SELECT d.*
                    FROM deleted_records AS d
                    LEFT JOIN updated_records AS u
                        ON d.nk_superset_chart_details = u.nk_superset_chart_details
                    WHERE u.nk_superset_chart_details IS NULL
                    UNION
                    SELECT u.*
                    FROM updated_records AS u
                    UNION
                    SELECT n.*
                    FROM new_records AS n )
                SELECT row_number() OVER (order by nk_superset_chart_details ) AS sk_superset_chart_details , 
                nk_superset_chart_details , 
                chart_name , 
                chart_description , 
                is_deleted , 
                updated_at
                FROM summary 

{% else %}

    SELECT row_number() OVER (order by nk_superset_chart_details ) AS sk_superset_chart_details , 
    id AS nk_superset_chart_details , 
    slice_name AS chart_name , 
    description AS chart_description , 
    false AS is_deleted , 
    convert_timezone('America/New_York', getdate()) AS updated_at
    FROM {{ ref('stg_superset_slices') }} 

{% endif %} 