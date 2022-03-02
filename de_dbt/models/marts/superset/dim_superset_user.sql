{{ config(schema='odin_qa', materialized='incremental', unique_key='nk_superset_user', sort='nk_superset_user', dist='nk_superset_user', tags=["superset"]) }}

 {% if is_incremental() %}
WITH existing_records AS (
    SELECT nk_superset_user,
          is_deleted,
          created_on,
          user_email,
          full_name,
          username,
          updated_at
   FROM {{ this }}
), deleted_records AS  (
    SELECT nk_superset_user,
          TRUE AS is_deleted,
          created_on,
          user_email,
          full_name,
          username,
          convert_timezone('America/New_York', getdate()) AS updated_at
   FROM
     (SELECT d.nk_superset_user,
             d.created_on,
             d.user_email,
             d.full_name,
             d.username
      FROM {{ this }} AS d
      LEFT JOIN {{ ref('stg_superset_ab_user') }} AS src ON d.nk_superset_user = src.id
      WHERE src.id IS NULL
        AND d.is_deleted=FALSE) AS rows_to_inactive
), updated_records AS ( 
    SELECT nk_superset_user,
          is_deleted,
          created_on,
          user_email,
          full_name,
          username,
          convert_timezone('America/New_York', getdate()) AS updated_at
   FROM
     (SELECT src.id AS nk_superset_user,
             FALSE AS is_deleted,
             src.created_on,
             src.email AS user_email,
             (src.first_name || ' ' || src.last_name) AS full_name,
             src.username
      FROM {{ this}} AS d
      INNER JOIN {{ ref('stg_superset_ab_user') }} AS src ON d.nk_superset_user = src.id
      WHERE d.is_deleted=FALSE
        AND (src.created_on != d.created_on
             OR src.email != d.user_email
             OR src.username != d.username
             OR (src.first_name || ' ' || src.last_name) != d.full_name) ) AS rows_to_update
), new_records AS(
    SELECT id AS nk_superset_user,
          active AS is_deleted,
          convert_timezone('America/New_York', created_on) AS created_on,
          email AS user_email,
          first_name || ' ' || last_name AS full_name,
          username,
          convert_timezone('America/New_York', getdate()) AS updated_at
   FROM {{ ref('stg_superset_ab_user') }}
   WHERE id >
       (SELECT nvl(max(nk_superset_user), 0)
        FROM {{ this }})
), summary  AS (
    SELECT e.*
    FROM existing_records AS e
    LEFT JOIN deleted_records AS d ON e.nk_superset_user = d.nk_superset_user
    LEFT JOIN updated_records AS u ON e.nk_superset_user = u.nk_superset_user
    WHERE d.nk_superset_user IS NULL
    AND u.nk_superset_user IS NULL

    UNION

    SELECT d.*
    FROM deleted_records AS d
    LEFT JOIN updated_records AS u ON d.nk_superset_user = u.nk_superset_user
    WHERE u.nk_superset_user IS NULL

    UNION

    SELECT u.*
    FROM updated_records AS u

    UNION

    SELECT n.*
    FROM new_records AS n
)

select row_number() over (order by nk_superset_user )  as sk_superset_user, 
       nk_superset_user,
       is_deleted,
       created_on,
       user_email,
       full_name,
       username,
       updated_at
from summary

{% else %}

SELECT    row_number() over (order by nk_superset_user ) as sk_superset_user,  
          id AS nk_superset_user,
          active AS is_deleted,
          convert_timezone('America/New_York', created_on) AS created_on,
          email AS user_email,
          first_name || ' ' || last_name AS full_name,
          username,
          convert_timezone('America/New_York', getdate()) AS updated_at
   FROM {{ ref('stg_superset_ab_user') }}

{% endif %}