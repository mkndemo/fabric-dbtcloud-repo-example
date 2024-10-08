{{ config(
    materialized='incremental',
    unique_key='hash'
) }}

WITH source1 AS (
    SELECT * FROM {{ ref('source1_incremental') }}
),

source2 AS (
    SELECT pk, Column_3 FROM {{ ref('source2_incremental') }}
),

source3 AS (
    SELECT pk, Column_2, Column_4 FROM {{ ref('source3_incremental') }}
),

source4 AS (
    SELECT pk, Column_5, Column_200 FROM {{ ref('source4_incremental') }}
),

merged AS (
    SELECT
        s1.pk,
        s1.Column_1,
        COALESCE(s3.Column_2, s1.Column_2) AS Column_2,
        COALESCE(s2.Column_3, s1.Column_3) AS Column_3,
        COALESCE(s3.Column_4, s1.Column_4) AS Column_4,
        COALESCE(s4.Column_5, s1.Column_5) AS Column_5,
        -- Include other columns from s1 (Column_6 to Column_199), this jinja macro assumes that column names are Column_6-200
        {% for i in range(6, 200) %}
        s1.Column_{{ i }},
        {% endfor %}
        COALESCE(s4.Column_200, s1.Column_200) AS Column_200
    FROM source1 s1
    LEFT JOIN source2 s2 ON s1.pk = s2.pk
    LEFT JOIN source3 s3 ON s1.pk = s3.pk
    LEFT JOIN source4 s4 ON s1.pk = s4.pk
),

hashed AS (
    SELECT
        *,
        CONVERT(VARCHAR(32), HASHBYTES('MD5', 
            CONCAT_WS('|', 
                CAST(pk AS VARCHAR(MAX)),
                CAST(Column_1 AS VARCHAR(MAX)),
                CAST(Column_2 AS VARCHAR(MAX)),
                CAST(Column_3 AS VARCHAR(MAX)),
                CAST(Column_4 AS VARCHAR(MAX)),
                CAST(Column_5 AS VARCHAR(MAX)),
                {% for i in range(6, 201) %}
                CAST(Column_{{ i }} AS VARCHAR(MAX)){% if not loop.last %},{% endif %}
                {% endfor %}
            )
        ), 2) AS hash
    FROM merged
)

SELECT
    *,
    '{{ invocation_id }}' AS batch_id_final
FROM hashed

{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1 FROM {{ this }} WHERE hash = hashed.hash
)
{% endif %}
