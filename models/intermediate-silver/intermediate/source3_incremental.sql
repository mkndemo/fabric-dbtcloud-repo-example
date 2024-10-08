{{ config(
    materialized='incremental',
    unique_key='hash'
) }}

SELECT
    pk,
    Column_2,
    Column_4,
    hash,
    '{{ invocation_id }}' as batch_id
FROM {{ ref('stg_source_tables__source3') }} s

{% if is_incremental() %}
WHERE NOT EXISTS (SELECT 1 FROM {{ this }} WHERE hash = s.hash)
{% endif %}
