{{ config(
    materialized='incremental',
    unique_key='hash'
) }}

SELECT
    pk,
    Column_3,
    updated_at,
    hash,
    '{{ invocation_id }}' as batch_id
FROM {{ ref('stg_source_tables__source2') }} s

{% if is_incremental() %}
WHERE NOT EXISTS (SELECT 1 FROM {{ this }} WHERE hash = s.hash)
{% endif %}
