{{ config(
    materialized='incremental',
    unique_key='hash',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
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
