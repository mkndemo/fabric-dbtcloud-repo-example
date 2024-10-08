{{ config(
    materialized='incremental',
    unique_key='hash',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

SELECT
    pk,
    Column_5,
    Column_200,
    '{{ invocation_id }}' as batch_id
FROM {{ ref('stg_source_tables__source4') }} s

{% if is_incremental() %}
WHERE NOT EXISTS (SELECT 1 FROM {{ this }} WHERE hash = s.hash)
{% endif %}
