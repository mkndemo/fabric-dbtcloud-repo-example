{{ config(
    materialized='incremental',
    unique_key='pk',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

{% if is_incremental() %}
    WITH max_updated_at AS (
        SELECT MAX(updated_at) AS max_updated_at FROM {{ this }}
    ),
{% else %}
    WITH max_updated_at AS (
        SELECT CAST('1900-01-01 00:00:00' AS DATETIME2) AS max_updated_at
    ),
{% endif %}

changed_source1 AS (
    SELECT pk
    FROM {{ ref('source1_incremental') }} s1
    CROSS JOIN max_updated_at
    WHERE s1.updated_at > max_updated_at.max_updated_at
),
changed_source2 AS (
    SELECT pk
    FROM {{ ref('source2_incremental') }} s2
    CROSS JOIN max_updated_at
    WHERE s2.updated_at > max_updated_at.max_updated_at
),
changed_source3 AS (
    SELECT pk
    FROM {{ ref('source3_incremental') }} s3
    CROSS JOIN max_updated_at
    WHERE s3.updated_at > max_updated_at.max_updated_at
),
changed_source4 AS (
    SELECT pk
    FROM {{ ref('source4_incremental') }} s4
    CROSS JOIN max_updated_at
    WHERE s4.updated_at > max_updated_at.max_updated_at
),

changed_PKs AS (
    SELECT pk FROM changed_source1
    UNION
    SELECT pk FROM changed_source2
    UNION
    SELECT pk FROM changed_source3
    UNION
    SELECT pk FROM changed_source4
),

combined_data AS (
    SELECT
        s1.pk,
        s1.Column_1,
        COALESCE(s3.Column_2, s1.Column_2) AS Column_2,
        COALESCE(s2.Column_3, s1.Column_3) AS Column_3,
        COALESCE(s3.Column_4, s1.Column_4) AS Column_4,
        COALESCE(s4.Column_5, s1.Column_5) AS Column_5,
        -- Include Columns 6 to 199 from source1
        s1.Column_6,
        s1.Column_7,
        s1.Column_8,
        s1.Column_9,
        s1.Column_10,
        s1.Column_11,
        s1.Column_12,
        s1.Column_13,
        s1.Column_14,
        s1.Column_15,
        s1.Column_16,
        s1.Column_17,
        s1.Column_18,
        s1.Column_19,
        s1.Column_20,
        s1.Column_21,
        s1.Column_22,
        s1.Column_23,
        s1.Column_24,
        s1.Column_25,
        s1.Column_26,
        s1.Column_27,
        s1.Column_28,
        s1.Column_29,
        s1.Column_30,
        s1.Column_31,
        s1.Column_32,
        s1.Column_33,
        s1.Column_34,
        s1.Column_35,
        s1.Column_36,
        s1.Column_37,
        s1.Column_38,
        s1.Column_39,
        s1.Column_40,
        s1.Column_41,
        s1.Column_42,
        s1.Column_43,
        s1.Column_44,
        s1.Column_45,
        s1.Column_46,
        s1.Column_47,
        s1.Column_48,
        s1.Column_49,
        s1.Column_50,
        s1.Column_51,
        s1.Column_52,
        s1.Column_53,
        s1.Column_54,
        s1.Column_55,
        s1.Column_56,
        s1.Column_57,
        s1.Column_58,
        s1.Column_59,
        s1.Column_60,
        s1.Column_61,
        s1.Column_62,
        s1.Column_63,
        s1.Column_64,
        s1.Column_65,
        s1.Column_66,
        s1.Column_67,
        s1.Column_68,
        s1.Column_69,
        s1.Column_70,
        s1.Column_71,
        s1.Column_72,
        s1.Column_73,
        s1.Column_74,
        s1.Column_75,
        s1.Column_76,
        s1.Column_77,
        s1.Column_78,
        s1.Column_79,
        s1.Column_80,
        s1.Column_81,
        s1.Column_82,
        s1.Column_83,
        s1.Column_84,
        s1.Column_85,
        s1.Column_86,
        s1.Column_87,
        s1.Column_88,
        s1.Column_89,
        s1.Column_90,
        s1.Column_91,
        s1.Column_92,
        s1.Column_93,
        s1.Column_94,
        s1.Column_95,
        s1.Column_96,
        s1.Column_97,
        s1.Column_98,
        s1.Column_99,
        s1.Column_100,
        s1.Column_101,
        s1.Column_102,
        s1.Column_103,
        s1.Column_104,
        s1.Column_105,
        s1.Column_106,
        s1.Column_107,
        s1.Column_108,
        s1.Column_109,
        s1.Column_110,
        s1.Column_111,
        s1.Column_112,
        s1.Column_113,
        s1.Column_114,
        s1.Column_115,
        s1.Column_116,
        s1.Column_117,
        s1.Column_118,
        s1.Column_119,
        s1.Column_120,
        s1.Column_121,
        s1.Column_122,
        s1.Column_123,
        s1.Column_124,
        s1.Column_125,
        s1.Column_126,
        s1.Column_127,
        s1.Column_128,
        s1.Column_129,
        s1.Column_130,
        s1.Column_131,
        s1.Column_132,
        s1.Column_133,
        s1.Column_134,
        s1.Column_135,
        s1.Column_136,
        s1.Column_137,
        s1.Column_138,
        s1.Column_139,
        s1.Column_140,
        s1.Column_141,
        s1.Column_142,
        s1.Column_143,
        s1.Column_144,
        s1.Column_145,
        s1.Column_146,
        s1.Column_147,
        s1.Column_148,
        s1.Column_149,
        s1.Column_150,
        s1.Column_151,
        s1.Column_152,
        s1.Column_153,
        s1.Column_154,
        s1.Column_155,
        s1.Column_156,
        s1.Column_157,
        s1.Column_158,
        s1.Column_159,
        s1.Column_160,
        s1.Column_161,
        s1.Column_162,
        s1.Column_163,
        s1.Column_164,
        s1.Column_165,
        s1.Column_166,
        s1.Column_167,
        s1.Column_168,
        s1.Column_169,
        s1.Column_170,
        s1.Column_171,
        s1.Column_172,
        s1.Column_173,
        s1.Column_174,
        s1.Column_175,
        s1.Column_176,
        s1.Column_177,
        s1.Column_178,
        s1.Column_179,
        s1.Column_180,
        s1.Column_181,
        s1.Column_182,
        s1.Column_183,
        s1.Column_184,
        s1.Column_185,
        s1.Column_186,
        s1.Column_187,
        s1.Column_188,
        s1.Column_189,
        s1.Column_190,
        s1.Column_191,
        s1.Column_192,
        s1.Column_193,
        s1.Column_194,
        s1.Column_195,
        s1.Column_196,
        s1.Column_197,
        s1.Column_198,
        s1.Column_199,
        COALESCE(s4.Column_200, s1.Column_200) AS Column_200,
        (
            SELECT MAX(v.updated_at)
            FROM (VALUES 
                (s1.updated_at), 
                (s2.updated_at), 
                (s3.updated_at), 
                (s4.updated_at)
            ) AS v(updated_at)
        ) AS updated_at,
        '{{ invocation_id }}' as batch_id
    FROM {{ ref('source1_incremental') }} s1
    LEFT JOIN {{ ref('source2_incremental') }} s2 ON s1.pk = s2.pk
    LEFT JOIN {{ ref('source3_incremental') }} s3 ON s1.pk = s3.pk
    LEFT JOIN {{ ref('source4_incremental') }} s4 ON s1.pk = s4.pk
    {% if is_incremental() %}
    WHERE s1.pk IN (SELECT pk FROM changed_PKs)
    {% endif %}
)

SELECT * FROM combined_data
