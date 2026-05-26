{{ config(materialized='table') }}

SELECT
    cityHash64(ifNull(user_uuid, '')) AS user_key,
    any(user_id) AS user_id,
    user_uuid,
    max(toUInt8(is_test_user)) AS is_test_user,
    max(source_file) AS source_file,
    'S3' as source,
    now() as loaded_at
FROM {{ ref('stg_users') }}
WHERE user_uuid IS NOT NULL
GROUP BY user_uuid
