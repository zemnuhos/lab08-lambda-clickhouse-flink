{{ config(materialized='view') }}

WITH all_uuid AS (
    SELECT
        nullIf(user_uuid, '') AS user_uuid
    FROM {{ source('analytics', 'raw_users') }}

    UNION DISTINCT

    SELECT
        nullIf(test_user_uuid, '') AS user_uuid
    FROM {{ source('analytics', 'raw_test_users') }}
)

SELECT
    coalesce(ru.user_id, -1) AS user_id,
    au.user_uuid AS user_uuid,
    coalesce(ru.is_test_user, true) AS is_test_user,
    coalesce(ru.source_file, tu.source_file) AS source_file,
    coalesce(ru.loaded_at, tu.loaded_at) AS loaded_at
FROM all_uuid au
LEFT JOIN {{ source('analytics', 'raw_users') }} ru
    ON au.user_uuid = ru.user_uuid
LEFT JOIN {{ source('analytics', 'raw_test_users') }} tu
    ON au.user_uuid = tu.test_user_uuid
