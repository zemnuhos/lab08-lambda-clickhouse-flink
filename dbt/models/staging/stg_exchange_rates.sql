{{ config(materialized='view') }}

SELECT
	update_id,
	toDateTime(timestamp,'UTC')
        AS ts,
	'PUNK'
        AS currency,
	rate_tgrk_punk
        AS rate,
	source_file,
	loaded_at
FROM
	{{ source('analytics', 'raw_exchange_rates') }}
UNION ALL

SELECT
	update_id,
	toDateTime(timestamp,'UTC')
        AS ts,
	'RUB'
        AS currency,
	rate_tgrk_rub
        AS rate,
	source_file,
	loaded_at
FROM
	{{ source('analytics', 'raw_exchange_rates') }}
UNION ALL

SELECT
	1 as update_id,
    toDateTime('1970-01-01 00:00:00', 'UTC') AS ts,
	'TGRK'
        AS currency,
	1
        AS rate,
	'manual' as source_file,
	now() as loaded_at