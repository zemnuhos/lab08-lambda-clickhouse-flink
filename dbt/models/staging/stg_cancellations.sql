{{ config(materialized='view') }}

SELECT
	concat(replaceAll(extract(source_file, 'day=([^/]+)'), '-', ''), '#', cancellation_id) as cancellation_nk,
	cancellation_id,
	original_transaction_id,
	reason,
	parseDateTimeBestEffortOrNull(cancelled_at,'UTC') AS cancelled_at,
	refund_amount,
	source_file,
	loaded_at
FROM {{ source('analytics', 'raw_cancellations') }}
