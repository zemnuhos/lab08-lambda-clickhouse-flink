{{ config(materialized='view') }}

with prep_trans as (
select
	if(
            created_at != 0,
            toDateTime(created_at, 'UTC'),
            toDateTime(
                concat(
                    extract(source_file, 'day=([^/]+)'),
                    ' ',
                    replace(extract(source_file, 'slot=([^/]+)'), '-', ':'),
                    ':00'
                )
            )
        ) as created_at_full,
	concat(replaceAll(
            concat(
                extract(source_file, 'day=([^/]+)'),
                extract(source_file, 'slot=([^/]+)')
            ),
            '-',
            ''
        ), '#', toString(transaction_id)) as transaction_nk,
	transaction_id,
	user_id,
	nullIf(user_uuid, '') as user_uuid,
	amount,
	currency,
	transaction_type,
	promo_code_id,
	status,
	source_file,
	loaded_at
	FROM {{ source('analytics', 'raw_transactions') }}
),
duplicates as (
select
	transaction_nk,
	transaction_id,
	user_id,
	user_uuid,
	amount,
	currency,
	transaction_type,
	promo_code_id,
	status,
	created_at_full as created_at,
	source_file,
	loaded_at,
	row_number() over (
            partition by transaction_nk
order by
	created_at
        ) as transaction_dup_rn,
	count() over (
            partition by transaction_nk
        ) as transaction_dup_cnt
from
	prep_trans
)
select
	transaction_nk,
	transaction_id,
	user_id,
	user_uuid,
	amount,
	currency,
	transaction_type,
	promo_code_id,
	status,
	created_at,
	source_file,
	loaded_at,
    --Порядковый номер записи с одним ключом
    transaction_dup_rn,
    --Количество дублей внутри одного файла
    transaction_dup_cnt,
	-- Дубли transaction_id внутри одного файла
    if(transaction_dup_cnt > 1, 1, 0) as is_transaction_duplicate,
	-- Пустой пользователь
    if(user_uuid is null, 1, 0) as is_empty_user,
	-- Отрицательная сумма
    if(amount < 0, 1, 0) as is_negative_amount,
	-- Нулевая сумма
    if(amount = 0, 1, 0) as is_zero_amount,
    -- Успешная транзакция
    if(status = 'completed', 1, 0) as is_completed,
    -- Флаг покупки
    if(transaction_type = 'purchase', 1, 0) as is_purchase
from
	duplicates
