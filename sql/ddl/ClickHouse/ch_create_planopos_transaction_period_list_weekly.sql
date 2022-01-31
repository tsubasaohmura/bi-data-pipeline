CREATE TABLE commercial.planopos_transaction_period_list_weekly 
(
	COMPANY_CODE String,
	STORE_CODE String,
	PERIOD DATE
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(PERIOD)
ORDER BY (PERIOD, COMPANY_CODE, STORE_CODE);