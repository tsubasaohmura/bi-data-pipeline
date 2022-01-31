CREATE TABLE commercial.planopos_transaction_company_weekly
(
	COMPANY_CODE String,
	STORE_CODE String,
	JAN_CODE String,
	PERIOD DATE,
	QUANTITY Nullable(Decimal(34,4)),
	AMOUNT_OF_MONEY Nullable(Decimal(34,4)),
	GROSS_PROFIT Nullable(Decimal(34,4)),
	SELLING_PRICE Nullable(Decimal(34,4)),
	SALE_QUANTITY Nullable(Decimal(34,4)),
	SALE_AMOUNT Nullable(Decimal(34,4)),
	BARGAIN_GROSS_PROFIT Nullable(Decimal(34,4)),
	SALE_PRICE Nullable(Decimal(34,4)),
	NUMBER_OF_DEALERS Nullable(Decimal(34,4)),
	NUMBER_OF_VISITORS Nullable(Decimal(38,4)),
	LOAD_DATE DATE
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(PERIOD)
ORDER BY (PERIOD, LOAD_DATE, COMPANY_CODE, STORE_CODE, JAN_CODE);