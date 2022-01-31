CREATE TABLE commercial.planopos_master_company
(
	COMPANY_CODE String,
	COMPANY_NAME String,
	SALES_DIVISION String,
	HQ String,
	DIV String,
	CHANNEL String,
	CUSTOMER_L3 String,
	CUSTOMER_EN String,
	NSR_RATIO Nullable(Decimal(18,10))
)
ENGINE = MergeTree
PARTITION BY HQ
ORDER BY (COMPANY_CODE, COMPANY_NAME, SALES_DIVISION, HQ, DIV, CHANNEL, CUSTOMER_L3);