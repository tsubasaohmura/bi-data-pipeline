CREATE VIEW commercial.planopos_company_weekly_with_period_list 
AS SELECT
	cw.COMPANY_CODE,
	cw.STORE_CODE,
	cw.JAN_CODE,
	cw.PERIOD,
	cw.QUANTITY,
	cw.AMOUNT_OF_MONEY,
	cw.GROSS_PROFIT,
	cw.SELLING_PRICE,
	cw.SALE_QUANTITY,
	cw.SALE_AMOUNT,
	cw.BARGAIN_GROSS_PROFIT,
	cw.SALE_PRICE,
	cw.NUMBER_OF_DEALERS,
	cw.NUMBER_OF_VISITORS,
	cw.LOAD_DATE
FROM
	commercial.planopos_transaction_company_weekly
AS cw
INNER JOIN (
	SELECT
		COMPANY_CODE,
		PERIOD
	FROM
		commercial.planopos_transaction_period_list_weekly
	WHERE
		STORE_CODE IS NULL
) plw ON
	cw.COMPANY_CODE = plw.COMPANY_CODE
	AND cw.PERIOD = plw.PERIOD