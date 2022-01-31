CREATE VIEW commercial.planopos_stone_daily_1year 
AS SELECT
	sd.COMPANY_CODE,
	sd.STORE_CODE,
	sd.JAN_CODE,
	sd.PERIOD,
	sd.QUANTITY,
	sd.AMOUNT_OF_MONEY,
	sd.GROSS_PROFIT,
	sd.SELLING_PRICE,
	sd.SALE_QUANTITY,
	sd.SALE_AMOUNT,
	sd.BARGAIN_GROSS_PROFIT,
	sd.SALE_PRICE,
	sd.NUMBER_OF_DEALERS,
	sd.NUMBER_OF_VISITORS,
	sd.LOAD_DATE
FROM
	commercial.planopos_transaction_store_daily
AS sd
WHERE
    sd.PERIOD BETWEEN '2020-10-01' AND '2021-10-01';