CREATE VIEW commercial.planopos_report_compnay_weekly_middle(
      PERIOD,
      HQ,
      CHANNEL,
      DIV,
      COMPANY_CODE,
      CUSTOMER_EN,
      CUSTOMER_L3,
      LARGE_MANUFACTURER,
      MIDDLE_CATEGORY,
      LARGE_PKG,
      NSR_RATIO,
      SUM_AMOUNT_OF_MONEY,
      SUM_QUANTITY,
      SUM_QUANTITY_IRISUKANSAN
) AS (
  WITH COMPANY_WEEKLY_REPORTS AS (
    SELECT 
      C.COMPANY_CODE, C.PERIOD, C.QUANTITY, C.AMOUNT_OF_MONEY, C.GROSS_PROFIT, C.SELLING_PRICE, C.SALE_QUANTITY, C.SALE_AMOUNT, C.BARGAIN_GROSS_PROFIT, C.SALE_PRICE,
      M_G.SKU, M_G.LARGE_PKG, M_G.LARGE_MANUFACTURER,M_G.MIDDLE_CATEGORY,M_G.QUANTITY AS MG_QUANTITY,
      M_C.COMPANY_NAME, M_C.HQ, M_C.DIV, M_C.CHANNEL,M_C.CUSTOMER_EN, M_C.CUSTOMER_L3, M_C.NSR_RATIO,
      toString(C.PERIOD,'yyyy') AS STR_YEAR,
      toString(C.PERIOD,'mmddW') AS STR_WEEKDATE
    FROM commercial.planopos_company_weekly_with_period_list AS C
    INNER JOIN GOODS_MASTER AS M_G ON C.JAN_CODE = TRIM(M_G.SKU)
    INNER JOIN COMPANY_MASTER AS M_C ON C.COMPANY_CODE = M_C.COMPANY_CODE
  )
  SELECT 
      PERIOD,
      HQ,
      CHANNEL,
      DIV,
      COMPANY_CODE,
      CUSTOMER_EN,
      CUSTOMER_L3,
      LARGE_MANUFACTURER,
      MIDDLE_CATEGORY,
      LARGE_PKG,
      NSR_RATIO,
      SUM(AMOUNT_OF_MONEY),
      SUM(QUANTITY),
      SUM(QUANTITY*MG_QUANTITY)
  FROM COMPANY_WEEKLY_REPORTS
  GROUP BY PERIOD, LARGE_MANUFACTURER, MIDDLE_CATEGORY, HQ, CHANNEL, DIV, COMPANY_CODE, CUSTOMER_EN, CUSTOMER_L3, LARGE_PKG, NSR_RATIO
);