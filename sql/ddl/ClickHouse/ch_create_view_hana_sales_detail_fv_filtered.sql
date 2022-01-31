CREATE VIEW IF NOT EXISTS commercial.hana_view_sales_detail_fv_filtered
AS
SELECT sales_detail.*
FROM commercial.sales_detail
	JOIN commercial.zjcust on sales_detail.zjcustbrg = zjcust.zjcust
	JOIN commercial.zjmate on sales_detail.zjmatrial = zjmate.zjmate
WHERE zjmate.zjbudtf in ('X:バジェット対象(飲料製品)', 'Y:バジェット対象(飲料以外・KO-EAST/FV)', 'Z:バジェット対象(飲料以外・FVのみ)')
    and zjcust.zjdfflg in ('2: Vending_OCS (223)', '3: 営業本部外');
