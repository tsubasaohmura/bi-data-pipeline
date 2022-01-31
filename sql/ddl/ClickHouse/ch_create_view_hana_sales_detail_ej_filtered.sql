CREATE VIEW IF NOT EXISTS commercial.hana_view_sales_detail_ej_filtered
AS
SELECT sales_detail.*
FROM commercial.sales_detail
	JOIN commercial.zjcust on sales_detail.zjcustbrg = zjcust.zjcust
	JOIN commercial.zjmate on sales_detail.zjmatrial = zjmate.zjmate
WHERE zjmate.zjbudtf in ('X:バジェット対象(飲料製品)', 'Y:バジェット対象(飲料以外・KO-EAST/FV)')
    and zjcust.zjdfflg = '0:通常'
    and zjcust.zjdivold2 in ('13', '21', '22', '23', '24', '28', '32', '39')
    and zjcust.zjddexfl___t = '送信対象';
