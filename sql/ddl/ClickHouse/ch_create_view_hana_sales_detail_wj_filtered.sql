CREATE VIEW IF NOT EXISTS commercial.hana_view_sales_detail_wj_filtered
AS
SELECT sales_detail.*
FROM commercial.sales_detail
	JOIN commercial.zjcust on sales_detail.zjcustbrg = zjcust.zjcust
	JOIN commercial.zjmate on sales_detail.zjmatrial = zjmate.zjmate
WHERE zjmate.zjbudtf = 'X:バジェット対象(飲料製品)'
    and zjcust.zjdfflg = '0:通常'
    and zjcust.zjdivold2 in ('35', '42')
    and zjcust.zjddexfl___t = '送信対象';
