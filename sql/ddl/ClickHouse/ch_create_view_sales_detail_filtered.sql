CREATE VIEW IF NOT EXISTS commercial.hana_view_sales_detail_filtered
AS
SELECT * FROM commercial.hana_view_sales_detail_ej_filtered
UNION ALL SELECT * FROM commercial.hana_view_sales_detail_wj_filtered
UNION ALL SELECT * FROM commercial.hana_view_sales_detail_fv_filtered;