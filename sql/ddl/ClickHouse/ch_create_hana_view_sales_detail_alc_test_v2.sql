CREATE VIEW hana_view_sales_detail_alc_v2 ON CLUSTER prod_cluster
AS SELECT
    T.calmonth AS yearmonth,
    T.calday AS date,
	M1._zero_region AS prefecture_code,
	M1._zero_region___t AS prefecture_name_en,
	M1.zjsreigp___t AS prefecture_name_jp,
    M1.zjdivgcd AS sales_region_code,
    M1.zjdivgcd___t AS sales_region_name,
    M1.zjdivcd AS sales_district_code,
    M1.zjdivcd___t AS sales_district_name,
    M1.zjrgncd AS sales_branch_code,
    M1.zjrgncd___t AS sales_branch_name,
    M1.zjslsctcd AS sales_center_code,
    M1.zjslsctcd___t AS sales_center_name,
    M1.zjchncd AS ccej_channel_code,
    M1.zjchncd___t AS ccej_channel_name,
    M1.zjbjiclv1 AS channel_lv_1_code,
    M1.zjbjiclv1___t AS channel_lv_1_name,
    M1.zjbjiclv2 AS channel_lv_2_code,
    M1.zjbjiclv2___t AS channel_lv_2_name,
    M1.zjbjicl_m AS channel_lv_3_code,
    M1.zjbjicl_m___t AS channel_lv_3_name,
    M1.zjdlrc AS customer_code,
    M1.zjdlrc___t AS customer_name,
    M2.zjancd AS jan_code,
    M2.zjmatagsp AS product_code,
    M2.zjmatagg___t AS product_name,
    M2.zjejbrand AS brand_code,
    M2.zjejbrand___t AS brand_name,
    M2.zjcatcd AS category_code,
    M2.zjcatcd___t AS category_name,
    M2.zjsbcatcd AS sub_category_code,
    M2.zjsbcatcd___t AS sub_category_name,
    T.cm_cy_bapc AS bapc_cy,
    T.cm_ly_bapc AS bapc_ly,
    T.cm_cy_nsr AS nsr_cy,
    T.cm_ly_nsr AS nsr_ly,
    T.cm_cy_gp AS gp_cy,
    T.cm_ly_gp AS gp_ly,
    T.cm_cy_cogs AS cogs_cy,
    T.cm_ly_cogs AS cogs_ly,
    T.cm_cy_rb AS rpp_rebate_cy,
    T.cm_ly_rb AS rpp_rebate_ly,
    T.cm_cy_pc AS purchase_price_cy,
    T.cm_ly_pc AS purchase_price_ly
FROM commercial.sales_detail AS T
    JOIN    (
        SELECT 
            zjcust, zjdlrc___t, zjcust___t,
            zjdlrc, zjbjiclv1, zjbjiclv1___t,
            zjbjiclv2, zjbjiclv2___t, zjbjicl_m,
            zjbjicl_m___t, zjchncd, zjchncd___t,
            zjdivgcd, zjdivgcd___t, zjdivcd,
            zjdivcd___t, zjrgncd, zjrgncd___t,
            zjslsctcd, zjslsctcd___t, 
            _zero_region, _zero_region___t,zjsreigp___t
        FROM commercial.hana_master_zjcust_all
    ) M1 ON T.zjcustbrg = M1.zjcust
    JOIN    (
        SELECT 
            zjmate, zjmatagsp, zjmatagg___t,
            zjancd, zjcatcd, zjcatcd___t,
            zjsbcatcd, zjsbcatcd___t, zjejbrand, zjejbrand___t
        FROM commercial.hana_master_zjmate_all
    ) M2 ON T.zjmatrial = M2.zjmate
WHERE M2.zjcatcd___t = 'ｱﾙｺｰﾙ';