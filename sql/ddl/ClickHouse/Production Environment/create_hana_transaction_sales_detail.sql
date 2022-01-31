CREATE TABLE commercial.hana_transaction_sales_detail_local ON CLUSTER prod_cluster
(
calyear LowCardinality(String),
calmonth LowCardinality(String),
calday Date,
budat Nullable(Date),
bw_calday_week Nullable(Date),
year_ind LowCardinality(String),
comp_code LowCardinality(String),
zjcustbrg String,
zjmatrial String,
zj445qatr LowCardinality(Nullable(String)),
zj445week LowCardinality(Nullable(String)),
calday_prevyear Nullable(Date),
calday_week_prevyear Nullable(Date),
werks LowCardinality(Nullable(String)),
ww202 LowCardinality(Nullable(String)),
zjmatagsp Nullable(String),
absmg_me Nullable(String),
absmg_me_ly Nullable(String),
absmg_cy Nullable(Decimal(18,5)),
absmg_ly Nullable(Decimal(18,5)),
cm_cy_bapc Nullable(Decimal(18,6)),
cm_ly_bapc Nullable(Decimal(18,6)),
cm_cy_barasu Nullable(Decimal(18,6)),
cm_ly_barasu Nullable(Decimal(18,6)),
cm_cy_nsr Nullable(Decimal(18,6)),
cm_ly_nsr Nullable(Decimal(18,6)),
cm_cy_cogs Nullable(Decimal(18,6)),
cm_ly_cogs Nullable(Decimal(18,6)),
cm_cy_pc Nullable(Decimal(18,6)),
cm_ly_pc Nullable(Decimal(18,6)),
cm_cy_rb Nullable(Decimal(18,6)),
cm_ly_rb Nullable(Decimal(18,6)),
cm_cy_gp Nullable(Decimal(18,6)),
cm_ly_gp Nullable(Decimal(18,6)),
cm_cy_fscom Nullable(Decimal(18,6)),
cm_ly_fscom Nullable(Decimal(18,6)),
cm_cy_logistics_cst Nullable(Decimal(18,6)),
cm_ly_logistics_cst Nullable(Decimal(18,6)),
cm_cy_sal_del_cst Nullable(Decimal(18,6)),
cm_ly_sal_del_cst Nullable(Decimal(18,6)),
cm_cy_cen_shp_cst Nullable(Decimal(18,6)),
cm_ly_cen_shp_cst Nullable(Decimal(18,6)),
cm_cy_cent_fee Nullable(Decimal(18,6)),
cm_ly_cent_fee Nullable(Decimal(18,6)),
cm_cy_bs_en_fee Nullable(Decimal(18,6)),
cm_ly_bs_en_fee Nullable(Decimal(18,6)),
cm_cy_trp_cst Nullable(Decimal(18,6)),
cm_ly_trp_cst Nullable(Decimal(18,6)),
cm_cy_mp Nullable(Decimal(18,6)),
cm_ly_mp Nullable(Decimal(18,6)),
cm_cy_jr_opr_cst Nullable(Decimal(18,6)),
cm_ly_jr_opr_cst Nullable(Decimal(18,6)),
cm_cy_jr_tr_fee Nullable(Decimal(18,6)),
cm_ly_jr_tr_fee Nullable(Decimal(18,6)),
vv511_cy Nullable(Decimal(15,2)),
vv511_ly Nullable(Decimal(15,2)),
vv512_cy Nullable(Decimal(15,2)),
vv512_ly Nullable(Decimal(15,2)),
vv513_cy Nullable(Decimal(15,2)),
vv513_ly Nullable(Decimal(15,2)),
vv517_cy Nullable(Decimal(15,2)),
vv517_ly Nullable(Decimal(15,2)),
vv521_cy Nullable(Decimal(15,2)),
vv521_ly Nullable(Decimal(15,2)),
vv522_cy Nullable(Decimal(15,2)),
vv522_ly Nullable(Decimal(15,2)),
vv523_cy Nullable(Decimal(15,2)),
vv523_ly Nullable(Decimal(15,2)),
vv524_cy Nullable(Decimal(15,2)),
vv524_ly Nullable(Decimal(15,2)),
vv525_cy Nullable(Decimal(15,2)),
vv525_ly Nullable(Decimal(15,2)),
vv526_cy Nullable(Decimal(15,2)),
vv526_ly Nullable(Decimal(15,2)),
vv527_cy Nullable(Decimal(15,2)),
vv527_ly Nullable(Decimal(15,2)),
vv528_cy Nullable(Decimal(15,2)),
vv528_ly Nullable(Decimal(15,2)),
mgr_data_type LowCardinality(Nullable(String)),
hana_view LowCardinality(Nullable(String)),
plant_t LowCardinality(Nullable(String)),
CM_CY_OFF_INV_RB Nullable(String),
CM_LY_OFF_INV_RB Nullable(String),
CM_CY_RPP_NSR Nullable(String),
CM_LY_RPP_NSR Nullable(String),
CM_CY_CCR_PRC Nullable(String),
CM_LY_CCR_PRC Nullable(String),
CM_CY_OTR_CST Nullable(String),
CM_LY_OTR_CST Nullable(String),
CM_CY_NET_CCR_PRC Nullable(String),
CM_LY_NET_CCR_PRC Nullable(String),
CM_CY_OTR_RB Nullable(String),
CM_LY_OTR_RB Nullable(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(calday)
ORDER BY (calday, year_ind, comp_code, zjcustbrg, zjmatrial);