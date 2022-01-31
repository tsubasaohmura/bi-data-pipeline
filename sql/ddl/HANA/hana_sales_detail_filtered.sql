-- HANA daily sales only columns needed for DBP
-- Work in progress, unfinished selection
SELECT "CALDAY"
, "CALDAY_prevyear"
, "BW_CALDAY_WEEK"
, "CALDAY_WEEK_prevyear"
, "ZJMATRIAL"
, COUNT("ZJMATRIAL") AS "NUM_TRANSACTIONS"
, "COMP_CODE"
, "ZJCUSTBRG"
, "ZJ445QATR"
, "ZJ445WEEK"
, "WERKS"
, "WW202"
, "PLANT_T"
, SUM("CM_CY_BAPC")
, SUM("CM_LY_BAPC")
, SUM("CM_CY_BARASU")
, SUM("CM_LY_BARASU")
, SUM("CM_CY_NSR")
, SUM("CM_LY_NSR")
, SUM("CM_CY_COGS")
, SUM("CM_LY_COGS")
, SUM("CM_CY_PC")
, SUM("CM_LY_PC")
, SUM("CM_CY_RB")
, SUM("CM_LY_RB")
, SUM("CM_CY_GP")
, SUM("CM_LY_GP")
, SUM("CM_CY_FSCOM")
, SUM("CM_LY_FSCOM")
, SUM("CM_CY_LOGISTICS_CST")
, SUM("CM_LY_LOGISTICS_CST")
, SUM("CM_CY_SAL_DEL_CST")
, SUM("CM_LY_SAL_DEL_CST")
, SUM("CM_CY_CEN_SHP_CST")
, SUM("CM_LY_CEN_SHP_CST")
, SUM("CM_CY_CENT_FEE")
, SUM("CM_LY_CENT_FEE")
, SUM("CM_CY_BS_EN_FEE")
, SUM("CM_LY_BS_EN_FEE")
, SUM("CM_CY_TRP_CST")
, SUM("CM_LY_TRP_CST")
, SUM("CM_CY_MP")
, SUM("CM_LY_MP")
, SUM("CM_CY_JR_OPR_CST")
, SUM("CM_LY_JR_OPR_CST")
, SUM("CM_CY_JR_TR_FEE")
, SUM("CM_LY_JR_TR_FEE")
FROM CCEJ_VIRTUAL."ccej.zj_sales.ccw.SalesReporting_Perf_Opt.data::persisted_sales_details.zj_sr_tr_sales_detail"
WHERE 
    "BW_CALDAY_WEEK" <> ''
    AND "CALDAY" = '20200901'
GROUP BY
    "CALDAY"
    , "BW_CALDAY_WEEK"
    , "CALDAY_prevyear"
    , "CALDAY_WEEK_prevyear"
    , "ZJMATRIAL"
    , "COMP_CODE"
    , "ZJCUSTBRG"
    , "ZJ445QATR"
    , "ZJ445WEEK"
    , "WERKS"
    , "WW202"
    , "PLANT_T"
ORDER BY
    "CALDAY"
    , "BW_CALDAY_WEEK"
    , "CALDAY_prevyear"
    , "CALDAY_WEEK_prevyear"
    , "ZJMATRIAL"
    , "COMP_CODE"
    , "ZJCUSTBRG"
    , "ZJ445QATR"
    , "ZJ445WEEK"
    , "WERKS"
    , "WW202"
    , "PLANT_T"