CREATE TABLE IF NOT EXISTS commercial.equipment_test
(
   WO_NO String,
   SAGYOSYA String,
   IN_OUT String,
   ADDAT Date,
   IDAT2 Date,
   TARGET_DATE String,
   WORK_NAME String,
   SOLD_TO String,
   SOLD_TO_PARTY String,
   SHIPTO String,
   SHIP_TO_PARTY String,
   FS_REG String,
   SERIAL_NUMBER String,
   STARTUP_DATE String,
   EQUIPMENT_CODE String,
   ITEM_CODE String,
   ITEM_NAME String,
   NEW_OLD String,
   CDE_TYPE_L_NAME String,
   CDE_TYPE_M_NAME String,
   CDE_TYPE_S_NAME String,
   CDE_BUDGET_CATEGORY_NAME String,
   PREFECTURE String,
   SERIAL_NUMBER2 String,
   STARTUP_DATE2 String,
   EQUIPMENT_CODE2 String,
   ITEM_CODE2 String,
   ITEM_NAME2 String,
   CDE_TYPE_L_NAME2 String,
   CDE_TYPE_M_NAME2 String,
   CDE_TYPE_S_NAME2 String,
   CDE_BUDGET_CATEGORY_NAME2 String,
   SOLD_TO2 String,
   SHIP_TO2 String,
   COMPANY_CODE String,
   VKBUR String,  
   BEZEI String,
   KPI_COUNT String,
   SYSTEM_STATUS String,
   WORK_CENTER_CODE String,
   KOSTL String,
   WBS String,
   AUART String,
   DELETE_FLAG String,
   ZJDIVGCD String,
   ZJDIVGCD__T String,
   ZJDIVCD String,
   ZJDIVCD__T String,
   ZJBJICLV1 String,
   ZJBJICLV__T String,
   ZJBJICLV2 String,
   ZJBJICLV2__T String,
   ZJBJICL_M String,
   ZJBJICL_M__T String,   
   ZJEJAL3 String,
   ZJEJAL3__T String,
   ZJKAMDPL1 String,
   ZJKAMDPL1__T String,
   CDE_BUDGET_CATEGORY String,
   CDE_TYPE_L String,
   CDE_TYPE_M String,
   CDE_TYPE_S String,
   SALES_FORCAST String,
   SALES_OFFICE_CODE String,
   Prefecture String,
   Salses_forcast String

)
ENGINE = MergeTree() 
ORDER BY ADDAT;