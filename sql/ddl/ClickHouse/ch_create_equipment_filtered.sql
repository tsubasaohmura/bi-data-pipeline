CREATE VIEW IF NOT EXISTS commercial."equipment_filtered"
AS SELECT WO_NO AS OrderID,
 ADDAT AS DateScheduled,
 IDAT2 AS DatePlacement,
 CASE TARGET_DATE WHEN '202001' THEN '1'
 WHEN '202002' THEN '2'
 WHEN '202003' THEN '3'
 WHEN '202004' THEN '4'
 WHEN '202005' THEN '5'
 WHEN '202006' THEN '6'
 WHEN '202007' THEN '7'
 WHEN '202008' THEN '8'
 WHEN '202009' THEN '9'
 WHEN '202010' THEN '10'
 WHEN '202011' THEN '11'
 WHEN '202012' THEN '12'
 ELSE TARGET_DATE
 END AS DateMonth,
 WORK_NAME AS PlacementTypeJP,
 CASE WORK_NAME WHEN '新規設置' THEN 'NewPlacement'
 WHEN '交換' THEN 'Exchange'
 WHEN '玉突' THEN 'PileUp'
 WHEN '移動' THEN 'Move'
 WHEN '交換' THEN 'Exchange'
 WHEN '単独撤収' THEN 'Replace'
 ELSE WORK_NAME END AS PlacementTypeEN,
 SOLD_TO AS CustomerCode,
 SOLD_TO_PARTY AS CustomerName,
 ITEM_CODE AS ItemCode,
 ITEM_NAME AS ItemName,
 CDE_TYPE_L_NAME AS DeviceTypeL,
 CDE_TYPE_M_NAME AS DeviceTypeM,
 CDE_TYPE_S_NAME AS DeviceTypeS,
 Prefecture AS Prefecture,
 CASE ZJBJICLV__T WHEN '' THEN 'Other'
 WHEN 'Food' THEN 'RF'
 WHEN 'Retail' THEN 'RF'
 WHEN 'SMDD' THEN 'CS'
 ELSE ZJBJICLV__T END AS Channel,
 1 AS Amount

FROM tsubasa_test."equipment_test"