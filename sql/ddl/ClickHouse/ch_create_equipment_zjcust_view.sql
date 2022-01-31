CREATE VIEW IF NOT EXISTS commercial."zjcust_ar_equipment"
AS SELECT DISTINCT 
  zjcdsrecd AS EmployeeCode,
  zjcdsrecd___t AS EmployeeName,
  zjlocacd AS CustomerCode,
  zjlocacd___t AS CustomerName,
  zjsmwerks AS WorkCode,
  zjsmpdroc AS RouteCode
FROM commercial."zjcust";