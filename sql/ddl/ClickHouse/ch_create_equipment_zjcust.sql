CREATE TABLE IF NOT EXISTS commercial.equipment_cust
(
   ZJCDSRECD String,
   ZJCDSRECD___T String,
   ZJLOCACD String,
   ZJLOCACD___T String,
   ZJSMWERKS String,
   ZJSMPDROC String


)
ENGINE = MergeTree() 
ORDER BY ZJCDSRECD;
