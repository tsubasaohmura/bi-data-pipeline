CREATE TABLE commercial.ar_transaction ON CLUSTER prod_cluster
(

   Email String,
   DayOfUsage Date,
   UsageAR Float

)
ENGINE = MergeTree() 
ORDER BY Email;