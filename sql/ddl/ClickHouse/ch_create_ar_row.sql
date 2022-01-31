CREATE commercial.ar_row ON CLUSTER prod_cluster
(

   Email String,
   UsageAR Integer

)
ENGINE = MergeTree() 
ORDER BY Email;