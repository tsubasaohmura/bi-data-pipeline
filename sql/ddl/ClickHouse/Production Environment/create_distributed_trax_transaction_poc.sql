CREATE TABLE commercial.trax_transaction_poc ON CLUSTER prod_cluster
AS commercial.trax_transaction_poc_local
ENGINE = Distributed(prod_cluster, commercial, trax_transaction_poc_local, rand());