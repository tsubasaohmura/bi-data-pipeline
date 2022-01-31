CREATE TABLE commercial.trax_transaction_poc_score ON CLUSTER prod_cluster
AS commercial.trax_transaction_poc_score_local
ENGINE = Distributed(prod_cluster, commercial, trax_transaction_poc_score_local, rand());