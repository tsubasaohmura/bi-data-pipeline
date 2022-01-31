CREATE TABLE commercial.trax_transaction_red_score ON CLUSTER prod_cluster
AS commercial.trax_transaction_red_score_local
ENGINE = Distributed(prod_cluster, commercial, trax_transaction_red_score_local, rand());