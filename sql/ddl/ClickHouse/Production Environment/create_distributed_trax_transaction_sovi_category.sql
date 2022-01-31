CREATE TABLE commercial.trax_transaction_sovi_category ON CLUSTER prod_cluster
AS commercial.trax_transaction_sovi_category_local
ENGINE = Distributed(prod_cluster, commercial, trax_transaction_sovi_category_local, rand());