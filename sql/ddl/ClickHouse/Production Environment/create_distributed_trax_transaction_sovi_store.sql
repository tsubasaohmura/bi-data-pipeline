CREATE TABLE commercial.trax_transaction_sovi_store ON CLUSTER prod_cluster
AS commercial.trax_transaction_sovi_store_local
ENGINE = Distributed(prod_cluster, commercial, trax_transaction_sovi_store_local, rand());