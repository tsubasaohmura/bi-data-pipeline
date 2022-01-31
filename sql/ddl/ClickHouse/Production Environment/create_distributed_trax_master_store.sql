CREATE TABLE commercial.trax_master_store ON CLUSTER prod_cluster
AS commercial.trax_master_store_local
ENGINE = Distributed(prod_cluster, commercial, trax_master_store_local, rand());