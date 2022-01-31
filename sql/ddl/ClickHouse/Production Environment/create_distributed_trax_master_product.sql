CREATE TABLE commercial.trax_master_product ON CLUSTER prod_cluster
AS commercial.trax_master_product_local
ENGINE = Distributed(prod_cluster, commercial, trax_master_product_local, rand());