CREATE TABLE commercial.trax_transaction_msl_product ON CLUSTER prod_cluster
AS commercial.trax_transaction_msl_product_local
ENGINE = Distributed(prod_cluster, commercial, trax_transaction_msl_product_local, rand());