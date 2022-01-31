CREATE TABLE commercial.trax_transaction_unique_distribution ON CLUSTER prod_cluster
AS commercial.trax_transaction_unique_distribution_local
ENGINE = Distributed(prod_cluster, commercial, trax_transaction_unique_distribution_local, rand());