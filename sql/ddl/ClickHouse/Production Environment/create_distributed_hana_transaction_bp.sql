CREATE TABLE commercial.hana_transaction_bp ON CLUSTER prod_cluster
AS commercial.hana_transaction_bp_local
ENGINE = Distributed(prod_cluster, commercial, hana_transaction_bp_local, rand());