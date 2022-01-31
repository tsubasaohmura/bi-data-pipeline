CREATE TABLE commercial.hana_master_zjmate ON CLUSTER prod_cluster
AS commercial.hana_master_zjmate_local
ENGINE = Distributed(prod_cluster, commercial, hana_master_zjmate_local, rand());