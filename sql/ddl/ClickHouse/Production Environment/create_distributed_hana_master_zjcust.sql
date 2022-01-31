CREATE TABLE commercial.hana_master_zjcust ON CLUSTER prod_cluster
AS commercial.hana_master_zjcust_local
ENGINE = Distributed(prod_cluster, commercial, hana_master_zjcust_local, rand());