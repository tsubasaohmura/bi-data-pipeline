CREATE TABLE commercial.hana_transaction_sales_detail ON CLUSTER prod_cluster
AS commercial.hana_transaction_sales_detail_local
ENGINE = Distributed(prod_cluster, commercial, hana_transaction_sales_detail_local, rand());