CREATE TABLE commercial.trax_transaction_photo ON CLUSTER prod_cluster
AS commercial.trax_transaction_photo_local
ENGINE = Distributed(prod_cluster, commercial, trax_transaction_photo_local, rand());