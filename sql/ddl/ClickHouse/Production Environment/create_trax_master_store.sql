CREATE TABLE commercial.trax_master_store_local ON CLUSTER prod_cluster
(
    customer_code String, 
    store_name String,
    office_code String,
    sales_center String,
    account_code String,
    account_name String,
    channel String,
    segment Nullable(String),
    city String,
    prefecture String
)
ENGINE = MergeTree()
ORDER BY customer_code;