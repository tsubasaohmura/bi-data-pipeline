CREATE TABLE commercial.trax_master_product_local ON CLUSTER prod_cluster
(
    product_code String,
    product_name String,
    product_type Nullable(String),
    product_uuid String,
    product_local_name Nullable(String),
    product_short_name Nullable(String),
    brand_name Nullable(String),
    brand_local_name Nullable(String),
    manufacturer_name Nullable(String),
    manufacturer_local_name Nullable(String),
    is_deleted String,
    product_client_code Nullable(String),
    container_type Nullable(String),
    size Nullable(String),
    unit_measurement Nullable(String),
    is_active String,
    category_name Nullable(String),
    category_local_name Nullable(String),
    images Nullable(String)
)
ENGINE = MergeTree()
ORDER BY product_uuid;