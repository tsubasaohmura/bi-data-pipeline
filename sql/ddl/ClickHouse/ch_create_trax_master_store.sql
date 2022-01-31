CREATE TABLE commercial.trax_master_store ON CLUSTER prod_cluster
(
    customer_code String, 
    store_name String,
    store_display_name Nullable(String),
    store_type_name Nullable(String),
    street Nullable(String),
    address_line_2 Nullable(String),
    city String,
    postal_code Nullable(String),
    latitude Nullable(Decimal(18,10)),
    longitude Nullable(Decimal(18,10)),
    manager_name Nullable(String),
    manager_phone Nullable(String),
    manager_email Nullable(String),
    region_name String,
    district_name,
    branch_name,
    retailer_name,
    state_code,
    store_additional_attribute_1 String,
    account_code String,
    account_name String,
    office_code String,
    office_name String,
    route_code String,
    person_in_charge String,
    person_in_charge_mail String,
    channel String,
    segment Nullable(String),
    prefecture String,
    is_active String,
    additional_attribute_8 Nullable(String),
    additional_attribute_11 Nullable(String),
    additional_attribute_12 Nullable(Decimal(16,8)),
    additional_attribute_14 Nullable(String),
    additional_attribute_15 Nullable(String),
    additional_attribute_16 Nullable(String),
    additional_attribute_17 Nullable(String),
    additional_attribute_18 Nullable(String),
    additional_attribute_19 Nullable(String),
    additional_attribute_20 Nullable(String),
    additional_attribute_21 Nullable(String),
    additional_attribute_22 Nullable(String)
)
ENGINE = MergeTree()
ORDER BY region_name, customer_code, account_code, office_code, route_code, prefecture;