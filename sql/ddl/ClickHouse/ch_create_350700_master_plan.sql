CREATE TABLE commercial.threeseven_master_plan ON CLUSTER prod_cluster
(
    jan_code String,
    case_jan_code String,
    js String,
    sku String,
    channel String,
    plan_type String,
    week String,
    year String,
    value Nullable(Decimal(18,5))
)
ENGINE = MergeTree()
ORDER BY (jan_code, case_jan_code);