CREATE TABLE commercial.hana_transaction_daily_sales_for_mtd
(
calday Date,
calday_prevyear Date,
sales_region_code String,
sales_region_name String,
sales_district_code String,
sales_district_name String,
sales_branch_code String,
sales_branch_name String,
channel_l2_code String,
channel_l2_name String,
channel_l3_code String,
channel_l3_name String,
bapc_cy Nullable(Decimal(18,6)),
bapc_ly Nullable(Decimal(18,6)),
bapc_vs_ly Nullable(Decimal(18,6)),
bapc_vs_ly_percentage Nullable(Decimal(18,6)),
gp_bapc Nullable(Decimal(18,6)),
nsr_cy Nullable(Decimal(18,6)),
nsr_ly Nullable(Decimal(18,6)),
nsr_vs_ly Nullable(Decimal(18,6)),
nsr_vs_ly_percentage Nullable(Decimal(18,6)),
gp Nullable(Decimal(18,6)),
gp_ly Nullable(Decimal(18,6))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(calday)
ORDER BY (calday, calday_prevyear, sales_region_code, sales_district_code, sales_branch_code, channel_l2_code, channel_l3_code);