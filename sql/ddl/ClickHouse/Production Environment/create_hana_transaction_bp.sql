CREATE TABLE commercial.hana_transaction_bp_local ON CLUSTER prod_cluster
(
calday Date,
compared_date Date,
department_code Nullable(String),
district_code Nullable(String),
l3_code Nullable(String),
sales_amount Nullable(Decimal(18,6)),
actual_sales_price Nullable(Decimal(18,6)),
sales_revenue Nullable(Decimal(18,6)),
revenue_limit Nullable(Decimal(18,6))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(calday)
ORDER BY (calday, compared_date);