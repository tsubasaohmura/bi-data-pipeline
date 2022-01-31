CREATE TABLE commercial.trax_transaction_red_score_local ON CLUSTER prod_cluster
(
    session_id_unique String,
    email String,
    visit_date Date,
    start_time Nullable(DateTime),
    end_time Nullable(DateTime),
    visit_type Nullable(String), 
    gps_latitude Nullable(String),
    gps_longitude Nullable(String),
    store_latitude Nullable(String),
    store_longitude Nullable(String),
    customer_code Nullable(String),
    red_score Nullable(Decimal(18,5))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(visit_date)
ORDER BY (session_id_unique, email, visit_date);