CREATE TABLE commercial.alc_master_product_info
(
    PRODUCT_NAME String,
    JAN_CODE String,
    CAN_QTY Nullable(Decimal(16,6))
)
ENGINE = MergeTree()
ORDER BY (JAN_CODE);
