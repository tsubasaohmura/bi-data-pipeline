CREATE TABLE commercial.alc_master_customer_softdrink
(
    CUSTOMER_CODE_LV3 String,
    CUSTOMER_NAME_LV3 String,
    CHANNEL String,
    SEGMENT String
)
ENGINE = MergeTree()
ORDER BY (CHANNEL, CUSTOMER_CODE_LV3);