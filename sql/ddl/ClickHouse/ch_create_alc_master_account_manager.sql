CREATE TABLE commercial.alc_master_account_manager
(
    CUSTOMER_CODE_BJI String,
    CUSTOMER_NAME_BJI String,
    CHANNEL String,
    SALES_TEAM_NAME String,
    ACCOUNT_MANAGER_NAME String
)
ENGINE = MergeTree()
ORDER BY (CHANNEL, CUSTOMER_CODE_BJI);