CREATE TABLE commercial.hana_master_zjmate_local ON CLUSTER prod_cluster
(
    zjancd Nullable(String),
    zjbevmnc___t Nullable(String),
    zjcatcd___t Nullable(String),
    zjddcscd___t Nullable(String),
    zjddctcd___t Nullable(String),
    zjejbrand___t Nullable(String),
    zjmate String,
    zjmate___t Nullable(String),
    zjnewprdc___t Nullable(String),
    zjpackcd___t Nullable(String),
    zjreldt Nullable(String),
    zjrpppkg___t Nullable(String),
    zjsbcatcd___t Nullable(String),
    zjsubcatc___t Nullable(String),
    zjbudtf Nullable(String)
)
ENGINE = MergeTree()
ORDER BY zjmate;