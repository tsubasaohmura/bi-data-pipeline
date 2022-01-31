CREATE TABLE commercial.trax_transaction_photo_local ON CLUSTER prod_cluster
(
    scene_id String,
    scene_uid String,
    scene_status Nullable(String),
    scene_closed_by_TTL Nullable(String),
    store_area_code Nullable(String),
    task_name Nullable(String),
    task_display_name Nullable(String),
    task_code Nullable(String),
    task_uuid String,
    thumbnail_url Nullable(String),
    preview_url Nullable(String),
    image_uid String,
    capture_time Date,
    customer_code Nullable(String),
    image_url_original String,
    image_url_medium Nullable(String),
    image_url_small Nullable(String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(capture_time)
ORDER BY (scene_id, scene_uid, capture_time);