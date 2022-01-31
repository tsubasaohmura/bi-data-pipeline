CREATE TABLE commercial.planopos_master_goods (
  `SKU` String,
  `PRODUCT_NAME_1` String,
  `PRODUCT_NAME_2` String,
  `PRODUCT_NAME_3` String,
  `QUANTITY` Nullable(Decimal(34, 4)),
  `LARGE_CATEGORY` String,
  `MIDDLE_CATEGORY` String,
  `SMALL_CATEGORY` String,
  `DETAIL_MANUFACTURER` String,
  `LARGE_CATEGORY_SPECIAL_INSURANCE_FUNCTIONAL_GENDER` String,
  `MIDDLE_CATEGORY_SPECIAL_INSURANCE_FUNCTIONAL_GENDER` String,
  `SEGMENT` String,
  `SEGMENT_SPECIAL_INSURANCE_FUNCTIONAL_GENDER` String,
  `CONTAINER` String,
  `LARGE_PKG` String,
  `LARGE_PKG_2` String,
  `LARGE_MANUFACTURER` String,
  `MIDDLE_MANUFACTURER` String,
  `SPECIAL_INSURANCE_FUNCTIONALITY` String,
  `SPECIAL_INSURANCE_FUNCTIONALITY_WITH_EFFICACY` String,
  `COFFEECLUSTER` String,
  `PITCH` String,
  `SPLIT_MATERIAL_FLG` String,
  `MULTI_CLASSIFICATION` String,
  `NEW_GOODS` String,
  `ALCOHOL_DEGREE` String,
  `SIZE` String,
  `CONTAINER_SIZE` String,
  `BRAND` String,
  `JS_CODE` String,
  `WIDTH` String,
  `HEIGHT` String,
  `DEPTH` String,
  `PRODUCT_RELEASE_DATE` String,
  `REGISTRATION_DATE` Date,
  `CONTENTS` String,
  `BOTTLER_NAME` String
) ENGINE = MergeTree PARTITION BY toYYYYMMDD(REGISTRATION_DATE)
ORDER BY
  (
    SKU,
    PRODUCT_NAME_1,
    PRODUCT_NAME_2,
    PRODUCT_NAME_3,
    LARGE_CATEGORY,
    JS_CODE,
    SEGMENT
  ) SETTINGS index_granularity = 8192