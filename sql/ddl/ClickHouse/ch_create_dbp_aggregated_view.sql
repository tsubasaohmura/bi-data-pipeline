-- Work in progress
CREATE MATERIALIZED VIEW IF NOT EXISTS commercial."daily_business_pulse"
ENGINE = MergeTree
ORDER BY (calday, year_ind, comp_code, zjcustbrg, zjmatrial)
POPULATE AS 
SELECT
    detail."calday" AS calday
    , customer."zero_region___t" AS region
    , customer."zjbjiclv2___t" AS channel_lv2
    , customer."zjsreigce" AS sales_center
    , customer."zjcust" AS customer_id
    , material."zjbevmnc___t" AS maker
    , material."zjcatcd___t" AS category
    , material."zjsubcatc___t" AS subcategory_jp
    , material."zjsbcatcd___t" AS subcategory_en
    , material."zjnewprdc___t" AS product_type
    , material."zjejbrand___t" AS brand
    , material."zjancd" AS jan_code
    , material."zjmate" AS product_code
    , material."zjmate___t" AS product_name_jp
    , material."zjpackcd___t" AS package
    , material."zjddctcd___t" AS package_type
    , material."zjddcscd___t" AS package_size
    , material."zjrpppkg___t" AS rpp_package
    , material."zjreldt" AS release_date  
    , SUM(detail.cm_cy_bapc) AS bapc_cy
    , SUM(detail.cm_ly_bapc) AS bapc_ly
    , SUM(detail.cm_cy_nsr) AS nsr_cy
    , SUM(detail.cm_ly_nsr) AS nsr_ly
    , SUM(detail.cm_cy_cogs) AS cogs_cy
    , SUM(detail.cm_ly_cogs) AS cogs_ly
    , SUM(detail.cm_cy_gp) AS gp_cy
    , SUM(detail.cm_ly_gp) AS gp_ly
    FROM commercial."sales_detail" AS detail
    LEFT JOIN commercial."zjcust" AS customer ON (detail."zjcustbrg" = customer."zjcust")
    LEFT JOIN commercial."zjmate" AS material ON (detail."zjmatrial" = material."zjmate")
    GROUP BY 
        calday
        , region
        , channel_lv2
        , sales_center
        , customer_id
        , maker
        , category
        , subcategory_jp
        , subcategory_en
        , product_type
        , brand
        , jan_code
        , product_code
        , product_name_jp
        , package
        , package_type
        , package_size
        , rpp_package
        , release_date
    ORDER BY
        calday
        , region
        , channel_lv2
        , sales_center
        , customer_id
        , maker
        , category
        , subcategory_jp
        , subcategory_en
        , product_type
        , brand
        , jan_code
        , product_code
        , product_name_jp
        , package
        , package_type
        , package_size
        , rpp_package
        , release_date
LIMIT 50;