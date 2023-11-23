CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.surge_mov_my_afv_ntiles` AS
SELECT 
    entity_id, city_name, zone_name, scheme_id, CONCAT(zone_name, '|', scheme_id) AS zone_scheme,
    AVG(gfv_ntile_5) AS gfv_ntile_5,
    AVG(gfv_ntile_10) AS gfv_ntile_10,
    AVG(gfv_ntile_15) AS gfv_ntile_15,
    AVG(gfv_ntile_20) AS gfv_ntile_20,
    AVG(gfv_ntile_25) AS gfv_ntile_25,
    AVG(gfv_ntile_30) AS gfv_ntile_30,
    AVG(gfv_ntile_35) AS gfv_ntile_35,
    AVG(gfv_ntile_40) AS gfv_ntile_40,
    AVG(gfv_ntile_45) AS gfv_ntile_45,
    AVG(gfv_ntile_50) AS gfv_ntile_50,
    AVG(gfv_ntile_55) AS gfv_ntile_55,
    AVG(gfv_ntile_60) AS gfv_ntile_60,
    AVG(gfv_ntile_65) AS gfv_ntile_65,
    AVG(gfv_ntile_70) AS gfv_ntile_70,
    AVG(gfv_ntile_75) AS gfv_ntile_75,
    AVG(gfv_ntile_80) AS gfv_ntile_80,
    AVG(gfv_ntile_85) AS gfv_ntile_85,
    AVG(gfv_ntile_90) AS gfv_ntile_90,
    AVG(gfv_ntile_95) AS gfv_ntile_95,
    AVG(gfv_ntile_100) AS gfv_ntile_100,
    COUNT(DISTINCT order_id) AS Order_count
FROM (
SELECT
    *,
    PERCENTILE_CONT(gfv_local, 0.05) OVER(PARTITION BY entity_id, city_name, zone_name, scheme_id) AS gfv_ntile_5, 
    PERCENTILE_CONT(gfv_local, 0.1) OVER(PARTITION BY entity_id, city_name, zone_name, scheme_id) AS gfv_ntile_10, 
    PERCENTILE_CONT(gfv_local, 0.15) OVER(PARTITION BY entity_id, city_name, zone_name, scheme_id) AS gfv_ntile_15, 
    PERCENTILE_CONT(gfv_local, 0.2) OVER(PARTITION BY entity_id, city_name, zone_name, scheme_id) AS gfv_ntile_20, 
    PERCENTILE_CONT(gfv_local, 0.25) OVER(PARTITION BY entity_id, city_name, zone_name, scheme_id) AS gfv_ntile_25, 
    PERCENTILE_CONT(gfv_local, 0.3) OVER(PARTITION BY entity_id, city_name, zone_name, scheme_id) AS gfv_ntile_30, 
    PERCENTILE_CONT(gfv_local, 0.35) OVER(PARTITION BY entity_id, city_name, zone_name, scheme_id) AS gfv_ntile_35, 
    PERCENTILE_CONT(gfv_local, 0.4) OVER(PARTITION BY entity_id, city_name, zone_name, scheme_id) AS gfv_ntile_40, 
    PERCENTILE_CONT(gfv_local, 0.45) OVER(PARTITION BY entity_id, city_name, zone_name, scheme_id) AS gfv_ntile_45, 
    PERCENTILE_CONT(gfv_local, 0.5) OVER(PARTITION BY entity_id, city_name, zone_name, scheme_id) AS gfv_ntile_50, 
    PERCENTILE_CONT(gfv_local, 0.55) OVER(PARTITION BY entity_id, city_name, zone_name, scheme_id) AS gfv_ntile_55, 
    PERCENTILE_CONT(gfv_local, 0.6) OVER(PARTITION BY entity_id, city_name, zone_name, scheme_id) AS gfv_ntile_60, 
    PERCENTILE_CONT(gfv_local, 0.65) OVER(PARTITION BY entity_id, city_name, zone_name, scheme_id) AS gfv_ntile_65, 
    PERCENTILE_CONT(gfv_local, 0.7) OVER(PARTITION BY entity_id, city_name, zone_name, scheme_id) AS gfv_ntile_70, 
    PERCENTILE_CONT(gfv_local, 0.75) OVER(PARTITION BY entity_id, city_name, zone_name, scheme_id) AS gfv_ntile_75, 
    PERCENTILE_CONT(gfv_local, 0.8) OVER(PARTITION BY entity_id, city_name, zone_name, scheme_id) AS gfv_ntile_80, 
    PERCENTILE_CONT(gfv_local, 0.85) OVER(PARTITION BY entity_id, city_name, zone_name, scheme_id) AS gfv_ntile_85, 
    PERCENTILE_CONT(gfv_local, 0.9) OVER(PARTITION BY entity_id, city_name, zone_name, scheme_id) AS gfv_ntile_90, 
    PERCENTILE_CONT(gfv_local, 0.95) OVER(PARTITION BY entity_id, city_name, zone_name, scheme_id) AS gfv_ntile_95, 
    PERCENTILE_CONT(gfv_local, 1) OVER(PARTITION BY entity_id, city_name, zone_name, scheme_id) AS gfv_ntile_100
FROM `dh-logistics-product-ops.pricing.surge_mov_my_individual_orders`
WHERE TRUE 
    AND created_date >= DATE('2021-11-10')
    AND CONCAT(zone_name, '|', scheme_id) IN ('Georgetown|1392', 'Kulim|206')
    AND dps_customer_tag != 'New'
    AND is_delivery_fee_covered_by_discount = FALSE 
    AND is_delivery_fee_covered_by_voucher = FALSE
)
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5


