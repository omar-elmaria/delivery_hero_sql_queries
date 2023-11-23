-- Q1: Price schemes in the test
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.hk_service_fee_price_schemes` AS

-- Get the total OD restaurant vendors in HK
WITH vendors_city AS (
    SELECT DISTINCT 
        v.entity_id,
        v.vendor_code
    FROM `fulfillment-dwh-production.cl.vendors_v2` v
    LEFT JOIN UNNEST(vendor) ven
    LEFT JOIN UNNEST(dps) dps
    LEFT JOIN UNNEST(zones) z 
    WHERE TRUE 
        AND v.entity_id IN ('FP_HK') -- HERE
),

vendor_city_details AS (
    SELECT DISTINCT
        v.entity_id,
        v.vendor_code,
        delivery_type,
        v.vertical_type,
    FROM `fulfillment-dwh-production.cl.vendors_v2` v
    LEFT JOIN UNNEST(vendor) ven
    LEFT JOIN UNNEST(dps) dps
    LEFT JOIN UNNEST(zones) z
    CROSS JOIN UNNEST(delivery_provider) AS delivery_type
    WHERE TRUE
        AND entity_id = ('FP_HK')
        AND v.vendor_code IN (SELECT vendor_code FROM vendors_city)
        AND delivery_type = 'OWN_DELIVERY' 
        AND vertical_type = 'restaurants'
),

-- Get details on the vendors in the test
vendor_list AS (
    SELECT DISTINCT 
        v.entity_id,
        v.vendor_code
    FROM `fulfillment-dwh-production.cl.vendors_v2` v
    LEFT JOIN UNNEST(vendor) ven
    LEFT JOIN UNNEST(dps) dps
    LEFT JOIN UNNEST(zones) z 
    WHERE TRUE 
        AND dps.scheme_name LIKE '%Service Fee AA Test%'
        AND v.entity_id IN ('FP_HK') -- HERE
),

vendor_details_unnested AS (
    SELECT
        v.entity_id,
        v.vendor_code,
        delivery_type,
        dps.assignment_type,
        CASE WHEN dps.assignment_type = 'Manual' THEN 'A' WHEN dps.assignment_type = 'Automatic' THEN 'B' WHEN dps.assignment_type = 'Country Fallback' THEN 'C' END AS assignment_type_sorting_col,
        v.vertical_type,
        dps.scheme_id,
        dps.scheme_name,
        z.id AS zone_id,
        z.name AS zone_name
    FROM `fulfillment-dwh-production.cl.vendors_v2` v
    LEFT JOIN UNNEST(vendor) ven
    LEFT JOIN UNNEST(dps) dps
    LEFT JOIN UNNEST(zones) z
    CROSS JOIN UNNEST(delivery_provider) AS delivery_type -- "delivery_provider" is an array that sometimes contains multiple elements, so we need to unnest it and break it down to its individual components
    WHERE TRUE
        AND entity_id = ('FP_HK')
        AND v.vendor_code IN (SELECT vendor_code FROM vendor_list)
),

test_zones AS (
    SELECT DISTINCT
        entity_id,
        scheme_id,
        scheme_name,
        CASE WHEN scheme_id NOT IN (2078, 2079, 2080, 2081, 2082, 2083, 2084, 2085, 2086) THEN "Standard" ELSE SUBSTR(scheme_name, 30, 11) END AS Variation,
        COUNT(DISTINCT vendor_code) AS Num_vendors,
    FROM vendor_details_unnested 
    WHERE assignment_type = 'Manual'
    GROUP BY 1,2,3
)

SELECT 
    a.*, 
    b.*
FROM test_zones a
LEFT JOIN (SELECT COUNT(DISTINCT vendor_code) AS OD_Restaurant_Vendors_HK FROM vendor_city_details) b ON TRUE
ORDER BY 1,2;

------------------------------------------------------------------------------END OF PART 1------------------------------------------------------------------------------

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.hk_service_fee_zones_and_treatment_groups` AS

-- Q2: Which zones are part of which groups 

-- Get the total OD restaurant vendors in HK
WITH vendors_city AS (
    SELECT DISTINCT 
        v.entity_id,
        v.vendor_code
    FROM `fulfillment-dwh-production.cl.vendors_v2` v
    LEFT JOIN UNNEST(vendor) ven
    LEFT JOIN UNNEST(dps) dps
    LEFT JOIN UNNEST(zones) z 
    WHERE TRUE 
        AND v.entity_id IN ('FP_HK') -- HERE
),

vendor_city_details AS (
    SELECT DISTINCT
        v.entity_id,
        v.vendor_code,
        delivery_type,
        v.vertical_type,
    FROM `fulfillment-dwh-production.cl.vendors_v2` v
    LEFT JOIN UNNEST(vendor) ven
    LEFT JOIN UNNEST(dps) dps
    LEFT JOIN UNNEST(zones) z
    CROSS JOIN UNNEST(delivery_provider) AS delivery_type
    WHERE TRUE
        AND entity_id = ('FP_HK')
        AND v.vendor_code IN (SELECT vendor_code FROM vendors_city)
        AND delivery_type = 'OWN_DELIVERY' 
        AND vertical_type = 'restaurants'
),

-- Getting data on vendors in the test
vendor_list AS (
    SELECT DISTINCT 
        v.entity_id,
        v.vendor_code
    FROM `fulfillment-dwh-production.cl.vendors_v2` v
    LEFT JOIN UNNEST(vendor) ven
    LEFT JOIN UNNEST(dps) dps
    LEFT JOIN UNNEST(zones) z 
    WHERE TRUE 
        AND dps.scheme_name LIKE '%Service Fee AA Test%'
        AND v.entity_id IN ('FP_HK') -- HERE
),

vendor_details_unnested AS (
    SELECT
        v.entity_id,
        v.vendor_code,
        delivery_type,
        dps.assignment_type,
        CASE WHEN dps.assignment_type = 'Manual' THEN 'A' WHEN dps.assignment_type = 'Automatic' THEN 'B' WHEN dps.assignment_type = 'Country Fallback' THEN 'C' END AS assignment_type_sorting_col,
        v.vertical_type,
        dps.scheme_id,
        dps.scheme_name,
        z.id AS zone_id,
        z.name AS zone_name
    FROM `fulfillment-dwh-production.cl.vendors_v2` v
    LEFT JOIN UNNEST(vendor) ven
    LEFT JOIN UNNEST(dps) dps
    LEFT JOIN UNNEST(zones) z
    CROSS JOIN UNNEST(delivery_provider) AS delivery_type -- "delivery_provider" is an array that sometimes contains multiple elements, so we need to unnest it and break it down to its individual components
    WHERE TRUE
        AND entity_id = ('FP_HK')
        AND v.vendor_code IN (SELECT vendor_code FROM vendor_list)
),

test_zones AS (
    SELECT DISTINCT
        entity_id,
        scheme_id,
        scheme_name,
        zone_id,
        zone_name,
        COUNT(DISTINCT vendor_code) AS num_vendors_in_test,
        ARRAY_TO_STRING(ARRAY_AGG(vendor_code ORDER BY vendor_code), ', ') AS vendor_list_in_test
    FROM vendor_details_unnested 
    WHERE assignment_type = 'Manual' AND scheme_id IN (2078, 2079, 2080, 2081, 2082, 2083, 2084, 2085, 2086) -- All manual assignments / 9 scheme IDs for variations
    GROUP BY 1,2,3,4,5
),

city_data AS (
    SELECT 
        p.entity_id,
        country_code,
        ci.name AS city_name,
        ci.id AS city_id,
        zo.shape AS zone_shape, 
        zo.name AS zone_name,
        zo.id AS zone_id
    FROM fulfillment-dwh-production.cl.countries co
    LEFT JOIN UNNEST(co.platforms) p
    LEFT JOIN UNNEST(co.cities) ci
    LEFT JOIN UNNEST(ci.zones) zo
    WHERE TRUE 
        AND entity_id = 'FP_HK'
),

final_list_all_data AS (
    SELECT 
        a.entity_id,
        b.city_name,
        b.city_id,
        a.scheme_id,
        a.scheme_name,
        SUBSTR(scheme_name, 30, 11) AS Variation,
        a.zone_id,
        a.zone_name,
        a.num_vendors_in_test,
        c.total_OD_rest_vendors_in_HK,
        a.vendor_list_in_test
    FROM test_zones a
    LEFT JOIN city_data b ON a.entity_id = b.entity_id AND a.zone_id = b.zone_id
    LEFT JOIN (SELECT COUNT(DISTINCT vendor_code) AS total_OD_rest_vendors_in_HK FROM vendor_city_details) c ON TRUE
    ORDER BY 6, num_vendors_in_test
),

vendors_zones AS (
    SELECT
        v.entity_id,
        z.id AS zone_id,
        z.name AS zone_name, 
        COUNT(DISTINCT vendor_code) AS total_OD_rest_vendors_in_zone
    FROM `fulfillment-dwh-production.cl.vendors_v2` v
    LEFT JOIN UNNEST(vendor) ven
    LEFT JOIN UNNEST(dps) dps
    LEFT JOIN UNNEST(zones) z
    CROSS JOIN UNNEST(delivery_provider) AS delivery_type 
    WHERE TRUE 
        AND v.entity_id IN ('FP_HK') -- HERE
        AND z.id IN (SELECT DISTINCT zone_id FROM final_list_all_data)
        AND delivery_type = 'OWN_DELIVERY' 
        AND v.vertical_type = 'restaurants'
    GROUP BY 1,2,3
)

SELECT 
    a.Variation,
    a.scheme_id,
    a.scheme_name,
    a.zone_id,
    a.zone_name,
    a.num_vendors_in_test,
    b.total_OD_rest_vendors_in_zone,
    ROUND(a.num_vendors_in_test / b.total_OD_rest_vendors_in_zone, 4) AS test_vendors_share_of_zone,
    a.total_OD_rest_vendors_in_HK,
    ROUND(a.num_vendors_in_test / a.total_OD_rest_vendors_in_HK, 4) AS test_vendors_share_of_HK,
FROM final_list_all_data a
LEFT JOIN vendors_zones b USING(entity_id, zone_id)
ORDER BY 1,2,num_vendors_in_test;

------------------------------------------------------------------------------END OF PART 2------------------------------------------------------------------------------

-- Get the order history to visualize it by Kepler

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.hk_service_fee_orders_data` AS
-- Get details on the vendors in the test
WITH vendor_zone_to_include AS (
    SELECT
        ST_UNION_AGG(zo.shape) zone_area_include,
    FROM `fulfillment-dwh-production.cl.countries` co
    LEFT JOIN UNNEST(co.platforms) p
    LEFT JOIN UNNEST(co.cities) ci
    LEFT JOIN UNNEST(ci.zones) zo
    WHERE
        p.entity_id LIKE 'FP_HK'
        AND zo.id IN (9, 63, 30, 39, 8, 47, 16, 48, 12, 31, 43, 72, 71, 22, 5, 67, 41, 33) -- Test zones
        AND ci.is_active
        AND zo.is_active
),

customer_zone_to_exclude AS (
    SELECT
        ST_UNION_AGG(zo.shape) zone_area_exclude,
    FROM `fulfillment-dwh-production.cl.countries` co
    LEFT JOIN UNNEST(co.platforms) p
    LEFT JOIN UNNEST(co.cities) ci
    LEFT JOIN UNNEST(ci.zones) zo
    WHERE
        p.entity_id LIKE 'FP_HK'
        -- AND zo.id IN (1, 2, 3, 4, 6, 17, 20, 23, 24, 38, 44, 46, 52, 56, 58, 59, 62, 68, 70, 115) -- Neighboring zones to the test zones above
        AND zo.id NOT IN (9, 63, 30, 39, 8, 47, 16, 48, 12, 31, 43, 72, 71, 22, 5, 67, 41, 33) -- All zones that are NOT in the test
        AND ci.is_active
        AND zo.is_active
),

vendor_list AS ( -- All vendors participating in the test
    SELECT DISTINCT 
        v.entity_id,
        v.vendor_code
    FROM `fulfillment-dwh-production.cl.vendors_v2` v
    LEFT JOIN UNNEST(vendor) ven
    LEFT JOIN UNNEST(dps) dps
    LEFT JOIN UNNEST(zones) z 
    WHERE TRUE 
        AND dps.scheme_name LIKE '%Service Fee AA Test%'
        AND v.entity_id IN ('FP_HK') -- HERE
),

vendors_to_include AS ( -- To be more conservative
    SELECT DISTINCT 
        v.global_entity_id,
        v.vendor_id,
        v.is_key_account,
        v.is_online,
        v.location.area AS zone_name,
        v.location.area_id AS zone_id
    FROM `fulfillment-dwh-production.curated_data_shared_central_dwh.vendors` v
    CROSS JOIN vendor_zone_to_include
    WHERE TRUE
        AND v.global_entity_id = 'FP_HK'
        AND v.store_type_l2 = 'restaurants'
        AND v.is_own_delivery
        AND ST_CONTAINS(zone_area_include, ST_GEOGPOINT(v.location.longitude,v.location.latitude))
        AND v.vendor_id IN (SELECT vendor_code FROM vendor_list)
),

city_data AS (
    SELECT 
        p.entity_id,
        country_code,
        ci.name AS city_name,
        ci.id AS city_id,
        zo.shape AS zone_shape, 
        zo.name AS zone_name,
        zo.id AS zone_id
    FROM fulfillment-dwh-production.cl.countries co
    LEFT JOIN UNNEST(co.platforms) p
    LEFT JOIN UNNEST(co.cities) ci
    LEFT JOIN UNNEST(ci.zones) zo
    WHERE TRUE 
        AND entity_id = 'FP_HK'
)

--List of valid orders FROM selected vendors NOT delivered to selected customers
SELECT
    o.global_entity_id AS entity_id,
    o.customer_account_id,
    DATE(o.placed_at) AS created_date, -- Order date
    o.placed_at AS order_placed_at, -- Order time stamp
    o.order_id,
    o.vendor_id,
    ARRAY_TO_STRING(ARRAY_AGG(cit.zone_name ORDER BY cit.zone_name), ', ') AS zone_name,
    ARRAY_TO_STRING(ARRAY_AGG(CAST(cit.zone_id AS STRING) ORDER BY cit.zone_name), ', ') AS zone_id,
    o.delivery_location.longitude,
    o.delivery_location.latitude,
    ST_GEOGPOINT(o.delivery_location.longitude, o.delivery_location.latitude) AS delivery_coordinates,
FROM `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` o
INNER JOIN vendors_to_include vin USING (global_entity_id, vendor_id)
CROSS JOIN customer_zone_to_exclude exc
LEFT JOIN city_data cit ON cit.entity_id = o.global_entity_id AND ST_CONTAINS(cit.zone_shape, ST_GEOGPOINT(delivery_location.longitude,delivery_location.latitude))
WHERE TRUE
    AND o.global_entity_id = 'FP_HK'
    AND DATE(o.placed_at_local) >= '2021-10-04'
    AND o.is_sent
    AND NOT ST_CONTAINS(exc.zone_area_exclude, ST_GEOGPOINT(delivery_location.longitude,delivery_location.latitude))
GROUP BY 1,2,3,4,5,6,9,10
ORDER BY 3,7
