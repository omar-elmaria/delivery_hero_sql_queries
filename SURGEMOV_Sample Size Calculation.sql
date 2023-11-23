-- The parts that contain the word "HERE" are the ones that change from one test to the next
-- Method 1: Using Sessions (here we use CVR1 as the success metric of the test)

DECLARE entity_var, city_name_var, v_type, od_status STRING;
DECLARE zone_name_var ARRAY <STRING>;
DECLARE price_scheme_var ARRAY <INT64>;
DECLARE start_date, end_date DATE;
SET (entity_var, city_name_var, v_type, od_status) = ('FP_HK', 'Hong kong', 'restaurants', 'OWN_DELIVERY');
SET zone_name_var = ['Central rider', 'Central walker']; -- Chosen zones (AB test part only)
-- SET zone_name_var = ['Central rider', 'Central walker', 'Tin shui wai rider', 'Tin shui wai walker']; -- Chosen zones
SET price_scheme_var = [2043, 2044]; -- Prevailing price schemes in the zones of interest
SET (start_date, end_date) = (DATE('2022-01-22'), DATE('2022-02-20'));
-- SET (start_date, end_date) = (DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY), CURRENT_DATE());

-------------------------------------------------------------END OF INPUT - RUN CODE DIRECTLY-------------------------------------------------------------

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.surge_mov_hk_sample_size_calculation_with_sessions` AS
WITH vendor_list AS (
    SELECT DISTINCT 
        entity_id,
        vendor_code
    FROM `fulfillment-dwh-production.cl.vendors_v2` v
    LEFT JOIN UNNEST(vendor) ven
    LEFT JOIN UNNEST(dps) dps
    LEFT JOIN UNNEST(zones) z
    CROSS JOIN UNNEST(delivery_provider) AS delivery_type -- "delivery_provider" is an array that sometimes contains multiple elements, so we need to unnest it and break it down to its individual components
    WHERE TRUE
        AND v.entity_id = entity_var -- HERE
        AND v.is_active -- Filter for active vendors only
        AND dps.assignment_type IS NOT NULL -- Eliminate records where the assignment type is not stored
        AND dps.assignment_type != 'Country Fallback' -- Eliminate records where the assignment type = 'Country Fallback'
        AND dps.scheme_id IN UNNEST(price_scheme_var) -- HERE. Uncomment if you want to filter for a specific scheme ID
        AND v.vertical_type = v_type -- HERE. Restaurants
        AND delivery_type = od_status -- HERE. OWN_DELIVERY
        AND z.name IN UNNEST(zone_name_var) -- HERE. Zones containing the vendors
),

cvr_events AS (
    SELECT 
        e.country_code,
        e.dps_city_name AS city_name,
        e.created_date
        , COUNT (DISTINCT e.ga_session_id) AS total_sessions
        , COUNT (DISTINCT e.shop_list_no) AS shop_list_sessions
        , COUNT (DISTINCT e.shop_menu_no) AS shop_menu_sessions
        , COUNT (DISTINCT e.checkout_no) AS checkout_sessions
        , COUNT (DISTINCT e.checkout_transaction) AS checkout_transaction_sessions
        , COUNT (DISTINCT e.transaction_no) AS transactions
        , COUNT (DISTINCT e.perseus_client_id) AS users
    FROM `fulfillment-dwh-production.cl.dps_cvr_events` e
    CROSS JOIN UNNEST(e.vendor_code) ven
    WHERE TRUE 
        AND e.entity_id = entity_var -- HERE
        AND e.created_date BETWEEN start_date AND end_date -- HERE
        AND (e.dps_city_name LIKE CONCAT('%', city_name_var, '%')) -- HERE
        AND (e.dps_zones_name LIKE '%Central rider%' OR e.dps_zones_name LIKE '%Central walker%') -- HERE
        AND ven IN (SELECT DISTINCT vendor_code FROM vendor_list) -- HERE: Treatment scope vendors (replace with your own table)
    GROUP BY 1,2,3
)

SELECT 
  city_name,
  SUM (users) AS users
  , SUM (total_sessions) AS total_sessions
  , SUM (shop_list_sessions) AS shop_list_sessions
  , SUM (shop_menu_sessions) AS shop_menu_sessions
  , SUM (checkout_sessions) AS checkout_sessions
  , SUM (checkout_transaction_sessions) AS checkout_transaction_sessions
  , SUM (transactions) AS transactions
  , ROUND(SUM (transactions) / SUM (total_sessions), 4) AS CVR1
  , ROUND(SUM (transactions) / SUM (shop_menu_sessions ), 4) AS CVR3
  , ROUND(SUM (checkout_transaction_sessions) / SUM (checkout_sessions), 4) AS mCVR4_prime
FROM cvr_events e
GROUP BY 1;

-- There are two ways of getting the CVR data on the level of your treatment scope 
-- Method 1: Get the sessions on the city level as a whole (i.e. across all verticals and delivery types, then we multiply by the **order share** of our treatment scope assuming that CVR is a constant multiplication factor)
-- Method 2 (the method we follow here because it is more rigorous): Filter for the sessions coming from the vendors under your target group AND potentially target zones as well. 
-- You need the target zones filter in case you are not planning on targeting customers in all zones

----------------------------------------------------------------------END OF METHOD 1----------------------------------------------------------------------

-- The parts that contain the word "HERE" are the ones that change from one test to the next
-- Method 2: Using Orders (here we use GPO "gross profit per order" as the main metric)
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.surge_mov_hk_sample_size_calculation_with_orders` AS
WITH city_data AS (
    SELECT 
        p.entity_id
        , cl. country_code
        , ci.id as city_id
        , ci.name as city_name
        , z.shape AS zone_shape
        , z.id as zone_id
        , z.name AS zone_name
    FROM `fulfillment-dwh-production.cl.countries` cl
    LEFT JOIN UNNEST (platforms) p
    LEFT JOIN UNNEST (cities) ci
    LEFT JOIN UNNEST (zones) z
    WHERE TRUE
        AND p.entity_id = entity_var -- HERE
        AND ci.name = city_name_var -- HERE
        AND z.name IN UNNEST(zone_name_var) -- HERE
        AND ci.is_active -- Active city
        AND z.is_active -- Active zone
),

vendor_list AS (
    SELECT DISTINCT 
        entity_id,
        vendor_code
    FROM `fulfillment-dwh-production.cl.vendors_v2` v
    LEFT JOIN UNNEST(vendor) ven
    LEFT JOIN UNNEST(dps) dps
    LEFT JOIN UNNEST(zones) z
    CROSS JOIN UNNEST(delivery_provider) AS delivery_type -- "delivery_provider" is an array that sometimes contains multiple elements, so we need to unnest it and break it down to its individual components
    WHERE TRUE
        AND v.entity_id = entity_var -- HERE
        AND v.is_active -- Filter for active vendors only
        AND dps.assignment_type IS NOT NULL -- Eliminate records where the assignment type is not stored
        AND dps.assignment_type != 'Country Fallback' -- Eliminate records where the assignment type = 'Country Fallback'
        AND dps.scheme_id IN UNNEST(price_scheme_var) -- HERE. Uncomment if you want to filter for a specific scheme ID
        AND v.vertical_type = v_type -- HERE. Restaurants
        AND delivery_type = od_status -- HERE. OWN_DELIVERY
        AND z.name IN UNNEST(zone_name_var) -- HERE. Zones containing the vendors
),

delivery_costs AS (
    SELECT
        p.entity_id,
        p.order_id, 
        o.platform_order_code,
        SUM(p.delivery_costs) AS delivery_costs_local,
        SUM(p.delivery_costs_eur) AS delivery_costs_eur
    FROM `fulfillment-dwh-production.cl.utr_timings` p
    LEFT JOIN `fulfillment-dwh-production.cl.orders` o ON p.entity_id = o.entity.id AND p.order_id = o.order_id -- Use the platform_order_code in this table as a bridge to join the order_id from utr_timings to order_id from central_dwh.orders 
    WHERE 1=1
        AND p.created_date BETWEEN start_date AND end_date -- HERE (For partitioning elimination and speeding up the query)
        AND o.created_date BETWEEN start_date AND end_date -- HERE (For partitioning elimination and speeding up the query)
    GROUP BY 1,2,3
    ORDER BY 1,2
)

SELECT
    dwh.global_entity_id AS entity_id,
    DATE(dwh.placed_at) AS created_date,
    dwh.order_id,
    dwh.value.gbv_local,
    COALESCE(
        pd.delivery_fee_local, 
        IF(o.is_delivery_fee_covered_by_discount = TRUE OR o.is_delivery_fee_covered_by_voucher = TRUE, 0, o.dps_delivery_fee_local)
    ) + o.commission_local + o.joker_vendor_fee_local + COALESCE(o.service_fee, 0) + COALESCE(dwh.value.mov_customer_fee_local, IF(o.gfv_local < o.dps_minimum_order_value_local, (o.dps_minimum_order_value_local - o.gfv_local), 0)) - cst.delivery_costs_local AS gross_profit_local,
FROM `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` dwh -- We use this table so that we can join on city_data
INNER JOIN city_data cd ON dwh.global_entity_id = cd.entity_id AND ST_CONTAINS(cd.zone_shape, ST_GEOGPOINT(delivery_location.longitude,delivery_location.latitude)) -- Filter for delivery locations in the city of choice
INNER JOIN vendor_list vn ON dwh.global_entity_id = vn.entity_id AND dwh.vendor_id = vn.vendor_code -- HERE: Treatment scope vendors (replace with your own table)
INNER JOIN `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` o ON dwh.global_entity_id = o.entity_id AND dwh.order_id = o.ga_platform_order_code -- We need the entries in this dps_sessions_mapped_to_orders table, but it does not match 1-1 with the cDWH, so we INNER JOIN on dps_sessions_mapped_to_orders
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` pd ON dwh.global_entity_id = pd.global_entity_id AND dwh.order_id = pd.code AND DATE(dwh.placed_at) = pd.created_date_utc -- Contains info on the orders in Pandora countries
LEFT JOIN delivery_costs cst ON o.entity_id = cst.entity_id AND o.order_id = cst.order_id -- The table that stores the cost per order
WHERE TRUE
    AND dwh.global_entity_id = entity_var -- HERE
    AND DATE(dwh.placed_at) BETWEEN start_date AND end_date -- HERE
    AND pd.created_date_utc BETWEEN start_date AND end_date -- HERE
    AND dwh.is_sent; -- Successful order

-- Calculations

SELECT STDDEV_SAMP(gross_profit_local) FROM `dh-logistics-product-ops.pricing.surge_mov_hk_sample_size_calculation_with_orders`; -- To calculate std dev of GPO (used as input to the inference for means calculator)

SELECT
    entity_id,
    created_date,
    COUNT(DISTINCT order_id) AS Num_orders,
    SUM(gross_profit_local) AS total_profit_per_day,
    SUM(gross_profit_local) / COUNT(DISTINCT order_id) AS GPO_per_day
FROM `dh-logistics-product-ops.pricing.surge_mov_hk_sample_size_calculation_with_orders`
GROUP BY 1,2
ORDER BY 1,2 -- To calculate GPO per day