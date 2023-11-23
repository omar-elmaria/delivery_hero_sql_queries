-- The parts that contain the word "HERE" are the ones that change from one test to the next
-- Method 1: Using Sessions (here we use CVR1 as the success metric of the test)

DECLARE entity, city_name_var STRING;
DECLARE zn_id ARRAY <INT64>;
DECLARE start_date, end_date DATE;
SET (entity, city_name_var) = ('FP_MY', 'Klang valley');
SET zn_id = [1];
SET (start_date, end_date) = (DATE('2021-12-01'), DATE('2022-01-31'));

-------------------------------------------------------------END OF INPUT - RUN CODE DIRECTLY-------------------------------------------------------------

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.sample_size_calculation_with_sessions` AS
WITH cvr_events_klv AS (
    SELECT 
        e.country_code,
        city_name_var AS city_name,
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
        AND e.entity_id = entity -- HERE
        AND e.created_date BETWEEN start_date AND end_date -- HERE
        AND (e.dps_city_name LIKE CONCAT('%', city_name_var, '%')) -- HERE
        AND ven IN (SELECT DISTINCT vendor_code FROM `dh-logistics-product-ops.pricing.loved_brands_apac_final_vendor_list_all_data_klang_valley`) -- HERE: Treatment scope vendors (replace with your own table)
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
FROM cvr_events_klv e
GROUP BY 1; 

-- There are two ways of getting the CVR data on the level of your treatment scope 
-- Method 1: Get the sessions on the city level as a whole (i.e. across all verticals and delivery types, then we multiply by the **order share** of our treatment scope assuming that CVR is a constant multiplication factor)
-- Method 2 (the method we follow here because it is more rigorous): Filter for the sessions coming from the vendors under your target group as shown in the cvr_events_klv query

----------------------------------------------------------------------END OF METHOD 1----------------------------------------------------------------------

-- The parts that contain the word "HERE" are the ones that change from one test to the next
-- Method 2: Using Orders (here we use GPO "gross profit per order" as the main metric)
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.sample_size_calculation_with_orders` AS
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
        AND ci.name = city_name_var -- HERE
        -- AND z.id IN UNNEST(zn_id) -- HERE
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
),

orders_raw_data AS (
    SELECT
        o.global_entity_id AS entity_id,
        DATE(o.placed_at) AS created_date,
        o.order_id,
        CASE -- If an order had a basket value below MOV (i.e. small order fee was charged), add the small order fee calculated as MOV - GFV to the profit 
            WHEN (dps.is_delivery_fee_covered_by_voucher = FALSE AND  dps.is_delivery_fee_covered_by_discount = FALSE) THEN ((dps.delivery_fee_local - dps.delivery_fee_vat_local) + dps.commission_local + joker_vendor_fee_local + COALESCE(dps.service_fee, 0) + o.value.mov_customer_fee_local - cst.delivery_costs_local)
            ELSE (dps.commission_local + joker_vendor_fee_local + COALESCE(dps.service_fee, 0) + o.value.mov_customer_fee_local - cst.delivery_costs_local)
        END AS gross_profit_local,
    FROM `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` o -- We use this table so that we can join on city_data
    INNER JOIN `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` dps ON o.global_entity_id = dps.entity_id AND o.order_id = dps.ga_platform_order_code -- We need the entries in this dps_sessions_mapped_to_orders table, but it does not match 1-1 with the cDWH, so we INNER JOIN on dps_sessions_mapped_to_orders
    INNER JOIN city_data cd ON o.global_entity_id = cd.entity_id AND ST_CONTAINS(cd.zone_shape, ST_GEOGPOINT(delivery_location.longitude,delivery_location.latitude)) -- Filter for delivery locations in the city of choice
    INNER JOIN `dh-logistics-product-ops.pricing.loved_brands_apac_final_vendor_list_all_data_klang_valley` lb ON o.global_entity_id = lb.entity_id AND o.vendor_id = lb.vendor_code -- HERE: Treatment scope vendors (replace with your own table)
    LEFT JOIN delivery_costs cst ON dps.entity_id = cst.entity_id AND dps.order_id = cst.order_id -- The table that stores the cost per order
    WHERE TRUE
        AND o.global_entity_id = entity -- HERE
        AND DATE(placed_at) BETWEEN start_date AND end_date -- HERE
        AND is_sent -- Successful order
)

-- SELECT STDDEV_SAMP(gross_profit_local) FROM orders_raw_data -- To calculate std dev of GPO (used as input to the inference for means calculator)

SELECT
    entity_id,
    created_date,
    COUNT(DISTINCT order_id) AS Num_orders,
    SUM(gross_profit_local) AS total_profit_per_day,
    SUM(gross_profit_local) / COUNT(DISTINCT order_id) AS GPO_per_day
FROM orders_raw_data
GROUP BY 1,2
ORDER BY 1,2 -- To calculate GPO per day