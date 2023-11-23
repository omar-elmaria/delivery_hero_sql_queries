DECLARE entity_var, city_name_var, d_type STRING;
DECLARE vertical_var ARRAY <STRING>;
DECLARE start_date, end_date DATE;
DECLARE start_date_tt_adj, end_date_tt_adj DATE;
DECLARE sessions_ntile_thr, orders_ntile_thr, cvr3_ntile_thr FLOAT64;
DECLARE vertical_zone_var STRING;
DECLARE scheme_id_var INT64;
SET (entity_var, city_name_var, d_type) = ('FP_SG', 'Singapore', 'OWN_DELIVERY');
SET vertical_var = ['groceries'];
SET (start_date, end_date) = (DATE_SUB(CURRENT_DATE(), INTERVAL 32 DAY), CURRENT_DATE());
SET (sessions_ntile_thr, orders_ntile_thr, cvr3_ntile_thr) = (0.75, 0.75, 0.75);
SET vertical_zone_var = 'Groceries Low PS';
SET scheme_id_var = 1673;

##------------------------------------------------------------------------END OF THE INPUT SECTION------------------------------------------------------------------------##

-- Query 1: Get data on the different pricing schemes used in a particular city and pick the dominant one
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.loved_brands_apac_dominant_price_scheme_sg_groceries_low_ps_cvr3_calc` AS
SELECT 
    *,
    ROUND(SUM(Vendor_Share) OVER (ORDER BY Vendor_Count DESC), 4) AS Vendor_Share_Cumulative,
    ROUND(SUM(Order_Share) OVER (ORDER BY Order_Count DESC), 4) AS Order_Share_Cumulative,
FROM (
    SELECT
        a.entity_id,
        a.city_name,
        a.city_id,
        a.scheme_id,
        sch.scheme_name,
        COUNT(DISTINCT vendor_id) AS Vendor_Count,  
        ROUND(COUNT(DISTINCT vendor_id) / SUM(COUNT(DISTINCT vendor_id)) OVER (), 4) AS Vendor_Share,
        SUM(COUNT(DISTINCT vendor_id)) OVER () AS Total_Vendors,
        COUNT(DISTINCT order_id) AS Order_Count,
        ROUND(COUNT(DISTINCT order_id) / SUM(COUNT(DISTINCT order_id)) OVER (), 4) AS Order_Share,
        SUM(COUNT(DISTINCT order_id)) OVER () AS Total_Orders
    FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` a
    LEFT JOIN (
        SELECT DISTINCT 
            entity_id, 
            dps.scheme_name, 
            dps.scheme_id 
        FROM `fulfillment-dwh-production.cl.vendors_v2` 
        LEFT JOIN UNNEST(dps) dps
    ) sch ON a.entity_id = sch.entity_id AND a.scheme_id = sch.scheme_id
    WHERE TRUE 
        AND a.created_date BETWEEN start_date AND end_date -- 30 days of data
        AND a.entity_id = entity_var -- Change the entity according to your use case
        AND a.city_name = city_name_var -- Change the city according to your use case
        AND a.is_own_delivery -- OD or MP
        AND a.delivery_status = 'completed'
        AND a.vertical_type IN UNNEST(vertical_var)
    GROUP BY 1,2,3,4,5
)
ORDER BY Order_Share DESC;

##------------------------------------------------------------------------END OF THE DOMINANT SCHEME PART------------------------------------------------------------------------##

--  Query 2: A query to select the location details of zones within a city. This table can be joined to "dps_sessions_mapped_to_ga_sessions" and the "zone_shape" geo field can be used to filter for sessions in the zones that are of interest to us
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.loved_brands_apac_city_data_sg_groceries_low_ps_cvr3_calc` AS
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
    AND entity_id = entity_var -- Entity ID
    AND ci.name = city_name_var -- City Name
    AND zo.is_active -- Active city
    AND ci.is_active; -- Active zone

##------------------------------------------------------------------------END OF THE CITY DATA PART------------------------------------------------------------------------##

-- Query 3: Get the orders, CVR3 and sessions per vendor for all vendors in a particular city under the scheme chosen above
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.loved_brands_apac_all_vendors_sg_groceries_low_ps_cvr3_calc` AS
WITH vendors AS ( -- A query to select all vendors within a city that are under a specific price scheme
    SELECT DISTINCT
        v.entity_id,
        city_name_var AS city_name,
        v.vendor_code
    FROM `fulfillment-dwh-production.cl.vendors_v2` v
    LEFT JOIN UNNEST(v.vendor) ven
    LEFT JOIN UNNEST(v.dps) dps
    LEFT JOIN UNNEST(v.hurrier) hur
    LEFT JOIN UNNEST(v.zones) z
    CROSS JOIN UNNEST(delivery_provider) AS delivery_type -- "delivery_provider" is an array that sometimes contains multiple elements, so we need to unnest it and break it down to its individual components
    INNER JOIN `dh-logistics-product-ops.pricing.loved_brands_apac_city_data_sg_groceries_low_ps_cvr3_calc` cd ON v.entity_id = cd.entity_id AND ST_CONTAINS(cd.zone_shape, v.location) -- This is an alternative to using dps.name/dps.id in the WHERE clause. Here, we filter for sessions in the chosen city
    WHERE TRUE 
        AND v.entity_id = entity_var -- Entity ID
        AND v.is_active -- Active vendors
        AND v.vertical_type IN UNNEST(vertical_var) -- Restaurants vertical only
        AND delivery_type = d_type -- Filter for OD vendors
        AND dps.scheme_id = scheme_id_var -- Scheme ID with the highest order share in the city
),

sessions AS ( -- Get session data for **all** sessions in the chosen city over the specified timeframe. We don't need to specify the expedition type (OD vs. pickup) or the vertical (restaurants, darkstores, pharmacies, etc.) because we already have a defined list of vendors that we will JOIN to below
    SELECT DISTINCT
        x.created_date, -- Date of the ga session
        x.entity_id, -- Entity ID
        x.platform, -- Operating system (iOS, Android, Web, etc.)
        x.brand, -- Talabat, foodpanda, Foodora, etc.
        x.events_ga_session_id, -- GA session ID
        x.fullvisitor_id, -- The visit_id defined by Google Analytics
        x.visit_id, -- 	The visit_id defined by Google Analytics
        x.has_transaction, -- A field that indicates whether or not a session ended in a transaction
        x.total_transactions, -- The total number of transactions in the GA session
        x.ga_dps_session_id, -- DPS session ID
        
        x.sessions.dps_session_timestamp, -- The timestamp of the DPS log.
        x.sessions.endpoint, -- The endpoint from where the DPS request is coming, including MultipleFee, which could come from Listing Page or others. and SingleFee, which could come from Menu page or others
        x.sessions.perseus_client_id, -- A unique customer identifier based on the device
        x.sessions.variant, -- AB variant (e.g. Control, Variation1, Variation2, etc.)
        x.sessions.customer_status, -- The customer.tag in DPS log, indicating whether the customer is new or not
        x.sessions.location, -- The customer.location in DPS log
        x.sessions.variant_concat, -- The concatenation of all the existing variants in the DPS log for the dps session id. There might be multiple variants due to location changes or session timeout
        x.sessions.location_concat, -- The concatenation of all the existing locations in the DPS log for the dps session id
        x.sessions.customer_status_concat, -- 	The concatenation of all the existing customer.tag in the DPS log for the dps session id

        e.event_action, -- Can have five values --> home_screen.loaded, shop_list.loaded, shop_details.loaded, checkout.loaded, transaction
        e.vendor_code, -- Vendor ID
        e.event_time, -- The timestamp of the event's creation.
        e.transaction_id, -- The transaction id for the GA session if the session has a transaction (i.e. order code)
        e.expedition_type, -- The delivery type of the session, pickup or delivery

        dps.id, -- Zone ID based on the DPS session
        dps.name, -- Zone name based on the DPS session
        dps.timezone, -- Time zone of the city based on the DPS session

        ST_ASTEXT(x.ga_location) AS ga_location -- GA location expressed as a STRING
    FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_ga_sessions` x
    LEFT JOIN UNNEST(events) e
    LEFT JOIN UNNEST(dps_zone) dps
    INNER JOIN `dh-logistics-product-ops.pricing.loved_brands_apac_city_data_sg_groceries_low_ps_cvr3_calc` cd ON x.entity_id = cd.entity_id AND ST_CONTAINS(cd.zone_shape, x.ga_location) -- This is an alternative to using dps.name/dps.id in the WHERE clause. Here, we filter for sessions in the chosen city
    WHERE TRUE
        AND x.entity_id = entity_var -- Filter for the entity of interest
        AND x.created_date BETWEEN start_date AND end_date -- Sessions' start and end date
        AND e.event_action IN ('shop_details.loaded', 'transaction') -- transaction / shop_details.loaded = CVR3
),

orders AS ( -- Get the number of orders of **all** vendors in the chosen city over the specified timeframe. We don't need to specify the expedition type (OD vs. pickup) or the vertical (restaurants, darkstores, pharmacies, etc.) because we already have a defined list of vendors that we will JOIN to below
    SELECT
        global_entity_id,
        vendor_id,
        COUNT(DISTINCT order_id) AS Num_orders
    FROM `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` o
    INNER JOIN `dh-logistics-product-ops.pricing.loved_brands_apac_city_data_sg_groceries_low_ps_cvr3_calc` cd ON o.global_entity_id = cd.entity_id AND ST_CONTAINS(cd.zone_shape, ST_GEOGPOINT(delivery_location.longitude, delivery_location.latitude)) -- Filter for delivery locations in the city of choice
    WHERE TRUE
        AND global_entity_id = entity_var
        AND DATE(placed_at) BETWEEN start_date AND end_date
        AND is_sent -- Successful order
    GROUP BY 1,2
),

all_metrics AS ( 
    SELECT 
        v.entity_id,
        v.city_name,
        v.vendor_code,
        COALESCE(o.Num_orders, 0) AS Num_orders,
        COALESCE(COUNT(DISTINCT CASE WHEN event_action = 'shop_details.loaded' THEN events_ga_session_id ELSE NULL END), 0) AS Num_Unique_Vendor_Visits, -- If a vendor was visited more than once in the same session, this is considered one visit
        COALESCE(COUNT(DISTINCT CASE WHEN event_action = 'shop_details.loaded' THEN event_time ELSE NULL END), 0) AS Num_Total_Vendor_Impressions, -- If a vendor was visited more than once in the same session, all impressions are counted
        COALESCE(COUNT(DISTINCT CASE WHEN event_action = 'transaction' THEN event_time ELSE NULL END), 0) AS Num_Transactions,
        COALESCE(ROUND(COUNT(DISTINCT CASE WHEN event_action = 'transaction' THEN event_time ELSE NULL END) / NULLIF(COUNT(DISTINCT CASE WHEN event_action = 'shop_details.loaded' THEN events_ga_session_id ELSE NULL END), 0), 3), 0) AS CVR3,
    FROM vendors v
    LEFT JOIN sessions s ON v.entity_id = s.entity_id  AND v.vendor_code = s.vendor_code -- LEFT JOIN because we assume that vendors_v2 contains ALL vendors with and without sessions, so any vendors without sessions will get a "zero"
    LEFT JOIN orders o ON v.entity_id = o.global_entity_id AND v.vendor_code = o.vendor_id -- LEFT JOIN for the same reason in the statement above
    GROUP BY 1,2,3,4
),

pct_ranks AS (
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY entity_id, city_name ORDER BY Num_orders), 4) AS Orders_pct_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY entity_id, city_name ORDER BY Num_Unique_Vendor_Visits), 4) AS Unique_visits_pct_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY entity_id, city_name ORDER BY CVR3), 4) AS CVR3_pct_rank
    FROM all_metrics
)

SELECT * FROM pct_ranks;

-- Filtering for vendors based on percentile ranks
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.loved_brands_apac_all_vendors_after_session_order_cvr_filters_sg_groceries_low_ps_cvr3_calc` AS
SELECT * FROM `dh-logistics-product-ops.pricing.loved_brands_apac_all_vendors_sg_groceries_low_ps_cvr3_calc`
WHERE Unique_visits_pct_rank >= sessions_ntile_thr AND Orders_pct_rank >= orders_ntile_thr AND CVR3_pct_rank >= cvr3_ntile_thr
ORDER BY 7 DESC;

#############################################################################################################################################################################################################
#####------------------------------------------------------------------------END OF THE ORDERS, SESSIONS, CVR3 FILTERING PROCESS------------------------------------------------------------------------#####
#############################################################################################################################################################################################################

-- Query 4: Get data about the DF tiers under the dominant price scheme in the city
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.loved_brands_apac_df_tiers_sg_groceries_low_ps_cvr3_calc` AS
SELECT 
    *,
    RANK() OVER (PARTITION BY scheme_id, component_id ORDER BY threshold) AS tier
FROM (
    SELECT DISTINCT 
        v.entity_id,
        d.scheme_id,
        d.scheme_name,
        pc.travel_time_config.id AS component_id,
        pc.travel_time_config.name AS component_name,
        CASE WHEN pc.travel_time_config.threshold IS NULL THEN 9999999 ELSE pc.travel_time_config.threshold END AS threshold,
        CASE 
            WHEN pc.travel_time_config.threshold IS NULL THEN 9999999 
            ELSE ROUND(FLOOR(pc.travel_time_config.threshold) + (pc.travel_time_config.threshold - FLOOR(pc.travel_time_config.threshold)) * 60/100, 2) 
        END AS threshold_in_min_and_sec,
        pc.travel_time_config.fee
    FROM `fulfillment-dwh-production.cl.vendors_v2` v
    LEFT JOIN UNNEST(zones) z
    LEFT JOIN UNNEST(dps) d
    LEFT JOIN UNNEST(d.vendor_config) vc
    LEFT JOIN UNNEST(vc.pricing_config) pc
    INNER JOIN `dh-logistics-product-ops.pricing.loved_brands_apac_city_data_sg_groceries_low_ps_cvr3_calc` cd ON v.entity_id = cd.entity_id AND ST_CONTAINS(cd.zone_shape, v.location)
    WHERE TRUE 
        AND v.entity_id = entity_var
        AND cd.city_name = city_name_var
        AND d.is_active
        AND d.scheme_id = scheme_id_var -- Scheme ID with the highest order share in the city
)
ORDER BY 1,2,4;

##------------------------------------------------------------------------END OF THE DF TIER DATA EXTRACTION PART------------------------------------------------------------------------##

-- Get data about the DF from the DPS logs
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.loved_brands_apac_dps_logs_sg_groceries_low_ps_cvr3_calc` AS
WITH dps_logs_stg_1 AS ( -- Will be used to get the CVR per DF
    SELECT DISTINCT
        logs.entity_id,
        logs.created_date,
        endpoint,
        customer.user_id AS perseus_id,
        customer.session.id AS dps_session_id,
        v.id AS vendor_code,
        v.delivery_fee.total AS DF_total,
        v.delivery_fee.travel_time AS DF_tt,
        customer.session.timestamp AS session_timestamp,
        logs.created_at
FROM `fulfillment-dwh-production.cl.dynamic_pricing_user_sessions` logs
LEFT JOIN UNNEST(vendors) v
INNER JOIN `dh-logistics-product-ops.pricing.loved_brands_apac_city_data_sg_groceries_low_ps_cvr3_calc` cd ON logs.entity_id = cd.entity_id AND ST_CONTAINS(cd.zone_shape, logs.customer.location) -- Filter for sessions in the city specified above
WHERE TRUE 
    AND logs.entity_id = entity_var
    AND logs.created_date BETWEEN start_date AND end_date
    AND logs.customer.session.id IS NOT NULL -- We must have the dps session ID to be able to obtain the session's DF in the next query
    AND endpoint IN ('singleFee') -- Caters to the all events --> "shop_list.loaded", "shop_details.loaded", "checkout.loaded" and "transaction"
    AND v.id IN (SELECT vendor_code FROM `dh-logistics-product-ops.pricing.loved_brands_apac_all_vendors_after_session_order_cvr_filters_sg_groceries_low_ps_cvr3_calc`) -- Filter for relevant DPS sessions ONLY (i.e., those that belong to vendor IDs that were selected in the first code section)
),

dps_logs_stg_2 AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY dps_session_id, endpoint, vendor_code ORDER BY created_at DESC) AS row_num_dps_logs -- Create a row counter to take the last delivery fee seen in the session. We assume that this is the one that the customer took their decision to purchase/not purchase on
    FROM dps_logs_stg_1
),

dps_logs AS(
    SELECT *
    FROM dps_logs_stg_2 
    WHERE row_num_dps_logs = 1 -- Take the last DF seen by the customer during the session
)

SELECT * FROM dps_logs
ORDER BY dps_session_id, vendor_code, created_at;

##-------------------------------------------------------------------------END OF THE DPS LOGS PART-------------------------------------------------------------------------##

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.loved_brands_apac_ga_dps_sessions_sg_groceries_low_ps_cvr3_calc` AS
WITH ga_dps_sessions AS (
    SELECT DISTINCT
        x.created_date, -- Date of the ga session
        x.entity_id, -- Entity ID
        x.platform, -- Operating system (iOS, Android, Web, etc.)
        x.brand, -- Talabat, foodpanda, Foodora, etc.
        x.events_ga_session_id, -- GA session ID
        x.fullvisitor_id, -- The visit_id defined by Google Analytics
        x.visit_id, -- 	The visit_id defined by Google Analytics
        x.has_transaction, -- A field that indicates whether or not a session ended in a transaction
        x.total_transactions, -- The total number of transactions in the GA session
        x.ga_dps_session_id, -- DPS session ID
        
        x.sessions.dps_session_timestamp, -- The timestamp of the DPS log.
        x.sessions.endpoint, -- The endpoint from where the DPS request is coming, including MultipleFee, which could come from Listing Page or others. and SingleFee, which could come from Menu page or others
        x.sessions.perseus_client_id, -- A unique customer identifier based on the device
        x.sessions.variant, -- AB variant (e.g. Control, Variation1, Variation2, etc.)
        x.sessions.customer_status, -- The customer.tag in DPS log, indicating whether the customer is new or not
        x.sessions.location, -- The customer.location in DPS log
        x.sessions.variant_concat, -- The concatenation of all the existing variants in the DPS log for the dps session id. There might be multiple variants due to location changes or session timeout
        x.sessions.location_concat, -- The concatenation of all the existing locations in the DPS log for the dps session id
        x.sessions.customer_status_concat, -- 	The concatenation of all the existing customer.tag in the DPS log for the dps session id

        e.event_action, -- Can have five values --> home_screen.loaded, shop_list.loaded, shop_details.loaded, checkout.loaded, transaction
        e.vendor_code, -- Vendor ID
        e.event_time, -- The timestamp of the event's creation.
        e.transaction_id, -- The transaction id for the GA session if the session has a transaction (i.e. order code)
        e.expedition_type, -- The delivery type of the session, pickup or delivery

        dps.id, -- Zone ID based on the DPS session
        dps.name, -- Zone name based on the DPS session
        dps.timezone, -- Time zone of the city based on the DPS session

        ST_ASTEXT(x.ga_location) AS ga_location, -- GA location expressed as a STRING
        
        logs.DF_total,
        logs.DF_tt,
    FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_ga_sessions` x
    LEFT JOIN UNNEST(events) e
    LEFT JOIN UNNEST(dps_zone) dps
    INNER JOIN `dh-logistics-product-ops.pricing.loved_brands_apac_city_data_sg_groceries_low_ps_cvr3_calc` cd ON x.entity_id = cd.entity_id AND ST_CONTAINS(cd.zone_shape, x.ga_location) -- This is an alternative to using dps.name/dps.id in the WHERE clause. Here, we filter for sessions in the chosen city
    LEFT JOIN `dh-logistics-product-ops.pricing.loved_brands_apac_dps_logs_sg_groceries_low_ps_cvr3_calc` logs -- You can use an INNER JOIN here if it's important for your to have a DF value associated with every session
        ON TRUE
        AND x.entity_id = logs.entity_id 
        AND x.ga_dps_session_id = logs.dps_session_id 
        AND x.created_date = logs.created_date 
        AND e.vendor_code = logs.vendor_code -- **IMPORTANT**: Sometimes, the dps logs give us multiple delivery fees per session. One reason for this could be a change in location. We eliminated sessions with multiple DFs in the previous step to keep the dataset clean
    WHERE TRUE
        AND x.entity_id = entity_var
        AND x.created_date BETWEEN start_date AND end_date -- Sessions' start and end date
        AND e.event_action IN ('shop_details.loaded', 'transaction') -- transaction / shop_details.loaded = CVR3
        AND (e.vendor_code IN (SELECT vendor_code FROM `dh-logistics-product-ops.pricing.loved_brands_apac_all_vendors_after_session_order_cvr_filters_sg_groceries_low_ps_cvr3_calc`)) -- Filter for relevant DPS sessions ONLY (i.e., those that belong to vendor IDs that were selected in the first code section)
)

SELECT * FROM ga_dps_sessions 
ORDER BY events_ga_session_id, event_time;

-------------------------------------------------------------------------END OF THE GA SESSIONS + DPS LOGS PART-------------------------------------------------------------------------

-- Now, calculate the conversion rate per DF bucket for each vendor
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.loved_brands_apac_cvr_per_df_bucket_sg_groceries_low_ps_cvr3_calc` AS
SELECT
    vertical_zone_var AS vertical_zone,
    ses.entity_id,
    ses.DF_tt,
    COUNT(DISTINCT ses.vendor_code) AS Vendor_count,
    COALESCE(COUNT(DISTINCT CASE WHEN ses.event_action = 'shop_details.loaded' THEN ses.events_ga_session_id ELSE NULL END), 0) AS Num_Unique_Vendor_Visits, -- If a vendor was visited more than once in the same session, this is considered one visit
    COALESCE(COUNT(DISTINCT CASE WHEN ses.event_action = 'shop_details.loaded' THEN ses.event_time ELSE NULL END), 0) AS Num_Total_Vendor_Impressions, -- If a vendor was visited more than once in the same session, all impressions are counted
    COALESCE(COUNT(DISTINCT CASE WHEN ses.event_action = 'transaction' THEN ses.event_time ELSE NULL END), 0) AS Num_Transactions,
    COALESCE(ROUND(COUNT(DISTINCT CASE WHEN ses.event_action = 'transaction' THEN ses.event_time ELSE NULL END) / NULLIF(COUNT(DISTINCT CASE WHEN ses.event_action = 'shop_details.loaded' THEN ses.events_ga_session_id ELSE NULL END), 0), 3), 0) AS CVR3,
FROM `dh-logistics-product-ops.pricing.loved_brands_apac_ga_dps_sessions_sg_groceries_low_ps_cvr3_calc` ses
WHERE DF_tt IN (SELECT fee FROM `dh-logistics-product-ops.pricing.loved_brands_apac_df_tiers_sg_groceries_low_ps_cvr3_calc`) -- Filter for the main DF thresholds ONLY
GROUP BY 1,2,3
ORDER BY 1,2,3;

-------------------------------------------------------------------------END OF THE CVR PER DF TIER PART---------------------------------------------------------------------------------

-- Sample size calculation based on CVR (H&W High PS)
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.sample_size_calculation_with_sessions_sg_groceries_low_ps_cvr3_calc` AS
WITH cvr_events AS (
    SELECT 
        e.country_code,
        city_name_var AS city_name,
        e.created_date
        , COUNT(DISTINCT e.ga_session_id) AS total_sessions
        , COUNT(DISTINCT e.shop_list_no) AS shop_list_sessions
        , COUNT(DISTINCT e.shop_menu_no) AS shop_menu_sessions
        , COUNT(DISTINCT e.checkout_no) AS checkout_sessions
        , COUNT(DISTINCT e.checkout_transaction) AS checkout_transaction_sessions
        , COUNT(DISTINCT e.transaction_no) AS transactions
        , COUNT(DISTINCT e.perseus_client_id) AS users
    FROM `fulfillment-dwh-production.cl.dps_cvr_events` e
    CROSS JOIN UNNEST(e.vendor_code) ven
    WHERE TRUE 
        AND e.entity_id = entity_var -- HERE
        AND e.created_date BETWEEN start_date AND end_date -- HERE
        AND (e.dps_city_name LIKE CONCAT('%', city_name_var, '%')) -- HERE
        AND ven IN (SELECT DISTINCT vendor_code FROM `dh-logistics-product-ops.pricing.loved_brands_apac_all_vendors_after_session_order_cvr_filters_sg_groceries_low_ps_cvr3_calc`) -- HERE: Treatment scope vendors (replace with your own table)
    GROUP BY 1,2,3
)

SELECT 
    city_name,
    COUNT(DISTINCT created_date) AS Count_days,
    SUM (users) AS users,
    SUM (total_sessions) AS total_sessions,
    SUM (shop_list_sessions) AS shop_list_sessions,
    SUM (shop_menu_sessions) AS shop_menu_sessions,
    SUM (checkout_sessions) AS checkout_sessions,
    SUM (checkout_transaction_sessions) AS checkout_transaction_sessions,
    SUM (transactions) AS transactions,
    ROUND(SUM (transactions) / SUM (total_sessions), 4) AS CVR1,
    ROUND(SUM (transactions) / SUM (shop_menu_sessions), 4) AS CVR3,
    ROUND(SUM (checkout_transaction_sessions) / SUM (checkout_sessions), 4) AS mCVR4_prime
FROM cvr_events e
GROUP BY 1;

-------------------------------------------------------------------------END OF THE SAMPLE SIZE ESTIMATION PART---------------------------------------------------------------------------------

-- Combining everything together
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.order_shares_sg_shops_cvr3_calc` AS
SELECT
    all_o.Orders_All_Shops,
    SUM(gr_l_all.Num_Orders) AS Orders_All_Vendors_Groceries_Low_PS,
    gr_l_pop.Orders_Popular_Vendors_Groceries_Low_PS,
    
    gr_h_all.Orders_All_Vendors_Groceries_High_PS,
    gr_h_pop.Orders_Popular_Vendors_Groceries_High_PS,
    
    hw_h_all.Orders_All_Vendors_H_and_W_High_PS,
    hw_h_pop.Orders_Popular_Vendors_H_and_W_High_PS,
    
    hw_l_all.Orders_All_Vendors_H_and_W_Low_PS,
    hw_l_pop.Orders_Popular_Vendors_H_and_W_Low_PS,

    swn_h_all.Orders_All_Vendors_Sweets_and_Snacks_High_PS,
    swn_h_pop.Orders_Popular_Vendors_Sweets_and_Snacks_High_PS
FROM `dh-logistics-product-ops.pricing.loved_brands_apac_all_vendors_sg_groceries_low_ps_cvr3_calc` gr_l_all
-- All shops
LEFT JOIN (
    SELECT
        COUNT(DISTINCT o.order_id) AS Orders_All_Shops
    FROM `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` o
    INNER JOIN `dh-logistics-product-ops.pricing.loved_brands_apac_city_data_sg_groceries_low_ps_cvr3_calc` cd ON o.global_entity_id = cd.entity_id AND ST_CONTAINS(cd.zone_shape, ST_GEOGPOINT(delivery_location.longitude, delivery_location.latitude)) -- Filter for delivery locations in the city of choice
    INNER JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.vendors` ven ON o.global_entity_id = ven.global_entity_id AND o.vendor_id = ven.vendor_id
    WHERE TRUE
        AND o.global_entity_id = entity_var
        AND DATE(o.placed_at) BETWEEN start_date AND end_date
        AND o.is_sent -- Successful order
        AND ven.is_own_delivery
        AND ven.vertical_parent = 'Local Store'
) all_o ON TRUE

-- Groceries Low PS
LEFT JOIN (
    SELECT SUM(a.Num_Orders) AS Orders_Popular_Vendors_Groceries_Low_PS
    FROM `dh-logistics-product-ops.pricing.loved_brands_apac_all_vendors_sg_groceries_low_ps_cvr3_calc` a
    INNER JOIN `dh-logistics-product-ops.pricing.loved_brands_apac_all_vendors_after_session_order_cvr_filters_sg_groceries_low_ps_cvr3_calc` b USING (entity_id, city_name, vendor_code)
) gr_l_pop ON TRUE 

-- Groceries High PS
LEFT JOIN (
    SELECT SUM(a.Num_Orders) AS Orders_All_Vendors_Groceries_High_PS
    FROM `dh-logistics-product-ops.pricing.loved_brands_apac_all_vendors_sg_groceries_high_ps_cvr3_calc` a
) gr_h_all ON TRUE

LEFT JOIN (
    SELECT SUM(a.Num_Orders) AS Orders_Popular_Vendors_Groceries_High_PS
    FROM `dh-logistics-product-ops.pricing.loved_brands_apac_all_vendors_sg_groceries_high_ps_cvr3_calc` a
    INNER JOIN `dh-logistics-product-ops.pricing.loved_brands_apac_all_vendors_after_session_order_cvr_filters_sg_groceries_high_ps_cvr3_calc` b USING (entity_id, city_name, vendor_code)
) gr_h_pop ON TRUE

-- Health and Wellness High PS
LEFT JOIN (
    SELECT SUM(a.Num_Orders) AS Orders_All_Vendors_H_and_W_High_PS
    FROM `dh-logistics-product-ops.pricing.loved_brands_apac_all_vendors_sg_health_and_wellness_high_ps_cvr3_calc` a
) hw_h_all ON TRUE

LEFT JOIN (
    SELECT SUM(a.Num_Orders) AS Orders_Popular_Vendors_H_and_W_High_PS
    FROM `dh-logistics-product-ops.pricing.loved_brands_apac_all_vendors_sg_health_and_wellness_high_ps_cvr3_calc` a
    INNER JOIN `dh-logistics-product-ops.pricing.loved_brands_apac_all_vendors_after_session_order_cvr_filters_sg_health_and_wellness_high_ps_cvr3_calc` b USING (entity_id, city_name, vendor_code)
) hw_h_pop ON TRUE

-- Health and Wellness Low PS
LEFT JOIN (
    SELECT SUM(a.Num_Orders) AS Orders_All_Vendors_H_and_W_Low_PS
    FROM `dh-logistics-product-ops.pricing.loved_brands_apac_all_vendors_sg_health_and_wellness_low_ps_cvr3_calc` a
) hw_l_all ON TRUE

LEFT JOIN (
    SELECT SUM(a.Num_Orders) AS Orders_Popular_Vendors_H_and_W_Low_PS
    FROM `dh-logistics-product-ops.pricing.loved_brands_apac_all_vendors_sg_health_and_wellness_low_ps_cvr3_calc` a
    INNER JOIN `dh-logistics-product-ops.pricing.loved_brands_apac_all_vendors_after_session_order_cvr_filters_sg_health_and_wellness_low_ps_cvr3_calc` b USING (entity_id, city_name, vendor_code)
) hw_l_pop ON TRUE

-- Snacks and Sweets High PS
LEFT JOIN (
    SELECT SUM(a.Num_Orders) AS Orders_All_Vendors_Sweets_and_Snacks_High_PS
    FROM `dh-logistics-product-ops.pricing.loved_brands_apac_all_vendors_sg_snacks_and_sweets_high_ps_cvr3_calc` a
) swn_h_all ON TRUE

LEFT JOIN (
    SELECT SUM(a.Num_Orders) AS Orders_Popular_Vendors_Sweets_and_Snacks_High_PS
    FROM `dh-logistics-product-ops.pricing.loved_brands_apac_all_vendors_sg_snacks_and_sweets_high_ps_cvr3_calc` a
    INNER JOIN `dh-logistics-product-ops.pricing.loved_brands_apac_all_vendors_after_session_order_cvr_filters_sg_snacks_and_sweets_high_ps_cvr3_calc` b USING (entity_id, city_name, vendor_code)
) swn_h_pop ON TRUE
GROUP BY 1,3,4,5,6,7,8,9,10,11;

-------------------------------------------------------------------------END OF THE ORDER SHARE PART-------------------------------------------------------------------------
