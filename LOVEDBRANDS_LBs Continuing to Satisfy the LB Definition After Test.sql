-- Please do the following before running the script:
/*
1. Update lines 11 to 13 with your parameters (zone ID is optional. If you set a value for it, uncomment the relevant lines in the WHERE clauses below)
2. Update line 17 with the name of the table from the first script
3. Update lines 150 to 156 manually according to the price schemes in the city that you are interested in
*/

-- HERE: I commented "Median_cvr3_threshold" out AND "Pct_chng_of_median_cvr3_from_base" due to thin data (median is 0)

DECLARE entity, city, vertical, d_type, variant_var STRING;
DECLARE zn_id ARRAY <INT64>;
DECLARE df_array ARRAY <FLOAT64>;
DECLARE df_lowest_tier FLOAT64;
DECLARE highest_main_df FLOAT64;
DECLARE start_date, end_date DATE;
SET (entity, city, vertical, d_type, variant_var) = ('FP_MY', 'Klang valley', 'restaurants', 'OWN_DELIVERY', 'Variation1');
SET zn_id = [1];
SET df_array = [0.49, 1.99, 2.99, 3.99, 4.99, 5.99, 6.99, 7.99, 8.99, 9.99]; -- Main DF tiers in the city
SET df_lowest_tier = 0.0; -- Lowest DF tier in the city
SET highest_main_df = 9.99; -- The highest DF tier beyond which we notice a significant drop in orders
SET (start_date, end_date) = (DATE('2022-03-03'), CURRENT_DATE());

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.loved_brands_apac_dps_logs_after_test_klang_valley` AS
WITH city_data AS (
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
        AND entity_id = entity
        AND ci.name = city
        AND ci.is_active -- Active city
        AND zo.is_active -- Active zone
        --AND zo.id IN UNNEST(zn_id)
),

dps_logs_stg_1 AS ( -- Will be used to get the CVR per DF
    SELECT DISTINCT
        logs.entity_id,
        logs.created_date,
        endpoint,
        customer.user_id AS perseus_id,
        customer.session.id AS dps_session_id,
        v.id AS vendor_code,
        v.delivery_fee.total AS DF_total,
        customer.session.timestamp AS session_timestamp,
        logs.created_at
FROM `fulfillment-dwh-production.cl.dynamic_pricing_user_sessions` logs
LEFT JOIN UNNEST(vendors) v
INNER JOIN city_data cd ON logs.entity_id = cd.entity_id AND ST_CONTAINS(cd.zone_shape, logs.customer.location) -- Filter for sessions in the city specified above
WHERE TRUE 
    AND logs.entity_id = entity
    AND logs.created_date BETWEEN start_date AND end_date
    AND logs.customer.session.id IS NOT NULL -- We must have the dps session ID to be able to obtain the session's DF in the next query
    AND endpoint = 'singleFee' -- Caters for the following events --> "shop_details.loaded", "checkout.loaded" and "transaction"
    AND customer.variant = variant_var
),

dps_logs_stg_2 AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY dps_session_id, vendor_code ORDER BY created_at DESC) AS row_num_dps_logs -- Create a row counter to take the last delivery fee seen in the session. We assume that this is the one that the customer took their decision to purchase/not purchase on
    FROM dps_logs_stg_1
),

dps_logs AS(
    SELECT *
    FROM dps_logs_stg_2 
    WHERE row_num_dps_logs = 1 -- Take the last DF seen by the customer during the session
)

SELECT * FROM dps_logs
ORDER BY dps_session_id, vendor_code, created_at;

-------------------------------------------------------------------------END OF THE DPS LOGS PART-------------------------------------------------------------------------

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.loved_brands_apac_ga_dps_sessions_after_test_klang_valley` AS
WITH city_data AS (
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
        AND entity_id = entity
        AND ci.name = city
        AND ci.is_active -- Active city
        AND zo.is_active -- Active zone
        --AND zo.id IN UNNEST(zn_id)
),

ga_dps_sessions AS (
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
        
        logs.DF_total
    FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_ga_sessions` x
    LEFT JOIN UNNEST(events) e
    LEFT JOIN UNNEST(dps_zone) dps
    LEFT JOIN UNNEST(ab_tests) ab
    INNER JOIN city_data cd ON x.entity_id = cd.entity_id AND ST_CONTAINS(cd.zone_shape, x.ga_location) -- This is an alternative to using dps.name/dps.id in the WHERE clause. Here, we filter for sessions in the chosen city
    LEFT JOIN `dh-logistics-product-ops.pricing.loved_brands_apac_dps_logs_after_test_klang_valley` logs -- You can use an INNER JOIN here if it's important for your to have a DF value associated with every session. The NULL values are anyway too few ()
        ON TRUE
        AND x.entity_id = logs.entity_id 
        AND x.ga_dps_session_id = logs.dps_session_id 
        AND x.created_date = logs.created_date 
        AND e.vendor_code = logs.vendor_code -- **IMPORTANT**: Sometimes, the dps logs give us multiple delivery fees per session. One reason for this could be a change in location. We eliminated sessions with multiple DFs in the previous step to keep the dataset clean
    WHERE TRUE
        AND x.entity_id = entity
        AND x.created_date BETWEEN start_date AND end_date -- Sessions' start and end date
        --AND dps.id IN UNNEST(zn_id) -- Zone name: Use dps.name and not ga.zone_name as the former is more complete
        AND e.event_action IN ('shop_details.loaded', 'transaction') -- transaction / shop_details.loaded = CVR3
        AND x.sessions.variant = variant_var
)

SELECT * FROM ga_dps_sessions 
ORDER BY events_ga_session_id, event_time;

-------------------------------------------------------------------------END OF THE GA SESSIONS + DPS LOGS PART-------------------------------------------------------------------------

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.loved_brands_apac_cvr_per_df_bucket_after_test_klang_valley` AS
WITH filtered_vendors AS (
    SELECT 
        a.entity_id,
        a.vendor_code
    FROM `dh-logistics-product-ops.pricing.loved_brands_apac_final_vendor_list_all_data_klang_valley` a -- Change the name of the table HERE
    LEFT JOIN `dh-logistics-product-ops.pricing.loved_brands_fd_cofund_sprint` b ON a.entity_id = b.entity_id AND a.vendor_code = b.vendor_code AND b.city_name = 'Klang valley'
    WHERE b.vendor_code IS NULL -- ANTI JOIN (vendors narrowed down to 3377 vendors before March 18th, 3366 vendors after March 18th)
)

-- Now, calculate the conversion rate per DF bucket for each vendor
SELECT
    ven.entity_id,
    ven.vendor_code,
    ses.DF_total,
    COALESCE(COUNT(DISTINCT CASE WHEN ses.event_action = 'shop_details.loaded' THEN ses.events_ga_session_id ELSE NULL END), 0) AS Num_Unique_Vendor_Visits, -- If a vendor was visited more than once in the same session, this is considered one visit
    COALESCE(COUNT(DISTINCT CASE WHEN ses.event_action = 'shop_details.loaded' THEN ses.event_time ELSE NULL END), 0) AS Num_Total_Vendor_Impressions, -- If a vendor was visited more than once in the same session, all impressions are counted
    COALESCE(COUNT(DISTINCT CASE WHEN ses.event_action = 'transaction' THEN ses.event_time ELSE NULL END), 0) AS Num_Transactions,
    COALESCE(ROUND(COUNT(DISTINCT CASE WHEN ses.event_action = 'transaction' THEN ses.event_time ELSE NULL END) / NULLIF(COUNT(DISTINCT CASE WHEN ses.event_action = 'shop_details.loaded' THEN ses.events_ga_session_id ELSE NULL END), 0), 3), 0) AS CVR3
FROM filtered_vendors ven
LEFT JOIN `dh-logistics-product-ops.pricing.loved_brands_apac_ga_dps_sessions_after_test_klang_valley` ses ON ven.entity_id = ses.entity_id AND ven.vendor_code = ses.vendor_code
GROUP BY 1,2,3
ORDER BY 1,2,3;

-------------------------------------------------------------------------END OF THE FIRST PART-------------------------------------------------------------------------

-- Instead of specifying CVR3 drop thresholds that cannot be exceeded in a manual manner, we will calculate the median CVR3 and total per DF bucket and use the percentage changes of either one of them from the lowest DF tier to each subsequent one as our thresholds.

-- First, calculate the overall and median CVR3 per DF tier/bucket
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.loved_brands_apac_cvr_per_df_bucket_plus_cvr_thresholds_after_test_klang_valley` AS
WITH cvr_per_df_bucket AS (
    SELECT
        entity_id,
        DF_total,
        SUM(Num_Unique_Vendor_Visits) AS Num_Unique_Vendor_Visits,
        SUM(Num_Transactions) AS Num_Transactions,
        ROUND(SUM(Num_Transactions) / SUM(Num_Unique_Vendor_Visits), 3) AS Overall_cvr3_per_df,
        AVG(Median_cvr3_per_df) AS Median_cvr3_per_df -- PERCENTILE_CONT does not aggregate, hence why we need to use AVG here
    FROM (
        SELECT
            entity_id,
            DF_total,
            Num_Unique_Vendor_Visits,
            Num_Transactions,
            ROUND(PERCENTILE_CONT(CVR3, 0.5) OVER (PARTITION BY entity_id, DF_total), 3) AS Median_cvr3_per_df
        FROM `dh-logistics-product-ops.pricing.loved_brands_apac_cvr_per_df_bucket_after_test_klang_valley`
        WHERE DF_total IN UNNEST(df_array) -- Filter for the main DF thresholds ONLY
    )
    GROUP BY 1,2
    ORDER BY 1,2
),

-- Second, calculate the overall and median CVR3 at the smallest DF so that we can calculate the percentage drop in CVR3 from that base 
cvr3_at_min_df_tbl AS (
    SELECT
        Overall_cvr3_per_df AS Overall_cvr3_at_min_df, 
        Median_cvr3_per_df AS Median_cvr3_at_min_df -- No need for an entity_id here since we are only working the same country any way. Adding entity_id will over-complicate the query
    FROM cvr_per_df_bucket a
    WHERE DF_total = (SELECT MIN(DF_total) FROM cvr_per_df_bucket)
),

-- Third, calculate the percentage drop in CVR3 from the base calculated in the previous step
cvr3_chngs AS (
    SELECT 
        *, 
        -- ROUND(Median_cvr3_per_df / Median_cvr3_at_min_df - 1, 3) AS Pct_chng_of_median_cvr3_from_base, -- HERE: Commented out due to thin data (median is 0)
        ROUND(Overall_cvr3_per_df / Overall_cvr3_at_min_df - 1, 3) AS Pct_chng_of_overall_cvr3_from_base
    FROM cvr_per_df_bucket a 
    LEFT JOIN cvr3_at_min_df_tbl b ON TRUE -- This table contains only one value so there are no keys ot join on
    ORDER BY 1,2
),

cvr_thresholds AS (
    SELECT
        a.*,
        b.CVR3 AS CVR3_df_lowest_tier, -- CVR3 at the lowest DF tier of the price scheme, not the lowest DF observed in the vendor's sessions
        ROUND(a.CVR3 / NULLIF(b.CVR3, 0) - 1, 3) AS Pct_chng_of_actual_cvr3_from_base, -- The base here being the lowest DF in the price scheme not the lowest DF observed in the vendor's sessions
        
        c.Median_cvr3_per_df,
        -- c.Pct_chng_of_median_cvr3_from_base, -- HERE: Commented out due to thin data (median is 0)
        -- CASE 
        --     WHEN a.DF_total = df_lowest_tier THEN NULL
        --     ELSE ROUND(b.CVR3 * (1 + c.Pct_chng_of_median_cvr3_from_base), 3) -- "+" as c.Pct_chng_of_median_cvr3_from_base is negative 
        -- END AS Median_cvr3_threshold, -- HERE: Commented out due to thin data (median is 0)
        
        c.Overall_cvr3_per_df,
        c.Pct_chng_of_overall_cvr3_from_base,
        CASE 
            WHEN a.DF_total = df_lowest_tier THEN NULL
            ELSE ROUND(b.CVR3 * (1 + c.Pct_chng_of_overall_cvr3_from_base), 3) -- "+" as c.Pct_chng_of_overall_cvr3_from_base is negative 
        END AS Overall_cvr3_threshold
    FROM `dh-logistics-product-ops.pricing.loved_brands_apac_cvr_per_df_bucket_after_test_klang_valley` a
    LEFT JOIN `dh-logistics-product-ops.pricing.loved_brands_apac_cvr_per_df_bucket_after_test_klang_valley` b ON a.entity_id = b.entity_id AND a.vendor_code = b.vendor_code AND b.DF_total = df_lowest_tier -- To get the CVR3 at the lowest DF tier of the price scheme
    LEFT JOIN cvr3_chngs c ON a.entity_id = c.entity_id AND a.DF_total = c.DF_total
    WHERE a.DF_total IN UNNEST(df_array)
    ORDER BY 1,2,3
),

cvr_thresholds_with_flags AS (
    SELECT 
        *,
        -- CASE WHEN CVR3 >= Median_cvr3_threshold AND Median_cvr3_threshold IS NOT NULL AND DF_total <= highest_main_df THEN 'Y' ELSE 'N' END AS Is_test_passed_median_cvr3, -- Don't consider the lowest DF tier OR DFs higher than "highest_main_df" -- HERE: Commented out due to thin data (median is 0)
        -- SUM(CASE WHEN CVR3 >= Median_cvr3_threshold AND Median_cvr3_threshold IS NOT NULL AND DF_total <= highest_main_df THEN 1 ELSE 0 END) OVER (PARTITION BY entity_id, vendor_code) AS Flag_median_cvr3, -- Don't consider the lowest DF tier OR DFs higher than "highest_main_df" -- HERE: Commented out due to thin data (median is 0)
        CASE WHEN CVR3 >= Overall_cvr3_threshold AND Overall_cvr3_threshold IS NOT NULL AND DF_total <= highest_main_df THEN 'Y' ELSE 'N' END AS Is_test_passed_overall_cvr3, -- Don't consider the lowest DF tier OR DFs higher than "highest_main_df"
        SUM(CASE WHEN CVR3 >= Overall_cvr3_threshold AND Overall_cvr3_threshold IS NOT NULL AND DF_total <= highest_main_df THEN 1 ELSE 0 END) OVER (PARTITION BY entity_id, vendor_code) AS Flag_overall_cvr3 -- Don't consider the lowest DF tier OR DFs higher than "highest_main_df"
    FROM cvr_thresholds
)

-- Display the results first
SELECT *
FROM cvr_thresholds_with_flags
ORDER BY 1,2,3;

-------------------------------------------------------------------------END OF THE SECOND PART-------------------------------------------------------------------------

-- Pull all the data associated with the filtered vendors in addition to the share of total orders they constitute in the city
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.loved_brands_apac_final_vendor_list_all_data_after_test_klang_valley` AS
WITH city_data AS (
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
        AND entity_id = entity
        AND ci.name = city
        AND ci.is_active -- Active city
        AND zo.is_active -- Active zone
        --AND zo.id IN UNNEST(zn_id)
),

orders_city AS ( -- Orders of ALL OD restaurant vendors in the city
    SELECT
        o.global_entity_id,
        COUNT(DISTINCT order_id) AS Orders_city
    FROM `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` o
    INNER JOIN city_data cd ON o.global_entity_id = cd.entity_id AND ST_CONTAINS(cd.zone_shape, ST_GEOGPOINT(o.delivery_location.longitude, o.delivery_location.latitude))
    WHERE TRUE
        AND o.global_entity_id = entity
        AND DATE(o.placed_at) BETWEEN start_date AND end_date
        AND o.is_sent -- Successful order
        AND o.is_own_delivery -- Own delivery vendors (no need to filter for restaurant vendors as we already filter for those in the vendors sub-table)
        AND o.is_qcommerce = FALSE -- restaurant orders  
    GROUP BY 1
),

filtered_vendors_cvr_by_df AS (
    SELECT
        entity_id,
        vendor_code,
        Flag_overall_cvr3,
        ARRAY_TO_STRING(ARRAY_AGG(CAST(DF_total AS STRING) ORDER BY DF_total), ', ') AS DFs_seen_in_sessions,
        ARRAY_TO_STRING(ARRAY_AGG(CAST(CVR3 AS STRING) ORDER BY DF_total), ', ') AS Actual_vendor_cvr3_by_df,
        ARRAY_TO_STRING(ARRAY_AGG(CAST(Pct_chng_of_actual_cvr3_from_base AS STRING) ORDER BY DF_total), ', ') AS Pct_chng_of_actual_vendor_cvr3_from_base,
        -- ARRAY_TO_STRING(ARRAY_AGG(CAST(Is_test_passed_median_cvr3 AS STRING) ORDER BY DF_total), ', ') AS Is_test_passed_median_cvr3, -- HERE: Commented out due to thin data (median is 0)
        ARRAY_TO_STRING(ARRAY_AGG(CAST(Overall_cvr3_per_df AS STRING) ORDER BY DF_total), ', ') AS Overall_cvr3_per_df,
        ARRAY_TO_STRING(ARRAY_AGG(CAST(Pct_chng_of_overall_cvr3_from_base AS STRING) ORDER BY DF_total), ', ') AS Pct_chng_of_overall_cvr3_from_base,
        ARRAY_TO_STRING(ARRAY_AGG(CAST(Is_test_passed_overall_cvr3 AS STRING) ORDER BY DF_total), ', ') AS Is_test_passed_overall_cvr3
    FROM `dh-logistics-product-ops.pricing.loved_brands_apac_cvr_per_df_bucket_plus_cvr_thresholds_after_test_klang_valley`
    WHERE Flag_overall_cvr3 > 0 -- Include vendors which passed at least one of the DF_threshold tests
    GROUP BY 1,2,3
    ORDER BY 1,2
),

filtered_vendors_all_metrics AS (
    SELECT 
        a.entity_id,
        a.vendor_code,
        a.Num_orders,
        a.Num_Unique_Vendor_Visits,
        a.Num_Total_Vendor_Impressions,
        a.Num_Transactions,
        a.CVR3,
        a.Orders_pct_rank,
        a.Unique_visits_pct_rank,
        a.CVR3_pct_rank,

        b.DFs_seen_in_sessions,
        b.Actual_vendor_cvr3_by_df,
        b.Pct_chng_of_actual_vendor_cvr3_from_base,
        b.Overall_cvr3_per_df,
        b.Pct_chng_of_overall_cvr3_from_base,
        b.Is_test_passed_overall_cvr3,
        b.Flag_overall_cvr3,
        c.Orders_city,
        SUM(a.Num_orders) OVER (PARTITION BY entity_id) AS Orders_filtered_vendors
    FROM `dh-logistics-product-ops.pricing.loved_brands_apac_final_vendor_list_all_data_klang_valley` a
    INNER JOIN filtered_vendors_cvr_by_df b USING(entity_id, vendor_code) -- Must have an inner join here because "a" contains more vendors than "b"
    INNER JOIN orders_city c ON a.entity_id = c.global_entity_id
)

SELECT * FROM filtered_vendors_all_metrics 
ORDER BY 1,2;
