-- V2 includes a shortened list of KPIs to improve the speed of the query
-- V3 incorporates Rok's comments about calculating the % difference between the even and odd days

-- Step 1: Declare the input variables used throughout the script
DECLARE entity_city_zone_var ARRAY <STRUCT <entity_id STRING, city_name STRING, zone_name STRING>>;
DECLARE v_type ARRAY <STRING>;
DECLARE start_date, end_date DATE;
DECLARE exclude_dates_var ARRAY <STRUCT <start_date_exclude DATE, end_date_exclude DATE>>;
DECLARE zone_combos ARRAY <STRUCT <entity_id STRING, city_name STRING, zone_combo ARRAY <STRING>, zone_combo_id INT64>>;

-- Choose the relevant DH entity-city combinations
SET entity_city_zone_var = [
    -- If you want to enter a NULL, it has to be with a CAST command. If you want several zones or cities, you need to make multiple entries
    STRUCT('FP_SG' AS entity_id, 'Singapore' AS city_name, CAST(NULL AS STRING) AS zone_name),
    STRUCT('FP_HK' AS entity_id, CAST(NULL AS STRING) AS city_name, CAST(NULL AS STRING) AS zone_name),
    STRUCT('FP_PH' AS entity_id, 'Cebu' AS city_name, 'Mandaue' AS zone_name),
    STRUCT('FP_PH' AS entity_id, 'Cebu' AS city_name, 'Naga city' AS zone_name),
    STRUCT('FP_PH' AS entity_id, CAST(NULL AS STRING) AS city_name, 'Marikina' AS zone_name),
    STRUCT('FP_PH' AS entity_id, 'Calamba' AS city_name, CAST(NULL AS STRING) AS zone_name),
    STRUCT('FP_TW' AS entity_id, CAST(NULL AS STRING) AS city_name, CAST(NULL AS STRING) AS zone_name),
    STRUCT('FP_PK' AS entity_id, CAST(NULL AS STRING) AS city_name, CAST(NULL AS STRING) AS zone_name),
    STRUCT('FP_BD' AS entity_id, CAST(NULL AS STRING) AS city_name, CAST(NULL AS STRING) AS zone_name),
    STRUCT('FP_LA' AS entity_id, CAST(NULL AS STRING) AS city_name, CAST(NULL AS STRING) AS zone_name),
    STRUCT('FP_MY' AS entity_id, CAST(NULL AS STRING) AS city_name, CAST(NULL AS STRING) AS zone_name),
    STRUCT('FP_KH' AS entity_id, CAST(NULL AS STRING) AS city_name, CAST(NULL AS STRING) AS zone_name),
    STRUCT('FP_MM' AS entity_id, CAST(NULL AS STRING) AS city_name, CAST(NULL AS STRING) AS zone_name),
    STRUCT('FP_TH' AS entity_id, CAST(NULL AS STRING) AS city_name, CAST(NULL AS STRING) AS zone_name)
];

-- You need to make a separate entry for every zone combination you want to pull historical data for. The zones included here MUST be the same as the ones defined above
-- You **cannot** have NULLs in this nested array of structs
-- Zones that are included in the "entity_city_zone_var" array of structs but not defined here are given a "zone_combo" of "Rest of Zones" and a "zone_combo_id" of "0"
SET zone_combos = [
    STRUCT('FP_SG' AS entity_id, 'Singapore' AS city_name, ['Amk', 'Bedok', 'Bukit timah', 'Bukitpanjang'] AS zone_combo, 1 AS zone_combo_id),
    STRUCT('FP_SG' AS entity_id, 'Singapore' AS city_name, ['Far_east', 'Geylang', 'Jurong east'] AS zone_combo, 2 AS zone_combo_id),
    STRUCT('FP_SG' AS entity_id, 'Singapore' AS city_name, ['Jurongwest', 'Sengkang'] AS zone_combo, 3 AS zone_combo_id),

    STRUCT('FP_HK' AS entity_id, 'Hong kong' AS city_name, ['Central rider', 'Central walker', 'Tin shui wai rider', 'Tin shui wai walker'] AS zone_combo, 1 AS zone_combo_id),
        
    STRUCT('FP_PH' AS entity_id, 'Cebu' AS city_name, ['Mandaue', 'Naga city'] AS zone_combo, 1 AS zone_combo_id),
    STRUCT('FP_PH' AS entity_id, 'Manila' AS city_name, ['Marikina'] AS zone_combo, 1 AS zone_combo_id),
    STRUCT('FP_PH' AS entity_id, 'Calamba' AS city_name, ['Cabuyao', 'Calamba'] AS zone_combo, 1 AS zone_combo_id),
    STRUCT('FP_PH' AS entity_id, 'Calamba' AS city_name, ['Canlubang'] AS zone_combo, 2 AS zone_combo_id),

    STRUCT('FP_TW' AS entity_id, CAST(NULL AS STRING) AS city_name, [] AS zone_combo, CAST(NULL AS INT64) AS zone_combo_id),
    STRUCT('FP_PK' AS entity_id, CAST(NULL AS STRING) AS city_name, [] AS zone_combo, CAST(NULL AS INT64) AS zone_combo_id),
    STRUCT('FP_BD' AS entity_id, CAST(NULL AS STRING) AS city_name, [] AS zone_combo, CAST(NULL AS INT64) AS zone_combo_id),
    STRUCT('FP_LA' AS entity_id, CAST(NULL AS STRING) AS city_name, [] AS zone_combo, CAST(NULL AS INT64) AS zone_combo_id),
    STRUCT('FP_MY' AS entity_id, CAST(NULL AS STRING) AS city_name, [] AS zone_combo, CAST(NULL AS INT64) AS zone_combo_id),
    STRUCT('FP_KH' AS entity_id, CAST(NULL AS STRING) AS city_name, [] AS zone_combo, CAST(NULL AS INT64) AS zone_combo_id),
    STRUCT('FP_MM' AS entity_id, CAST(NULL AS STRING) AS city_name, [] AS zone_combo, CAST(NULL AS INT64) AS zone_combo_id),
    STRUCT('FP_TH' AS entity_id, CAST(NULL AS STRING) AS city_name, [] AS zone_combo, CAST(NULL AS INT64) AS zone_combo_id)
]; -- Choose the zone combinations to cluster together
SET v_type = ['restaurants']; -- Choose the relevant vertical type(s)
-- Go back 52 weeks in time from **this week's start date**. The current week always starts on a Monday
SET (start_date, end_date) = (DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 52 WEEK), MONTH), DATE_TRUNC(CURRENT_DATE(), MONTH));
SET exclude_dates_var = [
    STRUCT(DATE('2020-01-01') AS start_date_exclude, DATE('2020-03-25') AS end_date_exclude), 
    STRUCT(DATE('2020-03-27') AS start_date_exclude, DATE('2020-04-15') AS end_date_exclude)
]; -- Define any date range that you wish to exclude in the format shown above. If you don't want to exclude any dates, simply input date ranges that are **outside** the range given above --> [start_date, end_date]

##-------------------------------------------------------------------------------------END OF INPUTS SECTION-------------------------------------------------------------------------------------##

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.switchback_testing_periods` AS
SELECT 
    [
        STRUCT(GENERATE_DATE_ARRAY(start_date + 0, start_date + 13, INTERVAL 1 DAY) AS time_frame, 1 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 14, start_date + 27, INTERVAL 1 DAY) AS time_frame, 2 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 28, start_date + 41, INTERVAL 1 DAY) AS time_frame, 3 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 42, start_date + 55, INTERVAL 1 DAY) AS time_frame, 4 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 56, start_date + 69, INTERVAL 1 DAY) AS time_frame, 5 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 70, start_date + 83, INTERVAL 1 DAY) AS time_frame, 6 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 84, start_date + 97, INTERVAL 1 DAY) AS time_frame, 7 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 98, start_date + 111, INTERVAL 1 DAY) AS time_frame, 8 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 112, start_date + 125, INTERVAL 1 DAY) AS time_frame, 9 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 126, start_date + 139, INTERVAL 1 DAY) AS time_frame, 10 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 140, start_date + 153, INTERVAL 1 DAY) AS time_frame, 11 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 154, start_date + 167, INTERVAL 1 DAY) AS time_frame, 12 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 168, start_date + 181, INTERVAL 1 DAY) AS time_frame, 13 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 182, start_date + 195, INTERVAL 1 DAY) AS time_frame, 14 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 196, start_date + 209, INTERVAL 1 DAY) AS time_frame, 15 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 210, start_date + 223, INTERVAL 1 DAY) AS time_frame, 16 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 224, start_date + 237, INTERVAL 1 DAY) AS time_frame, 17 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 238, start_date + 251, INTERVAL 1 DAY) AS time_frame, 18 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 252, start_date + 265, INTERVAL 1 DAY) AS time_frame, 19 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 266, start_date + 279, INTERVAL 1 DAY) AS time_frame, 20 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 280, start_date + 293, INTERVAL 1 DAY) AS time_frame, 21 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 294, start_date + 307, INTERVAL 1 DAY) AS time_frame, 22 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 308, start_date + 321, INTERVAL 1 DAY) AS time_frame, 23 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 322, start_date + 335, INTERVAL 1 DAY) AS time_frame, 24 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 336, start_date + 349, INTERVAL 1 DAY) AS time_frame, 25 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 350, start_date + 363, INTERVAL 1 DAY) AS time_frame, 26 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 364, start_date + 377, INTERVAL 1 DAY) AS time_frame, 27 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 378, start_date + 391, INTERVAL 1 DAY) AS time_frame, 28 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 392, start_date + 405, INTERVAL 1 DAY) AS time_frame, 29 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 406, start_date + 419, INTERVAL 1 DAY) AS time_frame, 30 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 420, start_date + 433, INTERVAL 1 DAY) AS time_frame, 31 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 434, start_date + 447, INTERVAL 1 DAY) AS time_frame, 32 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 448, start_date + 461, INTERVAL 1 DAY) AS time_frame, 33 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 462, start_date + 475, INTERVAL 1 DAY) AS time_frame, 34 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 476, start_date + 489, INTERVAL 1 DAY) AS time_frame, 35 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 490, start_date + 503, INTERVAL 1 DAY) AS time_frame, 36 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 504, start_date + 517, INTERVAL 1 DAY) AS time_frame, 37 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 518, start_date + 531, INTERVAL 1 DAY) AS time_frame, 38 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 532, start_date + 545, INTERVAL 1 DAY) AS time_frame, 39 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 546, start_date + 559, INTERVAL 1 DAY) AS time_frame, 40 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 560, start_date + 573, INTERVAL 1 DAY) AS time_frame, 41 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 574, start_date + 587, INTERVAL 1 DAY) AS time_frame, 42 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 588, start_date + 601, INTERVAL 1 DAY) AS time_frame, 43 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 602, start_date + 615, INTERVAL 1 DAY) AS time_frame, 44 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 616, start_date + 629, INTERVAL 1 DAY) AS time_frame, 45 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 630, start_date + 643, INTERVAL 1 DAY) AS time_frame, 46 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 644, start_date + 657, INTERVAL 1 DAY) AS time_frame, 47 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 658, start_date + 671, INTERVAL 1 DAY) AS time_frame, 48 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 672, start_date + 685, INTERVAL 1 DAY) AS time_frame, 49 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 686, start_date + 699, INTERVAL 1 DAY) AS time_frame, 50 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 700, start_date + 713, INTERVAL 1 DAY) AS time_frame, 51 AS period),
        STRUCT(GENERATE_DATE_ARRAY(start_date + 714, start_date + 727, INTERVAL 1 DAY) AS time_frame, 52 AS period)
    ] AS periods;

##-------------------------------------------------------------------------------------END OF SWITCHBACK TESTING PERIODS SECTION-------------------------------------------------------------------------------------##

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.switchback_testing_exclude_dates` AS

-- Step 1: Generate the date arrays that will be excluded from the orders data
WITH exclude_dates_unnest_struct AS ( -- Unnest the "exclude_dates_var" array of structs that was defined above in the inputs section
    SELECT 
        col.start_date_exclude, 
        col.end_date_exclude
    FROM UNNEST(exclude_dates_var) col
),

exclude_dates_filtering_array AS ( -- Generate arrays that are by bounded by each "start_date_exclude" and "end_date_exclude" defined above in the inputs section
    SELECT 
        a.*,
        GENERATE_DATE_ARRAY(a.start_date_exclude, a.end_date_exclude, INTERVAL 1 DAY) AS exclude_date_array
    FROM (
        SELECT 
            col.start_date_exclude, 
            col.end_date_exclude
        FROM UNNEST(exclude_dates_var) col
    ) a
),

exclude_date_unnested_filtering_array AS ( -- Unnest the arrays to a single column that can be used in the WHERE clause of the "dps_sessions_mapped_to_orders" table
    SELECT
        exclude_date_col
    FROM exclude_dates_filtering_array
    LEFT JOIN UNNEST(exclude_date_array) exclude_date_col
)

SELECT * FROM exclude_date_unnested_filtering_array;

##------------------------------------------------------------------------------END OF EXCLUDE DATES STEP------------------------------------------------------------------------------##

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.switchback_testing_relevant_geos` AS

-- Step 2: Generate the relevant entities, cities, and zones to get data for using the "entity_city_zone_var" array of structs that was defined above in the inputs section
WITH relevant_entity_city_zone_combos AS ( --
    SELECT *
    FROM UNNEST(entity_city_zone_var)
),

geo_data AS ( -- Get the cities and zones associated with the entities chosen by the user in the inputs section
    SELECT 
        p.entity_id,
        country_code,
        ci.name AS city_name,
        zo.name AS zone_name
    FROM fulfillment-dwh-production.cl.countries co
    LEFT JOIN UNNEST(co.platforms) p
    LEFT JOIN UNNEST(co.cities) ci
    LEFT JOIN UNNEST(ci.zones) zo
    WHERE TRUE 
        AND zo.is_active -- Active city
        AND ci.is_active -- Active zone
        AND p.entity_id IN (SELECT DISTINCT entity_id FROM relevant_entity_city_zone_combos) -- Chosen DH entities
)

SELECT -- Convert the "entity_city_zone_var" array of structs specified by the user in the inputs section to a table of entity, city, and zone combinations. This table will be used to filter for the relevant orders in the "raw orders" query below
    geo.entity_id,
    geo.country_code,
    geo.city_name,
    geo.zone_name,
    CONCAT(geo.entity_id, ' | ', geo.city_name, ' | ', geo.zone_name) AS concat_entity_city_zone
FROM geo_data geo
INNER JOIN relevant_entity_city_zone_combos com ON 
    CASE 
        WHEN com.city_name IS NULL AND com.zone_name IS NULL THEN geo.entity_id = com.entity_id
        WHEN com.city_name IS NOT NULL AND com.zone_name IS NULL THEN geo.entity_id = com.entity_id AND geo.city_name = com.city_name
        WHEN com.city_name IS NULL AND com.zone_name IS NOT NULL THEN geo.entity_id = com.entity_id AND geo.zone_name = com.zone_name
        WHEN com.city_name IS NOT NULL AND com.zone_name IS NOT NULL THEN geo.entity_id = com.entity_id AND geo.city_name = com.city_name AND geo.zone_name = com.zone_name
    END
ORDER BY 1,2,3,4;

##------------------------------------------------------------------------------END OF RELEVANT GEOS STEP------------------------------------------------------------------------------##

-- Step 3: Pull the raw orders from dps_sessions_mapped_to_orders_v2

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.switchback_testing_raw_orders` AS
WITH switchback_testing_periods AS (
    SELECT
        created_date,
        p.period,
        ROW_NUMBER() OVER(PARTITION BY period ORDER BY created_date) AS day_of_period_enumerator
    FROM `dh-logistics-product-ops.pricing.switchback_testing_periods`
    LEFT JOIN UNNEST(periods) p
    LEFT JOIN UNNEST(p.time_frame) created_date
),

zone_combos AS (
    SELECT
        z.entity_id,
        z.city_name,
        NULLIF(ARRAY_TO_STRING(z.zone_combo, ', '), '') AS zone_combo,
        zc AS zone_name,
        z.zone_combo_id
    FROM UNNEST(zone_combos) z -- That's a parameterized nested array of structs
    LEFT JOIN UNNEST(zone_combo) zc
),

entities AS ( -- A sub-query to get the region associated with every entity 
    SELECT
        ent.region,
        p.entity_id,
        ent.country_iso,
        ent.country_name,
FROM `fulfillment-dwh-production.cl.entities` ent
LEFT JOIN UNNEST(platforms) p
INNER JOIN (SELECT DISTINCT entity_id FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`) dps ON p.entity_id = dps.entity_id 
WHERE TRUE
    AND p.entity_id NOT LIKE 'ODR%' -- Eliminate entities starting with DN_ as they are not part of DPS
    AND p.entity_id NOT LIKE 'DN_%' -- Eliminate entities starting with ODR (on-demand riders)
    AND p.entity_id NOT IN ('FP_DE', 'FP_JP') -- Eliminate JP and DE because they are not DH markets any more
    AND p.entity_id != 'TB_SA' -- Eliminate this incorrect entity_id for Saudi
    AND p.entity_id != 'HS_BH' -- Eliminate this incorrect entity_id for Bahrain
    AND p.entity_id IN (SELECT DISTINCT entity_id FROM `dh-logistics-product-ops.pricing.switchback_testing_relevant_geos`) -- Pull data for the entities specified by the user in the inputs section
)

SELECT 
    -- Identifiers and supplementary fields     
    -- Date and time
    a.created_date,
    DATE_TRUNC(a.created_date, MONTH) AS month,
    FORMAT_DATE("%a", a.created_date) AS day_of_week,
    per.day_of_period_enumerator,
    CASE WHEN MOD(day_of_period_enumerator, 2) = 0 THEN 'even' ELSE 'odd' END AS even_or_odd_day, 
    DATE_DIFF(a.created_date, start_date, DAY) + 1 AS diff_in_days_from_min_date, -- "start_date" is a parametrized variable. We added "+ 1" so that we don't have zeroes in the dataset
    per.period AS switchback_testing_period,
    a.order_placed_at,

    -- Location of order
    a.region,
    a.entity_id,
    a.city_name,
    a.city_id,
    a.zone_name,
    a.zone_id,
    COALESCE(zc.city_name, 'Cities Not Explicitly Defined') AS city_name_zc,
    COALESCE(zc.zone_combo, 'Zones Not Explicitly Defined') AS zone_combo,
    COALESCE(zc.zone_combo_id, 0) AS zone_combo_id,

    -- Order/customer identifiers and session data
    a.variant,
    a.experiment_id,
    a.perseus_client_id,
    a.ga_session_id,
    a.dps_sessionid,
    a.dps_customer_tag,
    a.order_id,
    a.platform_order_code,
    a.scheme_id,
    a.vendor_price_scheme_type,	-- The assignment type of the scheme to the vendor during the time of the order, such as 'Automatic', 'Manual', 'Campaign', and 'Country Fallback'.

    -- Vendor data and information on the delivery
    a.vendor_id,
    a.chain_id,
    a.chain_name,
    a.vertical_type,
    a.vendor_vertical_parent,
    a.delivery_status,
    a.is_own_delivery,
    a.exchange_rate,

    -- Logistics KPIs
    a.dps_mean_delay, -- A.K.A Average fleet delay --> Average lateness in minutes of an order placed at this time (Used by dashboard, das, dps). This data point is only available for OD orders.
    a.dps_mean_delay_zone_id, 
    a.dps_travel_time, -- The time (min) it takes rider to travel from vendor location coordinates to the customers. This data point is only available for OD orders.
    a.mean_delay, -- Mean delay from a source different from the DPS logs (available for periods before dps_mean_delay was introduced)
    a.travel_time, -- Travel time from a source different from the DPS logs (available for periods before dps_travel_time was introduced)
    a.travel_time_distance_km, -- The distance (km) between the vendor location coordinates and customer location coordinates. This data point is only available for OD orders.
    a.delivery_distance_m, -- This is the "Delivery Distance" field in the overview tab in the AB test dashboard. The Manhattan distance (km) between the vendor location coordinates and customer location coordinates. This distance doesn't take into account potential stacked deliveries, and it's not the travelled distance. This data point is only available for OD orders.
    a.to_customer_time, -- The time difference between rider arrival at customer and the pickup time. This data point is only available for OD orders
    a.actual_DT
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` a -- Main orders table
LEFT JOIN switchback_testing_periods per ON a.created_date = per.created_date 
LEFT JOIN zone_combos zc ON --a.entity_id = zc.entity_id AND a.city_name = zc.city_name AND a.zone_name = zc.zone_name
    CASE 
        WHEN zc.city_name IS NULL AND zc.zone_name IS NULL THEN a.entity_id = zc.entity_id
        WHEN zc.city_name IS NOT NULL AND zc.zone_name IS NULL THEN a.entity_id = zc.entity_id AND a.city_name = zc.city_name
        WHEN zc.city_name IS NULL AND zc.zone_name IS NOT NULL THEN a.entity_id = zc.entity_id AND a.zone_name = zc.zone_name
        WHEN zc.city_name IS NOT NULL AND zc.zone_name IS NOT NULL THEN a.entity_id = zc.entity_id AND a.city_name = zc.city_name AND a.zone_name = zc.zone_name
    END
INNER JOIN entities ent ON a.entity_id = ent.entity_id -- INNER JOIN to only include active DH entities. This is used to get the region associated with every entity
WHERE TRUE
    AND CONCAT(a.entity_id, ' | ', a.city_name, ' | ', a.zone_name) IN (SELECT concat_entity_city_zone FROM `dh-logistics-product-ops.pricing.switchback_testing_relevant_geos`) -- Filter for the entity, city, and zone combos that the user entered in the inputs section
    AND a.created_date BETWEEN start_date AND end_date -- Filter for data in the time period that the user specified in the inputs section 
    AND a.created_date NOT IN (SELECT exclude_date_col FROM `dh-logistics-product-ops.pricing.switchback_testing_exclude_dates`) -- Exclude the dates that were chosen by the user in the inputs section
    AND a.is_own_delivery -- OD or MP
    AND a.vertical_type IN UNNEST(v_type) -- Orders from a particular vertical (restuarants, groceries, darkstores, etc.)
    AND a.delivery_status = 'completed' -- Successful orders
    AND per.period IS NOT NULL
ORDER BY a.entity_id, a.city_name, a.zone_name;

##------------------------------------------------------------------------------END OF RELEVANT THE RAW ORDERS PART------------------------------------------------------------------------------##

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.switchback_testing_two_week_periods_agg_kpis` AS
WITH two_week_agg_kpis AS (
    SELECT
        -- Grouping variables
        entity_id,
        -- city_name,
        city_name_zc AS city_name,
        zone_combo,
        zone_combo_id,
        switchback_testing_period,

        -- Logistics KPIs
        ROUND(AVG(CASE WHEN even_or_odd_day = 'even' THEN dps_mean_delay ELSE NULL END) / AVG(CASE WHEN even_or_odd_day = 'odd' THEN dps_mean_delay ELSE NULL END) - 1, 4) AS rel_chng_dps_mean_delay,
        ROUND(AVG(CASE WHEN even_or_odd_day = 'even' THEN mean_delay ELSE NULL END) / AVG(CASE WHEN even_or_odd_day = 'odd' THEN mean_delay ELSE NULL END) - 1, 4) AS rel_chng_mean_delay,
        ROUND(AVG(CASE WHEN even_or_odd_day = 'even' THEN delivery_distance_m ELSE NULL END) / AVG(CASE WHEN even_or_odd_day = 'odd' THEN delivery_distance_m ELSE NULL END) - 1, 4) AS rel_chng_p_d_dist,
        ROUND(AVG(CASE WHEN even_or_odd_day = 'even' THEN actual_DT ELSE NULL END) / AVG(CASE WHEN even_or_odd_day = 'odd' THEN actual_DT ELSE NULL END) - 1, 4) AS rel_chng_delivery_time
    FROM `dh-logistics-product-ops.pricing.switchback_testing_raw_orders`
    GROUP BY 1,2,3,4,5
),

distribution_quantiles AS (
    SELECT
        entity_id,
        city_name,
        zone_combo,
        zone_combo_id,
        switchback_testing_period,

        -- 95% confidence intervals
        ROUND(PERCENTILE_CONT(rel_chng_dps_mean_delay, 0.025) OVER(PARTITION BY entity_id, city_name, zone_combo, zone_combo_id), 4) AS rel_chng_dps_mean_delay_ntile_2_dot_5,
        ROUND(PERCENTILE_CONT(rel_chng_mean_delay, 0.025) OVER(PARTITION BY entity_id, city_name, zone_combo, zone_combo_id), 4) AS rel_chng_mean_delay_ntile_2_dot_5,
        ROUND(PERCENTILE_CONT(rel_chng_p_d_dist, 0.025) OVER(PARTITION BY entity_id, city_name, zone_combo, zone_combo_id), 4) AS rel_chng_p_d_dist_ntile_2_dot_5,
        ROUND(PERCENTILE_CONT(rel_chng_delivery_time, 0.025) OVER(PARTITION BY entity_id, city_name, zone_combo, zone_combo_id), 4) AS rel_chng_delivery_time_ntile_2_dot_5,

        ROUND(PERCENTILE_CONT(rel_chng_dps_mean_delay, 0.975) OVER(PARTITION BY entity_id, city_name, zone_combo, zone_combo_id), 4) AS rel_chng_dps_mean_delay_ntile_97_dot_5,
        ROUND(PERCENTILE_CONT(rel_chng_mean_delay, 0.975) OVER(PARTITION BY entity_id, city_name, zone_combo, zone_combo_id), 4) AS rel_chng_mean_delay_ntile_97_dot_5,
        ROUND(PERCENTILE_CONT(rel_chng_p_d_dist, 0.975) OVER(PARTITION BY entity_id, city_name, zone_combo, zone_combo_id), 4) AS rel_chng_p_d_dist_ntile_97_dot_5,
        ROUND(PERCENTILE_CONT(rel_chng_delivery_time, 0.975) OVER(PARTITION BY entity_id, city_name, zone_combo, zone_combo_id), 4) AS rel_chng_delivery_time_ntile_97_dot_5,
    FROM two_week_agg_kpis
)

SELECT 
    a.*,
    b.rel_chng_dps_mean_delay_ntile_2_dot_5,
    b.rel_chng_dps_mean_delay_ntile_97_dot_5,

    b.rel_chng_mean_delay_ntile_2_dot_5,
    b.rel_chng_mean_delay_ntile_97_dot_5,

    b.rel_chng_p_d_dist_ntile_2_dot_5,
    b.rel_chng_p_d_dist_ntile_97_dot_5,

    b.rel_chng_delivery_time_ntile_2_dot_5,
    b.rel_chng_delivery_time_ntile_97_dot_5,
FROM two_week_agg_kpis a
LEFT JOIN distribution_quantiles b USING(entity_id, city_name, zone_combo, zone_combo_id, switchback_testing_period)
ORDER BY entity_id, city_name, zone_combo, zone_combo_id, switchback_testing_period;

