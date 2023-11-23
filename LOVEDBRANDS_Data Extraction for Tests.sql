-- Step 1.1: Create a table containing the region of each entity ID
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.entities_lb_rollout_tests` AS
SELECT
    ent.region,
    p.entity_id,
    ent.country_iso,
    ent.country_name,
FROM `fulfillment-dwh-production.cl.entities` ent
LEFT JOIN UNNEST(platforms) p
INNER JOIN (SELECT DISTINCT entity_id FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`) dps ON p.entity_id = dps.entity_id 
WHERE TRUE
    AND p.entity_id NOT LIKE "ODR%" -- Eliminate entities starting with DN_ as they are not part of DPS
    AND p.entity_id NOT LIKE "DN_%" -- Eliminate entities starting with ODR (on-demand riders)
    AND p.entity_id NOT IN ("FP_DE", "FP_JP") -- Eliminate JP and DE because they are not DH markets any more
    AND p.entity_id != "TB_SA" -- Eliminate this incorrect entity_id for Saudi
    AND p.entity_id != "HS_BH" -- Eliminate this incorrect entity_id for Bahrain
;

-- Step 1.2: Pull the valid experiment names
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.valid_exp_names_lb_rollout_tests` AS 
SELECT DISTINCT
  entity_id,
  country_code,
  test_id,
  test_name,
FROM `fulfillment-dwh-production.cl.dps_experiment_setups` a
WHERE TRUE
  AND DATE(test_start_date) >= DATE("2022-11-28") -- Filter for tests that started from November 28th, 2022 (date of the first Loved Brands test using the productionized pipeline)
  AND (
    LOWER(test_name) LIKE "%loved_brands%" OR
    LOWER(test_name) LIKE "%love_brands%" OR
    LOWER(test_name) LIKE "%lb%" OR
    LOWER(test_name) LIKE "%lovedbrands%" OR
    LOWER(test_name) LIKE "%lovebrands%" OR
    LOWER(test_name) LIKE "%loyalty%" OR
    LOWER(test_name) LIKE "%_price sensitivity%"
  )
  AND test_name != "DK_20221213_R_B0_O_Aalborg"
;

###----------------------------------------------------------END OF VALID EXP NAMES PART----------------------------------------------------------###

-- Step 2: Extract the vendor IDs per target group along with their associated parent vertical and vertical type
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_target_groups_lb_rollout_tests` AS
WITH vendor_tg_vertical_mapping_with_dup AS (
  SELECT DISTINCT -- The DISTINCT command is important here
    entity_id,
    country_code,
    test_start_date,
    test_end_date,
    test_name,
    test_id,
    vendor_group_id,
    vendor_id AS vendor_code,
    parent_vertical, -- The parent vertical can only assume 7 values "Restaurant", "Shop", "darkstores", "restaurant", "restaurants", "shop", or NULL. The differences are due platform configurations
    CONCAT("TG", DENSE_RANK() OVER (PARTITION BY entity_id, test_name ORDER BY vendor_group_id)) AS tg_name,
    
    -- Time condition parameters
    schedule.id AS tc_id,
    schedule.priority AS tc_priority,
    schedule.start_at,
    schedule.recurrence_end_at,
    active_days,

    -- Customer condition parameters
    customer_condition.id AS cc_id,
    customer_condition.priority AS cc_priority,
    customer_condition.orders_number_less_than,
    customer_condition.days_since_first_order_less_than,
  FROM `fulfillment-dwh-production.cl.dps_experiment_setups` a
  CROSS JOIN UNNEST(a.matching_vendor_ids) AS vendor_id
  LEFT JOIN UNNEST(test_vertical_parents) parent_vertical
  LEFT JOIN UNNEST(schedule.active_days) active_days
  WHERE TRUE 
    AND test_name IN (SELECT DISTINCT test_name FROM `dh-logistics-product-ops.pricing.valid_exp_names_lb_rollout_tests`)
),

vendor_tg_vertical_mapping_agg AS (
  SELECT 
    * EXCEPT (parent_vertical),
    ARRAY_TO_STRING(ARRAY_AGG(parent_vertical RESPECT NULLS ORDER BY parent_vertical), ", ") AS parent_vertical_concat -- We do this step because some tests have two parent verticals. If we do not aggregate, we will get duplicates 
  FROM vendor_tg_vertical_mapping_with_dup 
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)

SELECT
  a.*,
  CASE 
    WHEN parent_vertical_concat = "" THEN NULL -- Case 1
    WHEN parent_vertical_concat LIKE "%,%" THEN -- Case 2 (tests where multiple parent verticals were chosen during configuration)
      CASE
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r"(.*),\s") IN ("restaurant", "restaurants") THEN "restaurant"
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r"(.*),\s") = "shop" THEN "shop"
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r"(.*),\s") = "darkstores" THEN "darkstores"
      END
    -- Case 3 (tests where a single parent vertical was chosen during configuration)
    WHEN LOWER(parent_vertical_concat) IN ("restaurant", "restaurants") THEN "restaurant"
    WHEN LOWER(parent_vertical_concat) = "shop" THEN "shop"
    WHEN LOWER(parent_vertical_concat) = "darkstores" THEN "darkstores"
  ELSE REGEXP_SUBSTR(parent_vertical_concat, r"(.*),\s") END AS first_parent_vertical,
  
  CASE
    WHEN parent_vertical_concat = "" THEN NULL
    WHEN parent_vertical_concat LIKE "%,%" THEN
      CASE
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r",\s(.*)") IN ("restaurant", "restaurants") THEN "restaurant"
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r",\s(.*)") = "shop" THEN "shop"
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r",\s(.*)") = "darkstores" THEN "darkstores"
      END
  END AS second_parent_vertical,
  b.vertical_type -- Vertical type of the vendor (NOT parent vertical)
FROM vendor_tg_vertical_mapping_agg a
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.vendors` b ON a.entity_id = b.global_entity_id AND a.vendor_code = b.vendor_id
ORDER BY 1,2,3,4,5
;

-- Step 3: Extract the zones that are part of the experiment
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_zone_ids_lb_rollout_tests` AS
SELECT DISTINCT -- The DISTINCT command is important here
    entity_id,
    country_code,
    test_start_date,
    test_end_date,
    test_name,
    test_id,
    zone_id
FROM `fulfillment-dwh-production.cl.dps_experiment_setups` a
CROSS JOIN UNNEST(a.zone_ids) AS zone_id
CROSS JOIN UNNEST(a.matching_vendor_ids) AS vendor_id
LEFT JOIN UNNEST(test_vertical_parents) parent_vertical
WHERE TRUE
  AND test_name IN (SELECT DISTINCT test_name FROM `dh-logistics-product-ops.pricing.valid_exp_names_lb_rollout_tests`)
ORDER BY 1,2
;

-- Step 4.1: Extract the target groups, variants, and price schemes of the tests
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_tgs_variants_and_schemes_lb_rollout_tests` AS
SELECT DISTINCT
    entity_id,
    country_code,
    test_start_date,
    test_end_date,
    test_name,
    test_id,
    CONCAT("TG", priority) AS target_group,
    variation_group AS variant,
    price_scheme_id
FROM `fulfillment-dwh-production.cl.dps_experiment_setups` a
CROSS JOIN UNNEST(a.matching_vendor_ids) AS vendor_id
LEFT JOIN UNNEST(test_vertical_parents) parent_vertical
WHERE TRUE 
  AND test_name IN (SELECT DISTINCT test_name FROM `dh-logistics-product-ops.pricing.valid_exp_names_lb_rollout_tests`)
ORDER BY 1,2
;

-- Step 4.2: Find the distinct combinations of target groups, variants, and price schemes per test
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_agg_tgs_variants_and_schemes_lb_rollout_tests` AS
SELECT 
  entity_id,
  country_code,
  test_name,
  test_id,
  ARRAY_TO_STRING(ARRAY_AGG(CONCAT(target_group, " | ", variant, " | ", price_scheme_id)), ", ") AS tg_var_scheme_concat
FROM `dh-logistics-product-ops.pricing.ab_test_tgs_variants_and_schemes_lb_rollout_tests`
GROUP BY 1,2,3,4
;

-- Step 5: Extract the polygon shapes of the experiment"s target zones
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_geo_data_lb_rollout_tests` AS
SELECT 
    p.entity_id,
    co.country_code,
    ci.name AS city_name,
    ci.id AS city_id,
    zo.shape AS zone_shape, 
    zo.name AS zone_name,
    zo.id AS zone_id,
    tgt.test_name,
    tgt.test_id,
    tgt.test_start_date,
    tgt.test_end_date,
FROM `fulfillment-dwh-production.cl.countries` co
LEFT JOIN UNNEST(co.platforms) p
LEFT JOIN UNNEST(co.cities) ci
LEFT JOIN UNNEST(ci.zones) zo
INNER JOIN `dh-logistics-product-ops.pricing.ab_test_zone_ids_lb_rollout_tests` tgt ON p.entity_id = tgt.entity_id AND co.country_code = tgt.country_code AND zo.id = tgt.zone_id 
WHERE TRUE 
    AND zo.is_active -- Active city
    AND ci.is_active -- Active zone
;

###----------------------------------------------------------END OF EXP SETUPS PART----------------------------------------------------------###

-- Step 6: Pull the business KPIs from dps_sessions_mapped_to_orders_v2
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_individual_orders_lb_rollout_tests` AS
WITH test_start_and_end_dates AS ( -- Get the start and end dates per test
  SELECT DISTINCT
    entity_id,
    country_code,
    test_start_date,
    test_end_date,
    test_name,
    test_id
  FROM `dh-logistics-product-ops.pricing.ab_test_zone_ids_lb_rollout_tests`
)

SELECT 
    -- Identifiers and supplementary fields     
    -- Date and time
    a.created_date AS created_date_utc,
    a.order_placed_at AS order_placed_at_utc,
    a.order_placed_at_local,
    FORMAT_DATE("%A", DATE(order_placed_at_local)) AS dow_local,
    a.dps_sessionid_created_at AS dps_sessionid_created_at_utc,
    DATE_DIFF(DATE(a.order_placed_at_local), DATE_ADD(DATE(dat.test_start_date), INTERVAL 1 DAY), DAY) + 1 AS day_num_in_test, -- We add "+1" so that the first day gets a "1" not a "0"

    -- Location of order
    a.region,
    a.entity_id,
    a.country_code,
    a.city_name,
    a.city_id,
    a.zone_name,
    a.zone_id,
    zn.zone_shape,
    ST_GEOGPOINT(dwh.delivery_location.longitude, dwh.delivery_location.latitude) AS customer_location,

    -- Order/customer identifiers and session data
    a.variant,
    a.experiment_id AS test_id,
    dat.test_name,
    dat.test_start_date,
    dat.test_end_date,
    a.perseus_client_id,
    a.ga_session_id,
    a.dps_sessionid,
    a.dps_customer_tag,
    a.customer_total_orders,
    a.customer_first_order_date,
    DATE_DIFF(a.order_placed_at, a.customer_first_order_date, DAY) AS days_since_first_order,
    a.order_id,
    a.platform_order_code,
    a.scheme_id,
    a.vendor_price_scheme_type,	-- The assignment type of the scheme to the vendor during the time of the order, such as "Automatic", "Manual", "Campaign", and "Country Fallback".
    
    -- Vendor data and information on the delivery
    a.vendor_id,
    COALESCE(tg.tg_name, "Non_TG") AS target_group,
    b.target_group AS target_group_bi,
    a.is_in_treatment,
    a.chain_id,
    a.chain_name,
    a.vertical_type,
    CASE 
      WHEN a.vendor_vertical_parent IS NULL THEN NULL 
      WHEN LOWER(a.vendor_vertical_parent) IN ("restaurant", "restaurants") THEN "restaurant"
      WHEN LOWER(a.vendor_vertical_parent) = "shop" THEN "shop"
      WHEN LOWER(a.vendor_vertical_parent) = "darkstores" THEN "darkstores"
    END AS vendor_vertical_parent,
    a.delivery_status,
    a.is_own_delivery,
    a.exchange_rate,

    -- Business KPIs (These are the components of profit)
    a.dps_delivery_fee_local,
    a.delivery_fee_local,
    IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.delivery_fee_local) AS delivery_fee_local_incl_disc_and_vouchers,
    a.dps_travel_time_fee_local,
    CASE WHEN ent.region != "MENA" THEN a.commission_local ELSE COALESCE(mn.commission_amount_lc, a.commission_local) END AS commission_local,
    a.joker_vendor_fee_local,
    COALESCE(a.service_fee_local, 0) AS service_fee_local,
    a.mov_customer_fee_local AS sof_local,
    a.delivery_costs_local,
    CASE
        WHEN ent.region IN ("Europe", "Asia") THEN COALESCE( -- Get the delivery fee data of Pandora countries from Pandata tables
            pd.delivery_fee_local, 
            -- In 99 pct of cases, we won"t need to use that fallback logic as pd.delivery_fee_local is reliable
            IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.delivery_fee_local)
        )
        -- If the order comes from a non-Pandora country, use delivery_fee_local
        WHEN ent.region NOT IN ("Europe", "Asia") THEN (CASE WHEN a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE THEN 0 ELSE a.delivery_fee_local END)
    END AS actual_df_paid_by_customer,
    a.gfv_local,
    a.gmv_local,

    -- Logistics KPIs
    a.mean_delay, -- A.K.A Average fleet delay --> Average lateness in minutes of an order at session start time (Used by dashboard, das, dps). This data point is only available for OD orders
    a.dps_mean_delay, -- A.K.A DPS Average fleet delay --> Average lateness in minutes of an order placed at this time coming from DPS service
    a.dps_mean_delay_zone_id, -- ID of the zone where fleet delay applies
    a.travel_time, -- The time (min) it takes rider to travel from vendor location coordinates to the customers. This data point is only available for OD orders.
    a.dps_travel_time, -- The calculated travel time in minutes from the vendor to customer coming from DPS
    a.travel_time_distance_km, -- The distance (km) between the vendor location coordinates and customer location coordinates. This data point is only available for OD orders
    a.delivery_distance_m, -- This is the "Delivery Distance" field in the overview tab in the AB test dashboard. The Manhattan distance (km) between the vendor location coordinates and customer location coordinates
    -- This distance doesn"t take into account potential stacked deliveries, and it"s not the travelled distance. This data point is only available for OD orders.
    a.to_customer_time, -- The time difference between rider arrival at customer and the pickup time. This data point is only available for OD orders
    a.actual_DT, -- The time it took to deliver the order. Measured from order creation until rider at customer. This data point is only available for OD orders.

    -- Special fields
    a.is_delivery_fee_covered_by_discount, -- Needed in the profit formula
    a.is_delivery_fee_covered_by_voucher, -- Needed in the profit formula
    tg.parent_vertical_concat,
    -- This filter is used to clean the data. It removes all orders that did not belong to the correct target_group, variant, scheme_id combination as dictated by the experiment"s setup
    CASE WHEN COALESCE(tg.tg_name, "Non_TG") = "Non_TG" OR vs.tg_var_scheme_concat LIKE CONCAT("%", COALESCE(tg.tg_name, "Non_TG"), " | ", a.variant, " | ", a.scheme_id, "%") THEN "Keep" ELSE "Drop" END AS keep_drop_flag,
    CASE WHEN 
      COALESCE(b.target_group, "Non_TG") = "Non_TG"
      OR
      vs.tg_var_scheme_concat LIKE CONCAT("%", COALESCE(CONCAT("TG", REGEXP_EXTRACT(b.target_group, r'\d+')), "Non_TG"), " | ", a.variant, " | ", a.scheme_id, "%") THEN "Keep" 
    ELSE "Drop"
    END AS keep_drop_flag_bi
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` a
LEFT JOIN `fulfillment-dwh-production.cl.dps_ab_test_orders_v2` b ON a.entity_id = b.entity_id AND a.order_id = b.order_id
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` dwh 
  ON TRUE 
    AND a.entity_id = dwh.global_entity_id
    AND a.platform_order_code = dwh.order_id -- There is no country_code field in this table
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` pd -- Contains info on the orders in Pandora countries
  ON TRUE 
    AND a.entity_id = pd.global_entity_id
    AND a.platform_order_code = pd.code 
    AND a.created_date = pd.created_date_utc -- There is no country_code field in this table
LEFT JOIN `tlb-data-prod.data_platform.fct_billing` mn ON a.platform_order_code = CAST(mn.order_id AS STRING) AND a.entity_id IN ("TB_OM", "TB_IQ", "TB_AE", "TB_KW", "YS_TR", "TB_QA", "TB_JO", "HF_EG", "HS_SA", "TB_BH")
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_geo_data_lb_rollout_tests` zn 
  ON TRUE 
    AND a.entity_id = zn.entity_id 
    AND a.country_code = zn.country_code
    AND a.zone_id = zn.zone_id 
    AND a.experiment_id = zn.test_id -- Filter for orders in the target zones (combine this JOIN with the condition in the WHERE clause)
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_target_groups_lb_rollout_tests` tg -- Tag the vendors with their target group association
  ON TRUE
    AND a.entity_id = tg.entity_id
    AND a.vendor_id = tg.vendor_code 
    AND a.experiment_id = tg.test_id 
    AND
      CASE WHEN DATE(tg.start_at) IS NULL AND DATE(tg.recurrence_end_at) IS NULL AND tg.active_days IS NULL THEN TRUE -- If there is no time condition in the experiment, skip the join step
      ELSE -- If there is, assign orders to the relevant target groups depending on the two time condition parts
        DATE(a.order_placed_at_local) BETWEEN DATE(tg.start_at) AND DATE(tg.recurrence_end_at) -- A join for the time condition (1)
        AND UPPER(FORMAT_DATE("%A", DATE(a.order_placed_at_local))) = tg.active_days -- A join for the time condition (2)
      END
    AND 
      CASE WHEN tg.orders_number_less_than IS NULL AND tg.days_since_first_order_less_than IS NULL THEN TRUE -- If there is no customer condition in the experiment, skip the join step
      ELSE -- If there is, assign the orders with dps_customer_tag = "New" to their relevant target groups depending on the "calendar week" AND the two customer condition parameters (total_orders and days_since_first_order)
        a.customer_total_orders < tg.orders_number_less_than -- customer_total_orders always > 0 when dps_customer_tag = "New"
        -- customer_first_order_date could be NULL or have a DATETIME value. In both cases, dps_customer_tag could be equal to "New"
        AND (DATE_DIFF(a.order_placed_at, a.customer_first_order_date, DAY) < tg.days_since_first_order_less_than OR DATE_DIFF(a.order_placed_at, a.customer_first_order_date, DAY) IS NULL)
      END
LEFT JOIN test_start_and_end_dates dat ON a.entity_id = dat.entity_id AND a.country_code = dat.country_code AND a.experiment_id = dat.test_id
LEFT JOIN `dh-logistics-product-ops.pricing.ab_test_agg_tgs_variants_and_schemes_lb_rollout_tests` vs -- Get the list of target_group | variation | scheme_id combinations that are relevant to the experiment
  ON TRUE
    AND a.entity_id = vs.entity_id 
    AND a.country_code = vs.country_code 
    AND a.experiment_id = vs.test_id
INNER JOIN `dh-logistics-product-ops.pricing.entities_lb_rollout_tests` ent ON a.entity_id = ent.entity_id -- Get the region associated with every entity_id
WHERE TRUE
    AND a.created_date >= DATE("2022-11-28") -- Filter for tests that started from November 28th, 2022 (date of the first Loved Brands test using the productionized pipeline)
    
    AND CONCAT(a.entity_id, " | ", a.country_code, " | ", a.experiment_id, " | ", a.variant) IN ( -- Filter for the right variants belonging to the experiment (essentially filter out NULL and Original)
      SELECT DISTINCT CONCAT(entity_id, " | ", country_code, " | ", test_id, " | ", variant) 
      FROM `dh-logistics-product-ops.pricing.ab_test_tgs_variants_and_schemes_lb_rollout_tests`
      WHERE CONCAT(entity_id, " | ", country_code, " | ", test_id, " | ", variant) IS NOT NULL
    )
    
    AND a.is_sent -- Successful orders
    
    AND CONCAT(a.entity_id, " | ", a.country_code, " | ", a.experiment_id) IN ( -- Filter for the right entity | experiment_id combos. 
      -- The "ab_test_target_groups_lb_rollout_tests" table was specifically chosen from the tables in steps 2-4 because it automatically eliminates tests where there are no matching vendors
      SELECT DISTINCT CONCAT(entity_id, " | ", country_code, " | ", test_id)
      FROM `dh-logistics-product-ops.pricing.ab_test_target_groups_lb_rollout_tests`
      WHERE CONCAT(entity_id, " | ", country_code, " | ", test_id) IS NOT NULL
    )
    
    AND ST_CONTAINS(zn.zone_shape, ST_GEOGPOINT(dwh.delivery_location.longitude, dwh.delivery_location.latitude)) -- Filter for orders coming from the target zones
;

###----------------------------------------------------------SEPARATOR----------------------------------------------------------###

-- Step 7: We did not add the profit metrics and the parent_vertical filter to the previous query because some of the fields used below had to be computed first
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ab_test_individual_orders_cleaned_lb_rollout_tests` AS
SELECT
  a.*,
  -- Revenue and profit formulas
  IF(a.is_own_delivery = TRUE, SAFE_DIVIDE(a.actual_df_paid_by_customer, 1 + b.vat_ratio), 0) + a.commission_local + a.joker_vendor_fee_local + SAFE_DIVIDE(a.service_fee_local, 1 + b.vat_ratio) + SAFE_DIVIDE(a.sof_local, 1 + b.vat_ratio) AS revenue_local,
  IF(a.is_own_delivery = TRUE, SAFE_DIVIDE(a.actual_df_paid_by_customer, 1 + b.vat_ratio), 0) + a.commission_local + a.joker_vendor_fee_local + SAFE_DIVIDE(a.service_fee_local, 1 + b.vat_ratio) + SAFE_DIVIDE(a.sof_local, 1 + b.vat_ratio) - a.delivery_costs_local AS gross_profit_local,

FROM `dh-logistics-product-ops.pricing.ab_test_individual_orders_lb_rollout_tests` a
INNER JOIN `fulfillment-dwh-production.cl.dps_ab_test_orders_v2` b ON a.entity_id = b.entity_id AND a.platform_order_code = b.platform_order_code
WHERE TRUE -- Filter for orders from the right parent vertical (restuarants, shop, darkstores, etc.) per experiment
    AND (
      CONCAT(a.entity_id, " | ", a.country_code, " | ", a.test_id, " | ", a.vendor_vertical_parent) IN ( -- If the parent vertical exists, filter for the right one belonging to the experiment
        SELECT DISTINCT CONCAT(entity_id, " | ", country_code, " | ", test_id, " | ", first_parent_vertical)
        FROM `dh-logistics-product-ops.pricing.ab_test_target_groups_lb_rollout_tests`
        WHERE CONCAT(entity_id, " | ", country_code, " | ", test_id, " | ", first_parent_vertical) IS NOT NULL
      )
      OR
      CONCAT(a.entity_id, " | ", a.country_code, " | ", a.test_id, " | ", a.vendor_vertical_parent) IN ( -- If the parent vertical exists, filter for the right one belonging to the experiment
        SELECT DISTINCT CONCAT(entity_id, " | ", country_code, " | ", test_id, " | ", second_parent_vertical)
        FROM `dh-logistics-product-ops.pricing.ab_test_target_groups_lb_rollout_tests`
        WHERE CONCAT(entity_id, " | ", country_code, " | ", test_id, " | ", second_parent_vertical) IS NOT NULL
      )
    )
;

###----------------------------------------------------------END OF RAW ORDERS EXTRACTION PART----------------------------------------------------------###

-- Step 8: Retrieve raw session data
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ga_sessions_data_lb_rollout_tests` AS
WITH raw_sessions_data AS (
  SELECT DISTINCT
    x.created_date, -- Date of the ga session
    ent.region, -- Region
    x.entity_id, -- Entity ID
    x.country_code, -- Country code
    x.platform, -- Operating system (iOS, Android, Web, etc.)
    x.brand, -- Talabat, foodpanda, Foodora, etc.
    x.events_ga_session_id, -- GA session ID
    x.fullvisitor_id, -- The visit_id defined by Google Analytics
    x.visit_id, -- 	The visit_id defined by Google Analytics
    x.has_transaction, -- A field that indicates whether or not a session ended in a transaction
    x.total_transactions, -- The total number of transactions in the GA session
    x.ga_dps_session_id, -- DPS session ID

    x.sessions.dps_session_timestamp, -- The timestamp of the DPS logs
    x.sessions.endpoint, -- The endpoint from where the DPS request is coming
    x.sessions.perseus_client_id, -- A unique customer identifier based on the device
    x.sessions.variant, -- AB variant (e.g. Control, Variation1, Variation2, etc.)
    x.sessions.experiment_id AS test_id, -- Experiment ID
    x.sessions.is_parallel, -- Is this a parallel test?
    LOWER(ven.vertical_parent) AS parent_vertical_vendor_tbl, -- Parent vertical from the vendors table
    CASE 
      WHEN ent.region IN ("Americas", "Asia") THEN
        CASE 
          WHEN LOWER(ven.vertical_parent) = "food" THEN "restaurant"
          WHEN LOWER(ven.vertical_parent) IN ("local store", "dmarts") THEN "shop"
          WHEN LOWER(ven.vertical_parent) = "courier" THEN "courier"
        ELSE LOWER(ven.vertical_parent) END
      WHEN ent.region = "MENA" THEN
        CASE 
          WHEN LOWER(ven.vertical_parent) = "food" THEN "restaurant"
          WHEN LOWER(ven.vertical_parent) = "local store" THEN "shop"
          WHEN LOWER(ven.vertical_parent) = "dmarts" THEN "darkstores"
          WHEN LOWER(ven.vertical_parent) = "courier" THEN "courier"
        ELSE LOWER(ven.vertical_parent) END
      WHEN ent.region = "Europe" THEN LOWER(ven.vertical_parent)
    END AS parent_vertical_test_equivalent,
    vert.first_parent_vertical AS first_parent_vertical_test,
    vert.second_parent_vertical AS second_parent_vertical_test,
    e.vertical_type, -- This field is NULL for event types home_screen.loaded and shop_list.loaded 
    e.vendor_group_id, -- Target group
    CASE 
        WHEN e.vendor_group_id IS NULL AND e.vendor_code IS NULL THEN "Unknown"
        WHEN e.vendor_group_id IS NULL AND e.vendor_code IS NOT NULL THEN "Non Target Group"
        ELSE CONCAT('Target Group ', DENSE_RANK() OVER (PARTITION BY x.entity_id, x.sessions.experiment_id ORDER BY COALESCE(e.vendor_group_id, 999999)))
    END AS target_group_bi,
    x.sessions.customer_status, -- The customer.tag, indicating whether the customer is new or not
    x.sessions.location, -- The customer.location
    x.sessions.variant_concat, -- The concatenation of all the existing variants for the dps session id. There might be multiple variants due to location changes or session timeout
    x.sessions.location_concat, -- The concatenation of all the existing locations for the dps session id
    x.sessions.customer_status_concat, -- The concatenation of all the existing customer.tag for the dps session id

    e.event_action, -- Can have five values --> home_screen.loaded, shop_list.loaded, shop_details.loaded, checkout.loaded, transaction
    e.vendor_code, -- Vendor ID
    e.event_time, -- The timestamp of the event's creation
    e.transaction_id, -- The transaction id for the GA session if the session has a transaction (i.e. order code)
    e.expedition_type, -- The delivery type of the session, pickup or delivery

    dps.city_id, -- City ID based on the DPS session
    dps.city_name, -- City name based on the DPS session
    dps.id AS zone_id, -- Zone ID based on the DPS session
    dps.name AS zone_name, -- Zone name based on the DPS session
    dps.timezone, -- Time zone of the city based on the DPS session

    ST_ASTEXT(x.ga_location) AS ga_location -- GA location expressed as a STRING
  FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_ga_sessions` AS x
  LEFT JOIN UNNEST(events) AS e
  LEFT JOIN UNNEST(dps_zone) AS dps
  LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.vendors` ven ON x.entity_id = ven.global_entity_id AND e.vendor_code = ven.vendor_id
  LEFT JOIN `dh-logistics-product-ops.pricing.entities_lb_rollout_tests` ent ON x.entity_id = ent.entity_id
  LEFT JOIN (
    SELECT DISTINCT entity_id, test_name, test_id, first_parent_vertical, second_parent_vertical
    FROM `dh-logistics-product-ops.pricing.ab_test_target_groups_lb_rollout_tests`
  ) vert ON x.entity_id = vert.entity_id AND x.sessions.experiment_id = vert.test_id
  WHERE TRUE
    AND created_date >= DATE("2022-11-28")
    AND CONCAT(x.entity_id, " | ", x.sessions.experiment_id, " | ", x.sessions.variant) IN ( -- Filter for the right variants belonging to the experiment (essentially filter out NULL and Original)
      SELECT DISTINCT CONCAT(entity_id, " | ", test_id, " | ", variant) 
      FROM `dh-logistics-product-ops.pricing.ab_test_tgs_variants_and_schemes_lb_rollout_tests`
      WHERE CONCAT(entity_id, " | ", test_id, " | ", variant) IS NOT NULL
    )
      
    AND CONCAT(x.entity_id, " | ", x.sessions.experiment_id) IN ( -- Filter for the right entity | experiment_id combos. 
      SELECT DISTINCT CONCAT(entity_id, " | ", test_id)
      FROM `dh-logistics-product-ops.pricing.ab_test_target_groups_lb_rollout_tests`
      WHERE CONCAT(entity_id, " | ", test_id) IS NOT NULL
    )
)

SELECT 
  a.*,
  CASE
    WHEN event_action IN ("home_screen.loaded", "shop_list.loaded") THEN "Unknown"
    WHEN event_action NOT IN ("home_screen.loaded", "shop_list.loaded") AND target_group_bi IN ("Non Target Group", "Unknown") THEN "N"
    WHEN event_action NOT IN ("home_screen.loaded", "shop_list.loaded") AND target_group_bi NOT IN ("Non Target Group", "Unknown") THEN "Y"
    ELSE NULL
  END AS is_session_in_treatment_raw
FROM raw_sessions_data a
;

-- Step 9: Create a treatment flag per session
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.treatment_flag_per_session_lb_rollout_tests` AS
WITH agg_session_stats AS (
  SELECT
    entity_id,
    test_id,
    events_ga_session_id,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT event_action ORDER BY event_action), ", ") AS distinct_event_actions_per_session,
    SUM(CASE WHEN is_session_in_treatment_raw = "Y" THEN 1 ELSE 0 END) AS num_instances_with_treated_vendors,
    SUM(CASE WHEN is_session_in_treatment_raw = "N" THEN 1 ELSE 0 END) AS num_instances_with_no_treated_vendors,
    SUM(CASE WHEN is_session_in_treatment_raw = "Unknown" THEN 1 ELSE 0 END) AS num_instances_with_unknown_treatment_vendors,
  FROM `dh-logistics-product-ops.pricing.ga_sessions_data_lb_rollout_tests`
  GROUP BY 1,2,3
)

SELECT
  *,
  CASE
    WHEN 
      distinct_event_actions_per_session = "home_screen.loaded" 
      OR distinct_event_actions_per_session = "home_screen.loaded, shop_list.loaded"
      OR distinct_event_actions_per_session = "shop_list.loaded" THEN "Unknown"
    WHEN 
      distinct_event_actions_per_session != "home_screen.loaded"
      AND distinct_event_actions_per_session != "home_screen.loaded, shop_list.loaded"
      AND distinct_event_actions_per_session != "shop_list.loaded" 
      AND num_instances_with_treated_vendors >= 1 THEN "Y"
  ELSE "N"
  END AS is_session_in_treatment_agg
FROM agg_session_stats
;

-- Step 10: Join the treatment flag to the sessions data and add the test_name
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ga_sessions_data_lb_rollout_tests` AS
SELECT
  a.*,
  b.is_session_in_treatment_agg,
  tst.test_name
FROM `dh-logistics-product-ops.pricing.ga_sessions_data_lb_rollout_tests` a
LEFT JOIN `dh-logistics-product-ops.pricing.treatment_flag_per_session_lb_rollout_tests` b ON a.entity_id = b.entity_id AND a.test_id = b.test_id AND a.events_ga_session_id = b.events_ga_session_id
LEFT JOIN `dh-logistics-product-ops.pricing.valid_exp_names_lb_rollout_tests` tst ON a.entity_id = tst.entity_id AND a.test_id = tst.test_id
;
