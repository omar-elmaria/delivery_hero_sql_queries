-- V2 has a new logic for defining target groups. We do not take TGs from "dps_ab_test_orders_v2" anymore, but from "cl.dps_experiment_setups"
-- V3 includes additional checks on the definition of a new customer
-- V4 includes the modifications necessary for the special test (TH_2022_04_12_MultipleFDNC_Ratchaburi_New) and removes the BIMA incentive fields due to duplicates
-- V5 includes automatic detection of FDNC tests and their associated setups and excludes the df_paid_by_new_customer = 0 condition from the definition of a new customer

-- Step 1: Declare the input variables used throughout the script
DECLARE test_name_combined_variants ARRAY <STRING>; -- Original (Real Control): Single FDNC; Variation1 + Control (Real Variation1): Multiple FDNC
DECLARE start_date, end_date DATE;
SET test_name_combined_variants = ['TH_2022_04_12_MultipleFDNC_Ratchaburi_New'];
SET (start_date, end_date) = (DATE('2022-02-22'), CURRENT_DATE());

###----------------------------------------------------------------END OF THE INPUT SECTION----------------------------------------------------------------###

-- Step 1: Extract the FDNC test names
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_names` AS 
SELECT DISTINCT
  entity_id,
  country_code,
  test_id,
  test_name
FROM `fulfillment-dwh-production.cl.dps_experiment_setups` a
CROSS JOIN UNNEST(a.matching_vendor_ids) AS vendor_id
LEFT JOIN UNNEST(test_vertical_parents) parent_vertical
WHERE TRUE
    AND misconfigured = FALSE -- Only look for setups of tests that are NOT misconfigured
    AND LOWER(test_name) LIKE '%fdnc%' -- Filter out tests that don't have the word 'fdnc' in their names
    AND vendor_id IS NOT NULL -- Filter out tests where there are no matching vendors
    AND parent_vertical IS NOT NULL; -- Filter out tests where there is no parent_vertical

###-------------------------------------------------------------------END OF STEP 1-----------------------------------------------------------------------###

-- Step 2: Extract the vendor IDs per target group along with their associated parent vertical and vertical type
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_target_groups` AS
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
    parent_vertical, -- The parent vertical can only assume 7 values 'Restaurant', 'Shop', 'darkstores', 'restaurant', 'restaurants', 'shop', or NULL. The differences are due platform configurations
    CONCAT('TG', DENSE_RANK() OVER (PARTITION BY entity_id, test_name ORDER BY vendor_group_id)) AS target_group,
  FROM `fulfillment-dwh-production.cl.dps_experiment_setups` a
  CROSS JOIN UNNEST(a.matching_vendor_ids) AS vendor_id
  LEFT JOIN UNNEST(test_vertical_parents) parent_vertical
  WHERE TRUE 
    AND test_name IN (SELECT DISTINCT test_name FROM `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_names`)
    AND misconfigured = FALSE -- Only look for setups of tests that are NOT misconfigured
    AND LOWER(test_name) LIKE '%fdnc%' -- Filter out tests that don't have the word 'fdnc' in their names
    AND vendor_id IS NOT NULL -- Filter out tests where there are no matching vendors
    AND parent_vertical IS NOT NULL -- Filter out tests where there is no parent_vertical
  QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_id, country_code, test_name, vendor_id ORDER BY vendor_group_id) = 1 -- If by mistake a vendor is included in two or more target groups, take the highest priority one 
),

vendor_tg_vertical_mapping_agg AS (
  SELECT 
    * EXCEPT (parent_vertical),
    -- We do this step because some tests may have two parent verticals. If we do not aggregate, we will get duplicates 
    ARRAY_TO_STRING(ARRAY_AGG(parent_vertical RESPECT NULLS ORDER BY parent_vertical), ', ') AS parent_vertical_concat
  FROM vendor_tg_vertical_mapping_with_dup 
  GROUP BY 1,2,3,4,5,6,7,8,9
)

SELECT
  a.*,
  CASE 
    WHEN parent_vertical_concat = '' THEN NULL -- Case 1
    WHEN parent_vertical_concat LIKE '%,%' THEN -- Case 2 (tests where multiple parent verticals were chosen during configuration)
      CASE
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r'(.*),\s') IN ('restaurant', 'restaurants') THEN 'restaurant'
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r'(.*),\s') = 'shop' THEN 'shop'
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r'(.*),\s') = 'darkstores' THEN 'darkstores'
      END
    -- Case 3 (tests where a single parent vertical was chosen during configuration)
    WHEN LOWER(parent_vertical_concat) IN ('restaurant', 'restaurants') THEN 'restaurant'
    WHEN LOWER(parent_vertical_concat) = 'shop' THEN 'shop'
    WHEN LOWER(parent_vertical_concat) = 'darkstores' THEN 'darkstores'
  ELSE REGEXP_SUBSTR(parent_vertical_concat, r'(.*),\s') END AS first_parent_vertical,
  
  CASE
    WHEN parent_vertical_concat = '' THEN NULL
    WHEN parent_vertical_concat LIKE '%,%' THEN
      CASE
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r',\s(.*)') IN ('restaurant', 'restaurants') THEN 'restaurant'
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r',\s(.*)') = 'shop' THEN 'shop'
        WHEN REGEXP_SUBSTR(LOWER(parent_vertical_concat), r',\s(.*)') = 'darkstores' THEN 'darkstores'
      END
  END AS second_parent_vertical,
  b.vertical_type -- Vertical type of the vendor (NOT parent vertical)
FROM vendor_tg_vertical_mapping_agg a
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.vendors` b ON a.entity_id = b.global_entity_id AND a.vendor_code = b.vendor_id
ORDER BY 1,2,3,4,5;

###-------------------------------------------------------------------END OF STEP 2-----------------------------------------------------------------------###

-- Step 3: Extract the zones that are part of the experiment
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_zone_ids` AS
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
    AND test_name IN (SELECT DISTINCT test_name FROM `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_names`)
    AND misconfigured = FALSE -- Only look for setups of tests that are NOT misconfigured
    AND LOWER(test_name) LIKE '%fdnc%' -- Filter out tests that don't have the word 'fdnc' in their names
    AND vendor_id IS NOT NULL -- Filter out tests where there are no matching vendors
    AND parent_vertical IS NOT NULL -- Filter out tests where there is no parent_vertical
ORDER BY 1,2;

###-------------------------------------------------------------------END OF STEP 3-----------------------------------------------------------------------###

-- Step 4.1: Extract the target groups, variants, and price schemes of the tests
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_tgs_variants_and_schemes` AS
SELECT DISTINCT
    entity_id,
    country_code,
    test_start_date,
    test_end_date,
    test_name,
    test_id,
    CONCAT('TG', priority) AS target_group,
    variation_group AS variant,
    price_scheme_id
FROM `fulfillment-dwh-production.cl.dps_experiment_setups` a
CROSS JOIN UNNEST(a.matching_vendor_ids) AS vendor_id
LEFT JOIN UNNEST(test_vertical_parents) parent_vertical
WHERE TRUE 
    AND test_name IN (SELECT DISTINCT test_name FROM `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_names`)
    AND misconfigured = FALSE -- Only look for setups of tests that are NOT misconfigured
    AND LOWER(test_name) LIKE '%fdnc%' -- Filter out tests that don't have the word 'fdnc' in their names
    AND vendor_id IS NOT NULL -- Filter out tests where there are no matching vendors
    AND parent_vertical IS NOT NULL -- Filter out tests where there is no parent_vertical
ORDER BY 1,2,5,CAST(REGEXP_EXTRACT(target_group, r'\d.*') AS INT64), variant, price_scheme_id;

-- Step 4.2: Find the distinct combinations of target groups, variants, and price schemes per test
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_agg_tgs_variants_and_schemes` AS
SELECT 
  entity_id,
  country_code,
  test_name,
  test_id,
  ARRAY_TO_STRING(ARRAY_AGG(CONCAT(target_group, ' | ', variant, ' | ', price_scheme_id)), ', ') AS tg_var_scheme_concat
FROM `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_tgs_variants_and_schemes`
GROUP BY 1,2,3,4;

###-------------------------------------------------------------------END OF STEP 4-----------------------------------------------------------------------###

-- Step 5: Extract the polygon shapes of the experiment's target zones
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_geo_data` AS
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
INNER JOIN `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_zone_ids` tgt ON p.entity_id = tgt.entity_id AND co.country_code = tgt.country_code AND zo.id = tgt.zone_id 
WHERE TRUE 
    AND zo.is_active -- Active city
    AND ci.is_active; -- Active zone

###-------------------------------------------------------------------END OF STEP 5-----------------------------------------------------------------------###

-- Step 6: Pull the business and logisitcal KPIs from "dps_sessions_mapped_to_orders_v2"
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_raw_orders` AS
WITH delivery_costs AS (
    SELECT
        p.entity_id,
        p.country_code,
        p.order_id, 
        o.platform_order_code,
        SUM(p.costs) AS delivery_costs_local,
        SUM(p.costs_eur) AS delivery_costs_eur
    FROM `fulfillment-dwh-production.cl.utr_timings` p
    LEFT JOIN `fulfillment-dwh-production.cl.orders` o ON p.entity_id = o.entity.id AND p.order_id = o.order_id -- Use the platform_order_code in this table as a bridge to join the order_id from utr_timings to order_id from central_dwh.orders 
    WHERE 1=1
        AND p.created_date BETWEEN start_date AND end_date -- For partitioning elimination and speeding up the query
        AND o.created_date BETWEEN start_date AND end_date -- For partitioning elimination and speeding up the query
    GROUP BY 1,2,3,4
),

test_start_and_end_dates AS ( -- Get the start and end dates per test
  SELECT DISTINCT
    entity_id,
    country_code,
    test_start_date,
    test_end_date,
    test_name,
    test_id
  FROM `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_zone_ids`
),

entities AS (
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
)

SELECT 
    -- Identifiers and supplementary fields     
    -- Date and time
    a.created_date,
    a.order_placed_at,

    -- Location of order
    a.region,
    a.entity_id,
    a.country_code,
    a.city_name,
    a.city_id,
    a.zone_name,
    a.zone_id,

    -- Order/customer identifiers and session data
    CASE 
        WHEN dat.test_name IN UNNEST(test_name_combined_variants) AND a.variant = 'Original' THEN 'Control'
        WHEN dat.test_name IN UNNEST(test_name_combined_variants) AND a.variant IN ('Control', 'Variation1') THEN 'Variation1'
        ELSE a.variant
    END AS variant, -- Takes into account the special multiple FDNC test in TH where control + V1 should be considered as one variant. This was a one-off thing
    dat.test_name,
    a.experiment_id AS test_id,
    dat.test_start_date,
    dat.test_end_date,
    a.perseus_client_id,
    a.ga_session_id,
    a.dps_sessionid,
    a.dps_customer_tag,
    a.order_id,
    a.platform_order_code,
    a.scheme_id,
    a.vendor_price_scheme_type,	-- The assignment type of the scheme to the vendor during the time of the order, such as 'Automatic', 'Manual' and 'Country Fallback'.
    
    -- Vendor data and information on the delivery
    a.vendor_id,
    COALESCE(tg.target_group, 'Non_TG') AS target_group,
    a.chain_id,
    a.chain_name,
    a.vertical_type,
    CASE 
        WHEN a.vendor_vertical_parent IS NULL THEN NULL 
        WHEN LOWER(a.vendor_vertical_parent) IN ('restaurant', 'restaurants') THEN 'restaurant'
        WHEN LOWER(a.vendor_vertical_parent) = 'shop' THEN 'shop'
        WHEN LOWER(a.vendor_vertical_parent) = 'darkstores' THEN 'darkstores'
    END AS vendor_vertical_parent,
    tg.parent_vertical_concat,
    a.delivery_status,
    a.is_own_delivery,
    a.exchange_rate,

    -- Business KPIs (pick the ones that are applicable to your test)
    a.delivery_fee_local, -- The delivery fee amount of the dps session.
    a.dps_travel_time_fee_local, -- The (dps_delivery_fee - dps_surge_fee) of the dps session.
    a.dps_minimum_order_value_local AS mov_local, -- The minimum order value of the dps session.
    a.dps_surge_fee_local, -- The surge fee amount of the session.
    a.dps_delivery_fee_local,
    a.joker_vendor_fee_local,
    COALESCE(a.service_fee_local, 0) AS service_fee_local, -- The service fee amount of the session.
    a.gmv_local, -- The gmv (gross merchandise value) of the order placed from backend
    a.gfv_local, -- The gfv (gross food value) of the order placed from backend
    a.dps_standard_fee_local, -- The standard fee for the session sent by DPS (Not a component of revenue. It's simply the fee from DBDF setup in DPS)
    a.commission_local,
    a.commission_base_local,
    ROUND(a.commission_local / NULLIF(a.commission_base_local, 0), 4) AS commission_rate,
    dwh.value.mov_customer_fee_local AS sof_local_cdwh,
    IF(a.gfv_local - a.dps_minimum_order_value_local >= 0, 0, COALESCE(dwh.value.mov_customer_fee_local, (a.dps_minimum_order_value_local - a.gfv_local))) AS sof_local,
    cst.delivery_costs_local,
    
    -- Logistics KPIs
    a.mean_delay, -- A.K.A Average fleet delay --> Average lateness in minutes of an order placed at this time (Used by dashboard, das, dps). This data point is only available for OD orders.
    a.travel_time, -- The time (min) it takes rider to travel from vendor location coordinates to the customers. This data point is only available for OD orders.
    a.travel_time_distance_km, -- The distance (km) between the vendor location coordinates and customer location coordinates. This data point is only available for OD orders.
    a.delivery_distance_m, -- This is the "Delivery Distance" field in the overview tab in the AB test dashboard. The Manhattan distance (km) between the vendor location coordinates and customer location coordinates. This distance doesn't take into account potential stacked deliveries, and it's not the travelled distance. This data point is only available for OD orders.
    a.to_customer_time, -- The time difference between rider arrival at customer and the pickup time. This data point is only available for OD orders
    a.actual_DT,

    -- Special fields
    CASE
        WHEN ent.region IN ('Europe', 'Asia') THEN COALESCE( -- Get the delivery fee data of Pandora countries from Pandata tables
            pd.delivery_fee_local, 
            -- In 99 pct of cases, we won't need to use that fallback logic as pd.delivery_fee_local is reliable
            IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.delivery_fee_local)
        )
        -- If the order comes from a non-Pandora country, use delivery_fee_local
        WHEN ent.region NOT IN ('Europe', 'Asia') THEN (CASE WHEN is_delivery_fee_covered_by_voucher = FALSE AND is_delivery_fee_covered_by_discount = FALSE THEN a.delivery_fee_local ELSE 0 END)
    END AS actual_df_paid_by_customer,
    a.is_delivery_fee_covered_by_discount,
    a.is_delivery_fee_covered_by_voucher,
    CASE WHEN is_delivery_fee_covered_by_discount = FALSE AND is_delivery_fee_covered_by_voucher = FALSE THEN 'No DF Voucher' ELSE 'DF Voucher' END AS df_voucher_flag,
    -- This filter is used to clean the data. It removes all orders that did not belong to the correct target_group, variant, scheme_id combination as dictated by the experiment's setup
    -- FDNC tests need a special treatment as the fallback when the condition is not satisfied is the ASA scheme
    CASE 
      WHEN
        -- If the target_group = Non_TG, don't look for a specific TG | variant | scheme combination
        COALESCE(tg.target_group, 'Non_TG') = 'Non_TG'
        OR
        -- If the order comes from a TG vendor AND the new customer condition is NOT satisfied (i.e., scheme used = ASA scheme), don't look for a specific TG | variant | scheme combination
        (COALESCE(tg.target_group, 'Non_TG') != 'Non_TG' AND (a.dps_customer_tag = 'Existing' OR a.dps_customer_tag IS NULL) AND a.vendor_price_scheme_type = 'Automatic scheme')
        OR
        -- If the order comes from a TG vendor AND the new customer condition is satisfied, look for a specific TG | variant | scheme combination
        (
          COALESCE(tg.target_group, 'Non_TG') != 'Non_TG' 
          AND a.dps_customer_tag = 'New' 
          AND vs.tg_var_scheme_concat LIKE CONCAT('%', COALESCE(tg.target_group, 'Non_TG'), ' | ', a.variant, ' | ', a.scheme_id, '%') 
          AND a.vendor_price_scheme_type = 'Experiment'
        )
      THEN 'Keep' 
    ELSE 'Drop' END AS keep_drop_flag,

    -- Date fields
    CONCAT(EXTRACT(YEAR FROM a.created_date), ' | ', 'CW ', EXTRACT(WEEK FROM a.created_date)) AS cw_year_order,
    EXTRACT(YEAR FROM a.created_date) AS year_order,
    EXTRACT(WEEK FROM a.created_date) AS cw_order,
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` a
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` dwh 
    ON TRUE 
        AND a.entity_id = dwh.global_entity_id
        AND a.platform_order_code = dwh.order_id -- There is no country_code field in this table
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` pd -- Contains info on the orders in Pandora countries
    ON TRUE 
        AND a.entity_id = pd.global_entity_id
        AND a.platform_order_code = pd.code 
        AND a.created_date = pd.created_date_utc -- There is no country_code field in this table
LEFT JOIN delivery_costs cst ON a.entity_id = cst.entity_id AND a.country_code = cst.country_code AND a.order_id = cst.order_id -- The table that stores the CPO
LEFT JOIN `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_geo_data` zn 
    ON TRUE 
        AND a.entity_id = zn.entity_id 
        AND a.country_code = zn.country_code
        AND a.zone_id = zn.zone_id 
        AND a.experiment_id = zn.test_id -- Filter for orders in the target zones (combine this JOIN with the condition in the WHERE clause)
LEFT JOIN `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_target_groups` tg 
    ON TRUE
        AND a.entity_id = tg.entity_id
        AND a.vendor_id = tg.vendor_code 
        AND a.experiment_id = tg.test_id -- Tag the vendors with their target group association
LEFT JOIN `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_agg_tgs_variants_and_schemes` vs 
    ON TRUE
        AND a.entity_id = vs.entity_id 
        AND a.country_code = vs.country_code 
        AND a.experiment_id = vs.test_id
LEFT JOIN test_start_and_end_dates dat ON a.entity_id = dat.entity_id AND a.country_code = dat.country_code AND a.experiment_id = dat.test_id
INNER JOIN entities ent ON a.entity_id = ent.entity_id -- Get the region associated with every entity_id
WHERE TRUE
    AND a.created_date BETWEEN start_date AND end_date
    AND a.delivery_status = 'completed'
    AND perseus_client_id IS NOT NULL AND perseus_client_id != '' -- Eliminate blank and NULL perseus client IDs
    AND a.is_own_delivery -- OD (MP vendors cannot get the FDNC offer because they are not on DPS)

    AND CONCAT(a.entity_id, ' | ', a.country_code, ' | ', a.experiment_id) IN ( -- Filter for the right entity | experiment_id combos. 
      -- The "fdnc_dashboard_ab_test_target_groups" table was specifically chosen from the tables in steps 2-4 because it automatically eliminates tests where there are no matching vendors
      SELECT DISTINCT CONCAT(entity_id, ' | ', country_code, ' | ', test_id)
      FROM `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_target_groups`
      WHERE CONCAT(entity_id, ' | ', country_code, ' | ', test_id) IS NOT NULL
    )
    
    AND CONCAT(a.entity_id, ' | ', a.country_code, ' | ', a.experiment_id, ' | ', a.variant) IN ( -- Filter for the right variants belonging to the experiment
      SELECT DISTINCT CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', variant) 
      FROM `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_tgs_variants_and_schemes`
      WHERE CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', variant) IS NOT NULL
    )

    AND ST_CONTAINS(zn.zone_shape, ST_GEOGPOINT(dwh.delivery_location.longitude, dwh.delivery_location.latitude)); -- Filter for orders coming from the target zones

###-------------------------------------------------------------------END OF STEP 6-----------------------------------------------------------------------###

-- Step 7: Add the profit metrics and the parent_vertical filter to the previous query because some of the fields used below had to be computed first
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_raw_orders_augmented` AS
SELECT
  *,
  -- Revenue and profit formulas
  COALESCE(
      actual_df_paid_by_customer, 
      IF(is_delivery_fee_covered_by_discount = TRUE OR is_delivery_fee_covered_by_voucher = TRUE, 0, delivery_fee_local)
  ) + commission_local + joker_vendor_fee_local + service_fee_local + COALESCE(sof_local_cdwh, sof_local) AS revenue_local,

  COALESCE(
      actual_df_paid_by_customer, 
      IF(is_delivery_fee_covered_by_discount = TRUE OR is_delivery_fee_covered_by_voucher = TRUE, 0, delivery_fee_local)
  ) + commission_local + joker_vendor_fee_local + service_fee_local + COALESCE(sof_local_cdwh, sof_local_cdwh) - delivery_costs_local AS gross_profit_local,
FROM `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_raw_orders`
WHERE TRUE -- Filter for orders from the right parent vertical (restuarants, shop, darkstores, etc.) per experiment
    AND (
      CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', vendor_vertical_parent) IN ( -- If the parent vertical exists, filter for the right one belonging to the experiment
        SELECT DISTINCT CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', first_parent_vertical)
        FROM `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_target_groups`
        WHERE CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', first_parent_vertical) IS NOT NULL
      )
      OR
      CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', vendor_vertical_parent) IN ( -- If the parent vertical exists, filter for the right one belonging to the experiment
        SELECT DISTINCT CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', second_parent_vertical)
        FROM `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_target_groups`
        WHERE CONCAT(entity_id, ' | ', country_code, ' | ', test_id, ' | ', second_parent_vertical) IS NOT NULL
      )
    );

###-------------------------------------------------------------------END OF STEP 7-----------------------------------------------------------------------###

-- Step 8: Clean the orders data by filtering for records where keep_drop_flag = 'Keep' (refer to the code above to see how this field was constructed) + add the order rank fields that depend on the variant
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_raw_orders_cleaned` AS
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY region, entity_id, test_id, variant, perseus_client_id ORDER BY order_placed_at) AS order_rank,
FROM `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_raw_orders_augmented`
WHERE TRUE
    AND keep_drop_flag = 'Keep'; -- Filter for the orders that have the correct target_group, variant, and scheme ID based on the configuration of the experiment

###-------------------------------------------------------------------END OF STEP 8-----------------------------------------------------------------------###

-- Step 9: Get the perseus client IDs of 'New' customers in the "control" and "variant" groups
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_new_customers` AS
 -- Caters to the case where the real control = No FDNC
SELECT *
FROM `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_raw_orders_cleaned`
WHERE TRUE
    AND test_name NOT IN UNNEST(test_name_combined_variants)
    AND variant = 'Control'
    AND (dps_customer_tag = 'New' AND order_rank = 1) -- Sometimes, dps_customer_tag = 'New' even after the first order, hence why we add the order_rank = 1 constraint

UNION ALL

-- Caters to the case where the real control = Single FDNC (ONLY TH_2022_04_12_MultipleFDNC_Ratchaburi_New) 
SELECT *
FROM `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_raw_orders_cleaned`
WHERE TRUE
    AND test_name IN UNNEST(test_name_combined_variants)
    AND variant = 'Control'
    AND (dps_customer_tag = 'New' AND order_rank = 1) -- Sometimes, dps_customer_tag = 'New' even after the first order, hence why we add the order_rank = 1 constraint
    -- AND actual_df_paid_by_customer = 0 -- Additional logic to reduce false positives as much as possible

UNION ALL

SELECT *
FROM `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_raw_orders_cleaned`
WHERE TRUE
    AND variant != 'Control' -- Variation(x) or Original
    AND (dps_customer_tag = 'New' AND order_rank = 1); -- Sometimes, dps_customer_tag = 'New' even after the first order, hence why we add the order_rank = 1 constraint
    -- AND actual_df_paid_by_customer = 0; -- Additional logic to reduce false positives as much as possible

###-------------------------------------------------------------------END OF STEP 9-----------------------------------------------------------------------###

-- Add the "customer_status_at_first_order", "first_order_placed_at", and "days_since_first_order" columns to `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_raw_orders_cleaned`
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_raw_orders_cleaned` AS
SELECT 
    a.*,
    b.order_placed_at AS first_order_placed_at,
    b.created_date AS first_order_created_date,
    b.dps_customer_tag AS customer_status_at_first_order,
    -- We calculate the differences between placed_at and first_order_placed_at in hours, then divide by 24 so that we can calculate the exact no. of days since the 1st order
    ROUND(DATE_DIFF(a.order_placed_at, b.order_placed_at, HOUR) / 24, 2) AS days_since_first_order
FROM `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_raw_orders_cleaned` a
LEFT JOIN `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_new_customers` b USING (region, entity_id, test_name, variant, perseus_client_id); -- These keys ensure that the column first_order_placed_at is only populated for the period where the first order was placed

###-------------------------------------------------------------------END OF STEP 10-----------------------------------------------------------------------###

-- Avg number of days after 1st order to order the nth order + re-order rates + business KPIs
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_tableau_dataset` AS
SELECT 
    -- Grouping variables
    region,
    entity_id,
    test_name,
    variant, -- Q: Do we want to amalgamate all target groups together and only analyze variation vs. control?
    perseus_client_id, 

    -- Information on the customer and time of placing the order
    order_id,
    order_placed_at,
    created_date,
    dps_customer_tag,
    customer_status_at_first_order,
    order_rank, -- Partitioned by region, entity_id, test_name, variant, and perseus_client_id
    first_order_placed_at,
    days_since_first_order, -- We calculate the differences between placed_at and first_order_placed_at in hours, then divide by 24 so that we can calculate the exact no. of days since the 1st order
    CONCAT(EXTRACT(YEAR FROM first_order_created_date), ' | ', 'CW ', EXTRACT(WEEK FROM first_order_created_date)) AS cw_year_first_order,
    EXTRACT(YEAR FROM first_order_created_date) AS year_first_order,
    EXTRACT(WEEK FROM first_order_created_date) AS cw_first_order,
    DENSE_RANK() OVER (
        PARTITION BY region, entity_id, test_name, customer_status_at_first_order, variant 
        ORDER BY EXTRACT(YEAR FROM first_order_created_date), EXTRACT(WEEK FROM first_order_created_date)
    ) AS cw_year_first_order_sorter,

    a.* EXCEPT (region, entity_id, test_name, variant, perseus_client_id, order_id, order_placed_at, created_date, dps_customer_tag, customer_status_at_first_order, order_rank, first_order_placed_at, days_since_first_order)
FROM `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_raw_orders_cleaned` a
-- Eliminates records where there is no data on the first customer's order (consequently reduces the dataset to new customers ONLY)
-- Can happen if one perseus_client_id takes part in two tests, where in one test they were a new FDNC customer (i.e., gets caught in the "ab_test_new_customers" query) and in the other, they were an existing customer)
WHERE first_order_placed_at IS NOT NULL
ORDER BY region, entity_id, test_name, customer_status_at_first_order, perseus_client_id, variant, order_rank;

##-----------------------------------------------------------------------END OF TABLEAU DATASET PART-----------------------------------------------------------------------##
