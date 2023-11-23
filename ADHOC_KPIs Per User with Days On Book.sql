DECLARE var_test_name ARRAY <STRING>;
DECLARE var_entity_id ARRAY <STRING>;
DECLARE start_date, end_date DATE;
SET var_test_name = ['MY_20220218_R_B0_P_LovedBrandsKlangValley']; -- Original test --> 'PH_20220420_R_B0_P_LovedBrandsMisamisOriental' with start date 19/4/2022
SET var_entity_id = ['FP_MY']; -- Name of the test in the AB test dashboard/DPS experiments tab
SET (start_date, end_date) = (DATE('2022-03-03'), CURRENT_DATE()); -- Should encompasses the entire duration of the test

##----------------------------------------------------------------------------------------------------------INPUT SECTION ENDS HERE----------------------------------------------------------------------------------------------------------##

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.raw_orders_data_for_bayesian` AS
WITH delivery_costs AS (
    SELECT
        p.entity_id,
        p.order_id, 
        o.platform_order_code,
        SUM(p.costs) AS delivery_costs_local,
        SUM(p.costs_eur) AS delivery_costs_eur
    FROM `fulfillment-dwh-production.cl.utr_timings` p
    LEFT JOIN `fulfillment-dwh-production.cl.orders` o ON p.entity_id = o.entity.id AND p.order_id = o.order_id -- Use the platform_order_code in this table as a bridge to join the order_id from utr_timings to order_id from central_dwh.orders 
    WHERE 1=1
        AND p.created_date BETWEEN start_date AND end_date -- For partitioning elimination and speeding up the query
        AND o.created_date BETWEEN start_date AND end_date -- For partitioning elimination and speeding up the query
    GROUP BY 1,2,3
),

raw_test_data AS (
  SELECT
    -- Date and time
    a.created_date,
    a.created_at,
    
    -- Location of the order
    a.region,
    a.entity_id,
    a.city_name,
    a.city_id,
    a.zone_name,
    a.zone_id,

    -- Order/customer identifiers and session data
    a.variant, -- The variant that the user was assigned to. Applies for parallel and non-parallel experiments
    b.perseus_client_id, -- The device ID of the user
    b.ga_session_id, -- The Google Analytics session ID associated with the order
    b.dps_sessionid, -- The DPS session ID associated with the order
    a.dps_customer_tag, -- A field that shows the customer status --> "New","Existing", "NULL". NULL is shown when DPS does NOT ask the CIA service for the customer status. This can happen when the vendor does NOT have the customer condition "ON"
    a.order_id, -- A unique order identifier. Can be used interchangeably with "platform_order_code"
    a.platform_order_code, -- A unique order identifier. Can be used interchangeably with "order_id"
    b.scheme_id, -- The scheme ID used to price the order
    b.vendor_price_scheme_type,	-- The assignment type of the scheme to the vendor during the time of the order, such as 'Automatic', 'Manual', 'Campaign', and 'Country Fallback'.
    
    -- Vendor and delivery info
    a.vendor_id, -- The unique identifier for the vendor from the platform backend.
    a.chain_id, -- The identifier for the chain the vendor belongs to, NULL if vendor does not belong to a chain. 
    a.chain_name, -- The name of the chain to which the vendor belongs in the local language, NULL if vendor does not belong to any chain.
    a.vertical_type, -- The vertical type of the vendor (NOT parent_vertical)
    b.vendor_vertical_parent, -- Relevant in case of parallel experiments
    b.delivery_status, -- Status of the delivery (e.g., completed)
    b.is_own_delivery, -- OD status
    b.exchange_rate, -- Exchange rate w.r.t the EUR

    -- Test info
    a.test_name, -- The name of the test as it was set up in the DPS experiments tab
    a.treatment, -- Boolean that returs true if the order is part of a treatment group and false if it isn't
    COALESCE(a.target_group, 'Non_TG') AS target_group, -- Indicates which Target Group the order belongs to.

    -- Business KPIs (Only revenue, profit, and their components are considered)
    a.delivery_fee_local, -- The delivery fee amount of the dps session coming from the cDWH. Includes travel time, surge fee, and VAT components (although in some APAC countries, VAT is now always included due to different tax treatments). Does not always reflect the **actual DF** paid by the user as there are downstream services that overwrite the prices coming from DPS. For a more accurate representation ofthe actual DF paid by the user, see the field "actual_df_paid_by_user"
    a.commission_local, -- The absolute commission associated with the order
    COALESCE(a.service_fee_local, 0) AS service_fee_local, -- The service fee amount associated with the order
    a.joker_vendor_fee_local, -- The joker vendor fee associated with the order
    IF(a.gfv_local - a.dps_minimum_order_value_local >= 0, 0, COALESCE(dwh.value.mov_customer_fee_local, (a.dps_minimum_order_value_local - a.gfv_local))) AS sof_local, -- The small order fee associated with the order
    cst.delivery_costs_local,
    cst.delivery_costs_eur,
  
    -- Special fields
    CASE
        WHEN a.region IN ('Europe', 'Asia') THEN COALESCE( -- Get the delivery fee data of Pandora countries from the "pd_orders" table
            pd.delivery_fee_local, -- This field ONLY works for Pandora countries (i.e., APAC and Europe)
            IF(b.is_delivery_fee_covered_by_discount = TRUE OR b.is_delivery_fee_covered_by_voucher = TRUE, 0, a.dps_delivery_fee_local) -- In 99.99% of the cases, the second fallback value will **never** be used. In other words, "pd_delivery_fee_local" is a reliable column
        )
        WHEN a.region NOT IN ('Europe', 'Asia') THEN (CASE WHEN b.is_delivery_fee_covered_by_voucher = FALSE AND b.is_delivery_fee_covered_by_discount = FALSE THEN a.delivery_fee_local ELSE 0 END) -- If the order comes from a non-Pandora country, use "delivery_fee_local". This usually corresponds to the DF that the user saw and based their purchase decision on
    END AS actual_df_paid_by_customer,
  FROM `fulfillment-dwh-production.cl.dps_ab_test_orders_v2` a
  INNER JOIN `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` b ON a.entity_id = b.entity_id AND a.order_id = b.order_id -- We INNER JOIN sessions_mapped_to_orders_v2 to dps_ab_test_orders_v2 because the former contains fields that are not available in the latter
  LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` dwh ON a.entity_id = dwh.global_entity_id AND a.platform_order_code = dwh.order_id -- The cDWH table. We use it to get info on the SOF
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` pd ON a.entity_id = pd.global_entity_id AND a.platform_order_code = pd.code AND a.created_date = pd.created_date_utc -- Contains info on the orders in Pandora countries 
  LEFT JOIN delivery_costs cst ON a.entity_id = cst.entity_id AND a.order_id = cst.order_id -- The table that stores the CPO
  WHERE TRUE 
    AND a.entity_id IN UNNEST(var_entity_id) -- Filter for the right entity/entities
    AND a.created_date BETWEEN start_date AND end_date -- Filter for the right time frame
    AND a.test_name IN UNNEST(var_test_name) -- Filter for the right test name(s)
    AND (a.variant ='Control' OR STARTS_WITH(a.variant, 'Variation')) -- Filter for Control and Variation(x) only
    AND b.is_own_delivery -- Filter only for OD orders
    AND b.delivery_status = 'completed' -- Successful orders
    AND a.order_id IS NOT NULL -- Gets rid of GA sessions that were not mapped to DPS sessions and thus have 'NULL' variant
)


SELECT 
  *,
  -- Revenue
  -- actual_df_paid_by_customer + commission_local + joker_vendor_fee_local + service_fee_local + sof_local AS revenue_local, -- This is the full revenue formula. We comment it out now until we include sof_local and actual_df_paid_by_customer in the BI pipeline
  -- (actual_df_paid_by_customer + commission_local + joker_vendor_fee_local + service_fee_local + sof_local) / exchange_rate AS revenue_eur, -- This is the full revenue formula. We comment it out now until we include sof_local and actual_df_paid_by_customer in the BI pipeline
  delivery_fee_local + commission_local + joker_vendor_fee_local + service_fee_local AS revenue_local,
  (delivery_fee_local + commission_local + joker_vendor_fee_local + service_fee_local) / exchange_rate AS revenue_eur,

  -- Profit
  -- actual_df_paid_by_customer + commission_local + joker_vendor_fee_local + service_fee_local + sof_local - delivery_costs_local AS gross_profit_local, -- This is the full profit formula. We comment it out now until we include sof_local and actual_df_paid_by_customer in the BI pipeline
  -- (actual_df_paid_by_customer + commission_local + joker_vendor_fee_local + service_fee_local + sof_local - delivery_costs_local - delivery_costs_local) / exchange_rate AS gross_profit_eur, -- This is the full profit formula. We comment it out now until we include sof_local and actual_df_paid_by_customer in the BI pipeline
  delivery_fee_local + commission_local + joker_vendor_fee_local + service_fee_local - delivery_costs_local AS gross_profit_local, -- This is the full profit formula. We comment it out now until we include sof_local and actual_df_paid_by_customer in the BI pipeline
  (delivery_fee_local + commission_local + joker_vendor_fee_local + service_fee_local - delivery_costs_local - delivery_costs_local) / exchange_rate AS gross_profit_eur,
FROM raw_test_data a
ORDER BY entity_id, test_name, created_date, target_group, variant;

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.users_per_test` AS -- Filter for all users under a specific test
  SELECT
  s.entity_id,
  s.test_name,
  variant, -- NEW field (please use for non-parallel experiments ONLY until BI implements the logic change in the sessions table)
  s.perseus_client_id,
  COALESCE(s.test_end_date, t.last_updated) last_date,
  COALESCE(s.target_group, 'Non_TG') AS target_group,
  MIN(s.created_at) min_created_at,
FROM `fulfillment-dwh-production.cl._dps_cvr_ab_tests_treatment` s
LEFT JOIN `fulfillment-dwh-production.curated_data_shared.table_last_updated` t ON table_name = 'dps_experiment_setups'
WHERE TRUE
  AND s.entity_id IN (SELECT DISTINCT entity_id FROM `dh-logistics-product-ops.pricing.raw_orders_data_for_bayesian`)
  AND s.test_name IN (SELECT DISTINCT test_name FROM `dh-logistics-product-ops.pricing.raw_orders_data_for_bayesian`)
  AND s.created_date BETWEEN start_date AND end_date -- Filter for the right time frame
  AND (s.variant ='Control' OR STARTS_WITH(s.variant, 'Variation'))
GROUP BY 1,2,3,4,5,6;

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.kpis_per_user` AS
SELECT
  s.entity_id,
  s.test_name,
  s.perseus_client_id,
  s.variant, -- NEW field
  s.target_group,
  date_diff(last_date, min_created_at, day) days_on_book,
  COALESCE(COUNT(DISTINCT m.order_id), 0) AS order_count_per_user,
  COALESCE(ROUND(SUM(m.revenue_local), 2), 0) AS revenue_per_user,
  COALESCE(ROUND(SUM(m.gross_profit_local), 2), 0) AS profit_per_user,
FROM `dh-logistics-product-ops.pricing.users_per_test` s
LEFT JOIN `dh-logistics-product-ops.pricing.raw_orders_data_for_bayesian` m USING(entity_id, test_name, target_group, variant, perseus_client_id) -- NEW field (added "variant" as a join key)
GROUP BY 1,2,3,4,5,6
QUALIFY DENSE_RANK() OVER (PARTITION BY entity_id, test_name, perseus_client_id ORDER BY variant) = 1 -- Eliminate users with more than one variant (< 0.1% of cases - This is simply data cleaning)
ORDER BY 1,2,3,4,5,6;