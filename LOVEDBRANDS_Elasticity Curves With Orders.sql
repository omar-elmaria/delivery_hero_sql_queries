-- Why we use CVR and not orders?
-- 1. We have more sessions than orders, so it is less likely we face problems with 0 orders in one of the tiers (base or intermediate)
-- 2. We don't face a problem with calculating elasticity when we have one of the tiers having a 0 TT fee
-- 3. Low orders in the base tier could inflate the elasticity numbers in subsequent tiers by a lot
-- Note: We get better looking curves when we calculate the elasticity differences between each tier and the subsequent one

-- Potential improvements to current logic?
-- 1. Fit a line through the elasticity curves
-- 2. Treat the case when the pct change is infinity (transactions of the base tier = 0) or -100% (transactions in a specific tier = 0) + when sessions are 0

-- Step 1: Get the DF tiers of each ASA and join the vendors per ASA to that table
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.vendors_and_fees_per_asa_order_elasticity_analysis` AS
WITH join_vendors_and_fees AS (
  SELECT 
    a.* EXCEPT(fee),
    b.is_asa_clustered,
    b.vendor_count_caught_by_asa,
    b.vendor_code,
    a.fee,
    MIN(a.fee) OVER (PARTITION BY a.entity_id, a.country_code, a.master_asa_id) AS min_tt_fee_asa_level,
    MIN(a.fee) OVER (PARTITION BY a.entity_id, a.country_code, a.master_asa_id, b.vendor_code) AS min_tt_fee_vendor_level
  FROM `dh-logistics-product-ops.pricing.df_tiers_per_asa_loved_brands_scaled_code` a -- This table contains the DF tiers of each ASA
  LEFT JOIN `dh-logistics-product-ops.pricing.vendor_ids_per_asa_loved_brands_scaled_code` b -- This table contains the vendor IDs per ASA
    ON TRUE
    AND a.entity_id = b.entity_id
    AND a.country_code = b.country_code
    AND a.master_asa_id = b.master_asa_id
)

SELECT 
  a.*,
  CASE WHEN c.vendor_code IS NOT NULL THEN 'Top 25%' ELSE 'Bottom 75%' END AS vendor_rank,
  COALESCE(COUNT(DISTINCT platform_order_code), 0) AS num_orders,
FROM join_vendors_and_fees a
LEFT JOIN `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` b
  ON TRUE
    AND a.entity_id = b.entity_id
    AND a.country_code = b.country_code
    AND a.vendor_code = b.vendor_id
    AND a.fee = b.dps_travel_time_fee_local
    AND b.created_date BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AND LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
    AND b.delivery_status = "completed" AND b.is_sent -- Completed AND Successful order
LEFT JOIN `dh-logistics-product-ops.pricing.all_metrics_after_session_order_cvr_filters_loved_brands_scaled_code` c -- Used the the vendor rank (Top 25% or bottom 75%)
  ON TRUE
    AND a.entity_id = c.entity_id
    AND a.country_code = c.country_code
    AND a.vendor_code = c.vendor_code
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
ORDER BY a.entity_id, a.master_asa_id, a.vendor_code, a.fee;

-- Step 2: Calculate the elasticity on the ASA level

-- Step 2.1: Get the ASA level data
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.asa_level_data_order_elasticity_analysis` AS
WITH asa_level_data AS (
  SELECT 
    region,
    entity_id,
    country_code,
    asa_id,
    master_asa_id,
    asa_name,
    asa_common_name,
    is_asa_clustered,
    vendor_count_caught_by_asa,
    fee,
    AVG(min_tt_fee_asa_level) AS min_tt_fee_asa_level,
    SUM(num_orders) AS order_count_per_tt_fee_asa_level	
  FROM `dh-logistics-product-ops.pricing.vendors_and_fees_per_asa_order_elasticity_analysis`
  WHERE vendor_rank = 'Top 25%'
  GROUP BY 1,2,3,4,5,6,7,8,9,10
),

add_min_order_count AS (
  SELECT 
    a.*,
    SUM(a.order_count_per_tt_fee_asa_level) OVER (PARTITION BY a.entity_id, a.master_asa_id) AS total_orders_asa_level,
    SUM(a.order_count_per_tt_fee_asa_level) OVER (PARTITION BY a.entity_id, a.master_asa_id) / SUM(a.order_count_per_tt_fee_asa_level) OVER (PARTITION BY a.entity_id) AS asa_order_share_of_entity,
    b.order_count_per_tt_fee_asa_level AS min_order_count_asa_level,
    LAG(a.fee) OVER (PARTITION BY a.entity_id, a.master_asa_id ORDER BY a.fee) AS previous_fee,
    LAG(a.order_count_per_tt_fee_asa_level) OVER (PARTITION BY a.entity_id, a.master_asa_id ORDER BY a.fee) AS previous_fee_order_count_asa_level
  FROM asa_level_data a
  INNER JOIN asa_level_data b ON a.entity_id = b.entity_id AND a.master_asa_id = b.master_asa_id AND b.fee = b.min_tt_fee_asa_level
)

SELECT
  *,
  CASE 
    WHEN fee = min_tt_fee_asa_level THEN NULL
    ELSE ROUND((order_count_per_tt_fee_asa_level / NULLIF(previous_fee_order_count_asa_level, 0) - 1) / (fee / NULLIF(previous_fee, 0) - 1), 4)
  END AS elasticity_previous_tier_asa_level,
  
  CASE 
    WHEN fee = min_tt_fee_asa_level THEN NULL
    WHEN (min_order_count_asa_level = 0 OR order_count_per_tt_fee_asa_level = 0) THEN NULL -- In these two cases, the elasticity would be infinity or -1
    ELSE ROUND((order_count_per_tt_fee_asa_level / min_order_count_asa_level - 1) / (fee / NULLIF(min_tt_fee_asa_level, 0) - 1), 4)
  END AS elasticity_base_tier_asa_level
FROM add_min_order_count
ORDER BY entity_id, master_asa_id, fee;
