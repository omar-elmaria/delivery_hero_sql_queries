-- Inputs section
DECLARE entity_id_var STRING;
DECLARE asa_id_var INT64;
DECLARE vertical_type_var ARRAY <STRING>;
DECLARE start_date DATE;
DECLARE end_date DATE;
DECLARE min_travel_time INT64;
DECLARE max_travel_time INT64;
DECLARE included_variants ARRAY <STRING>;
DECLARE included_assignment_types ARRAY <STRING>;
DECLARE scheme_id_var INT64;
DECLARE is_lb_lm_var ARRAY <STRING>;
SET entity_id_var = 'FP_TH';
SET asa_id_var = 1098;
SET vertical_type_var = ['restaurants'];
SET start_date = DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY);
SET end_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
SET min_travel_time = 0;
SET max_travel_time = 100;
SET included_variants = ['Original', 'Control'];
SET included_assignment_types = ['Experiment', 'Automatic scheme', 'Manual'];
SET scheme_id_var = 3121;
SET is_lb_lm_var = ["Y", "N"];

-- Get the vendor IDs
WITH selected_vendors AS ( -- All vendors (LBs and non-LBs)
  SELECT DISTINCT entity_id, vendor_code
  FROM `dh-logistics-product-ops.pricing.final_vendor_list_all_data_loved_brands_scaled_code` a
  LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.vendors` b ON a.entity_id = b.global_entity_id AND a.vendor_code = b.vendor_id
  WHERE TRUE
    AND entity_id = entity_id_var 
    AND asa_id = asa_id_var 
    AND update_timestamp = (SELECT MAX(update_timestamp) FROM `dh-logistics-product-ops.pricing.final_vendor_list_all_data_loved_brands_scaled_code`)
    AND b.vertical_type IN UNNEST(vertical_type_var)
    AND is_lb_lm IN UNNEST(is_lb_lm_var)
),

-- This is step 1/4 to generate this table --> `dh-logistics-product-ops.pricing.df_tiers_per_price_scheme_loved_brands_scaled_code`
asa_setups AS (
  SELECT DISTINCT
    ent.region,
    asa.entity_id,
    asa.country_code,

    asa.asa_id,
    asa.asa_name,
    asa.priority AS asa_priority,
    TIMESTAMP_TRUNC(asa.active_from, SECOND) AS asa_setup_active_from,
    MAX(TIMESTAMP_TRUNC(asa.active_from, SECOND)) OVER (PARTITION BY asa.entity_id, asa.country_code, asa.asa_id) AS asa_setup_active_from_max_date,
    TIMESTAMP_TRUNC(asa.active_to, SECOND) AS asa_setup_active_to,

    vendor_code,
    pc.scheme_id,
    asa.vendor_group_id AS vendor_group_price_config_id,
    pc.priority AS condition_priority,
    pc.schedule_id AS time_condition_id,
    pc.customer_condition_id,
    assigned_vendors_count AS vendor_count_caught_by_asa,
    n_schemes
  FROM `dh-logistics-product-ops.pricing.dps_asa_full_config_versions` AS asa
  INNER JOIN `dh-logistics-product-ops.pricing.active_entities_loved_brands_scaled_code` AS ent ON asa.entity_id = ent.entity_id -- Filter only for active DH entities
  LEFT JOIN UNNEST(sorted_assigned_vendor_ids) AS vendor_code
  LEFT JOIN UNNEST(asa.asa_price_config) AS pc
  WHERE asa.active_to IS NULL AND asa.priority > 0 -- Get the most up-to-date ASA assignment setup
),

-- This is step 2/4 to generate this table --> `dh-logistics-product-ops.pricing.df_tiers_per_price_scheme_loved_brands_scaled_code`
scheme_ids_per_asa AS (
  SELECT DISTINCT
    region,
    entity_id,
    country_code,

    asa_id,
    asa_name,
    vendor_count_caught_by_asa,
    scheme_id,
    CASE WHEN time_condition_id IS NULL AND customer_condition_id IS NULL THEN "Main Scheme" ELSE "Condition Scheme" END AS scheme_type
  FROM asa_setups
),

-- This is step 3/4 to generate this table --> `dh-logistics-product-ops.pricing.df_tiers_per_price_scheme_loved_brands_scaled_code`
scheme_config_stg AS (
  SELECT DISTINCT
    pcv.entity_id,
    pcv.country_code,
    pcv.scheme_id,
    pcv.scheme_name,
    TIMESTAMP_TRUNC(pcv.scheme_active_from, SECOND) AS scheme_active_from,
    TIMESTAMP_TRUNC(pcv.scheme_active_to, SECOND) AS scheme_active_to,
    pcv.scheme_component_ids.travel_time_config_id,
    COALESCE(ttc.travel_time_threshold, 9999999) AS tt_threshold,
    CASE
      WHEN ttc.travel_time_threshold IS NULL THEN 9999999
      ELSE ROUND(FLOOR(ttc.travel_time_threshold) + (ttc.travel_time_threshold - FLOOR(ttc.travel_time_threshold)) * 60 / 100, 2)
    END AS threshold_in_min_and_sec,
    ttc.travel_time_fee AS fee,
  FROM `fulfillment-dwh-production.cl.pricing_configuration_versions` pcv
  LEFT JOIN UNNEST(scheme_component_configs.travel_time_config) ttc
  WHERE scheme_active_to IS NULL
    AND CONCAT(pcv.entity_id, " | ", pcv.scheme_id) IN (
      SELECT DISTINCT CONCAT(entity_id, " | ", scheme_id) AS entity_country_scheme
      FROM scheme_ids_per_asa
    )
),

-- This is step 4/4 to generate this table --> `dh-logistics-product-ops.pricing.df_tiers_per_price_scheme_loved_brands_scaled_code`
df_tiers_per_price_scheme AS (
  SELECT 
    b.region,
    a.entity_id,
    a.country_code,
    -- Keep in mind that one scheme could be included in more than one ASA, so joining "dps_config_versions" on the "scheme_ids_per_asa_loved_brands_scaled_code" will produce duplicates and this is expected
    b.asa_id,
    b.asa_name,
    a.* EXCEPT (entity_id, country_code),
    RANK() OVER (PARTITION BY a.entity_id, a.country_code, b.asa_id, a.scheme_id, a.travel_time_config_id ORDER BY a.tt_threshold) AS tier
  FROM scheme_config_stg AS a
  LEFT JOIN scheme_ids_per_asa AS b
    ON TRUE
      AND a.entity_id = b.entity_id
      AND a.country_code = b.country_code
      AND a.scheme_id = b.scheme_id
),

scheme_config AS (
  SELECT *
  FROM df_tiers_per_price_scheme
  WHERE entity_id = entity_id_var AND asa_id = asa_id_var AND scheme_id = scheme_id_var
),

order_data AS (
  SELECT
    created_date,
    platform_order_code,
    dps.travel_time,
    con.tier,
    dps.dps_travel_time_fee_local,
    dps.delivery_fee_local,
    dps.gfv_local,
    dps.gmv_local,
    dps.commission_local,
    dps.joker_vendor_fee_local,
    dps.mov_customer_fee_local,
    dps.service_fee_local,
    
    (CASE WHEN dps.is_delivery_fee_covered_by_discount = TRUE OR dps.is_delivery_fee_covered_by_voucher = TRUE THEN 0 ELSE dps.delivery_fee_local END)
    + dps.commission_local + dps.joker_vendor_fee_local + dps.service_fee_local + dps.mov_customer_fee_local AS revenue_local,

    dps.delivery_costs_local,

    (CASE WHEN dps.is_delivery_fee_covered_by_discount = TRUE OR dps.is_delivery_fee_covered_by_voucher = TRUE THEN 0 ELSE dps.delivery_fee_local END) 
    + dps.commission_local + dps.joker_vendor_fee_local + dps.service_fee_local + dps.mov_customer_fee_local - dps.delivery_costs_local AS gross_profit_local,

    PERCENT_RANK() OVER (ORDER BY travel_time ASC) * 100 tt_percentile,
  FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` dps
  INNER JOIN selected_vendors v ON dps.entity_id = v.entity_id AND dps.vendor_id = v.vendor_code
  LEFT JOIN scheme_config con ON dps.entity_id = con.entity_id AND dps.dps_travel_time_fee_local = con.fee
  WHERE TRUE 
      -- period
      AND dps.created_date BETWEEN start_date AND end_date
      -- travel time range
      AND dps.travel_time > min_travel_time
      AND dps.travel_time < max_travel_time
      -- variants
      AND dps.variant IN UNNEST(included_variants)
      -- assignment_type
      AND dps.vendor_price_scheme_type IN UNNEST(included_assignment_types)
      AND dps.dps_travel_time_fee_local IN (SELECT DISTINCT fee FROM scheme_config)
)

SELECT
  tier,
  dps_travel_time_fee_local,
  ROUND(SUM(gross_profit_local) / COUNT(*), 2) AS avg_gross_profit_local,
  ROUND(SUM(delivery_fee_local) / COUNT(*), 2) AS avg_df_local,
  COUNT(*) AS order_count,
  SUM(COUNT(*)) OVER (ORDER BY tier) AS cum_sum_order_count,
  SUM(COUNT(*)) OVER () AS total_orders,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS order_share,
  ROUND(SUM(COUNT(*)) OVER (ORDER BY tier) / SUM(COUNT(*)) OVER (), 4) AS cum_order_share
FROM order_data
GROUP BY 1,2
ORDER BY 2