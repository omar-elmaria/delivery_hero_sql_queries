-- Order performance query

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.full_setup_lb_performance_tracking` AS
-- Get the names of the LB ASAs
WITH lb_asas AS (
SELECT DISTINCT
  ent.region,
  asa.entity_id,
  asa.country_code,
  asa.asa_name
FROM `dh-logistics-product-ops.pricing.dps_asa_full_config_versions` AS asa
INNER JOIN `dh-logistics-product-ops.pricing.active_entities_loved_brands_scaled_code` AS ent ON asa.entity_id = ent.entity_id -- Filter only for active DH entities
LEFT JOIN UNNEST(sorted_assigned_vendor_ids) AS vendor_code
LEFT JOIN UNNEST(asa.asa_price_config) AS pc
WHERE asa.active_to IS NULL AND asa.priority > 0 AND asa_name LIKE "%_LB" -- Get the most up-to-date LB ASAs
),

-- Get the names of the non-LB ASAs
non_lb_asas AS (
  SELECT
    region,
    entity_id,
    country_code,
    TRIM(ARRAY_TO_STRING(SPLIT(asa_name, "_LB"), '')) AS non_lb_asa
  FROM lb_asas
),

-- Union the names of the LB and non-LB ASAs
union_lb_non_lb_asas AS (
  SELECT *
  FROM lb_asas
  UNION ALL
  SELECT *
  FROM non_lb_asas
)

-- Extract the full setup of the LB and non-LB ASAs
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
WHERE asa.priority > 0 AND asa.asa_name IN (SELECT asa_name FROM union_lb_non_lb_asas)
ORDER BY ent.region, asa.entity_id, asa.country_code, asa.asa_id, asa.priority, vendor_code, pc.priority, pc.scheme_id
;

###--------------------------------------------------------------------------------------------------------------------###

-- Get the dates when the LB ASAs were first configured
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.lb_asas_dates_of_first_config_lb_performance_tracking` AS
SELECT DISTINCT
  fl.entity_id,
  fl.asa_name,
  fl.asa_id,
  ven.asa_common_name,
  DATE(MIN(asa_setup_active_from)) AS asa_first_active_date
FROM `dh-logistics-product-ops.pricing.full_setup_lb_performance_tracking` AS fl
LEFT JOIN (
  SELECT *
  FROM `dh-logistics-product-ops.pricing.final_vendor_list_all_data_loved_brands_scaled_code`
  WHERE update_timestamp = (SELECT MAX(update_timestamp) FROM `dh-logistics-product-ops.pricing.final_vendor_list_all_data_loved_brands_scaled_code`)
) AS ven ON fl.entity_id = ven.entity_id AND fl.asa_id = ven.asa_id
WHERE fl.asa_name LIKE "%_LB"
GROUP BY 1,2,3,4
ORDER BY 1,2,3
;

###--------------------------------------------------------------------------------------------------------------------###

-- Get the ASA names and IDs of the ASAs that should be tracked
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.orders_lb_performance_tracking` AS
WITH asas_to_track AS (
  SELECT DISTINCT
    fl.region,
    fl.entity_id,
    fl.country_code,
    fl.asa_id,
    fl.asa_name,
    ven.asa_common_name,
    fst.asa_first_active_date
  FROM `dh-logistics-product-ops.pricing.full_setup_lb_performance_tracking` fl
  LEFT JOIN (
      SELECT *
      FROM `dh-logistics-product-ops.pricing.final_vendor_list_all_data_loved_brands_scaled_code`
      WHERE update_timestamp = (SELECT MAX(update_timestamp) FROM `dh-logistics-product-ops.pricing.final_vendor_list_all_data_loved_brands_scaled_code`)
   ) AS ven ON fl.entity_id = ven.entity_id AND fl.asa_id = ven.asa_id
  LEFT JOIN `dh-logistics-product-ops.pricing.lb_asas_dates_of_first_config_lb_performance_tracking` fst ON fl.entity_id = fst.entity_id AND ven.asa_common_name = fst.asa_common_name
  WHERE asa_setup_active_to IS NULL -- The most up-to-date ASA setups
),

orders AS (
  SELECT 
      -- Identifiers and supplementary fields     
      -- Date and time
      a.created_date AS created_date_utc,
      a.order_placed_at AS order_placed_at_utc,
      FORMAT_DATE("%A", DATE(order_placed_at)) AS dow,

      asas.asa_first_active_date,
      DATE_DIFF(DATE(a.created_date), DATE(asas.asa_first_active_date), DAY) AS days_since_lb_activation,
      a.assignment_id AS asa_id,
      asas.asa_name,
      asas.asa_common_name,

      -- Location of order
      ent.region,
      a.entity_id,
      a.country_code,
      a.city_name,
      a.city_id,
      a.zone_name,
      a.zone_id,

      -- Order/customer identifiers and session data
      a.dps_customer_tag,
      a.platform_order_code,
      a.scheme_id,

      -- Vendor data and information on the delivery
      a.vendor_id,
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
      a.delivery_fee_local,
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
      END AS actual_df_paid_by_customer_local,
      a.gfv_local,
      a.gmv_local
  FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` a
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` pd -- Contains info on the orders in Pandora countries
    ON TRUE 
      AND a.entity_id = pd.global_entity_id
      AND a.platform_order_code = pd.code 
      AND a.created_date = pd.created_date_utc -- There is no country_code field in this table
  LEFT JOIN `tlb-data-prod.data_platform.fct_billing` mn ON a.platform_order_code = CAST(mn.order_id AS STRING) AND a.entity_id IN ("TB_OM", "TB_IQ", "TB_AE", "TB_KW", "YS_TR", "TB_QA", "TB_JO", "HF_EG", "HS_SA", "TB_BH")
  LEFT JOIN asas_to_track AS asas ON a.entity_id = asas.entity_id AND a.assignment_id = asas.asa_id
  INNER JOIN `dh-logistics-product-ops.pricing.active_entities_loved_brands_scaled_code` ent ON a.entity_id = ent.entity_id -- Get the region associated with every entity_id
  WHERE TRUE
      AND a.created_date >= DATE("2023-01-01") -- Filter for orders starting from the first of Feb 2023. That's one month before the first LB ASA was launched
      AND a.entity_id IN (SELECT DISTINCT entity_id FROM asas_to_track) -- Filter for the relevant entities
      AND vendor_price_scheme_type = "Automatic scheme" -- Filter for vendor_price_scheme_type = "Automatic scheme" so we can 
      AND CONCAT(a.entity_id, " | ", a.assignment_id) IN (SELECT CONCAT(entity_id, " | ", asa_id) FROM asas_to_track) -- Filter for the ASAs that we need to track
      AND a.is_sent -- Successful orders
),

-- Add profit to the orders table
orders_with_rev_and_gp AS (
  SELECT
    a.*,
    -- Revenue and profit formulas
    a.actual_df_paid_by_customer_local + a.commission_local + a.joker_vendor_fee_local + a.service_fee_local + a.sof_local AS revenue_local,
    a.actual_df_paid_by_customer_local + a.commission_local + a.joker_vendor_fee_local + a.service_fee_local + a.sof_local - a.delivery_costs_local AS gross_profit_local,
  FROM orders a
)

-- Add the EUR fields
SELECT
  *,
  delivery_fee_local / exchange_rate AS delivery_fee_eur,
  commission_local / exchange_rate AS commission_eur,
  joker_vendor_fee_local / exchange_rate AS joker_vendor_fee_eur,
  service_fee_local / exchange_rate AS service_fee_eur,
  sof_local / exchange_rate AS sof_eur,
  delivery_costs_local / exchange_rate AS delivery_costs_eur,
  actual_df_paid_by_customer_local / exchange_rate AS actual_df_paid_by_customer_eur,
  gfv_local / exchange_rate AS gfv_eur,
  gmv_local / exchange_rate AS gmv_eur,
  revenue_local / exchange_rate AS revenue_eur,
  gross_profit_local / exchange_rate AS gross_profit_eur,
FROM orders_with_rev_and_gp
;

###--------------------------------------------------------------------------------------------------------------------###

-- Track the LB vendor composition on the common ASA level
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.common_asa_lb_vendor_composition_lb_performance_tracking` AS
WITH lb_asa_list_in_lb_table AS (
  SELECT DISTINCT
    region,
    entity_id,
    asa_common_name,
    TRUE AS is_lb_active_for_asa
  FROM `dh-logistics-product-ops.pricing.final_vendor_list_all_data_loved_brands_scaled_code`
  WHERE TRUE
    AND update_timestamp = (SELECT MAX(update_timestamp) FROM `dh-logistics-product-ops.pricing.final_vendor_list_all_data_loved_brands_scaled_code`)
    AND asa_name LIKE "%_LB"
)

SELECT
  ent.region,
  ven.entity_id,
  ven.asa_common_name,
  COALESCE(lb.is_lb_active_for_asa, FALSE) AS is_lb_active_for_asa,
  DATE(update_timestamp) AS lb_pipeline_run_date,
  COUNT(DISTINCT CASE WHEN is_lb_lm = "Y" THEN vendor_code END) AS lb_vendor_count,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT IF(is_lb_lm = "Y", vendor_code, NULL) IGNORE NULLS ORDER BY IF(is_lb_lm = "Y", vendor_code, NULL)), ", ") AS lb_vendor_codes,
  COUNT(DISTINCT CASE WHEN is_lb_lm = "N" THEN vendor_code END) AS nlb_vendor_count,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT IF(is_lb_lm = "N", vendor_code, NULL) IGNORE NULLS ORDER BY IF(is_lb_lm = "N", vendor_code, NULL)), ", ") AS nlb_vendor_codes,
FROM `dh-logistics-product-ops.pricing.final_vendor_list_all_data_loved_brands_scaled_code` ven
LEFT JOIN lb_asa_list_in_lb_table AS lb ON ven.entity_id = lb.entity_id AND ven.asa_common_name = lb.asa_common_name
INNER JOIN `dh-logistics-product-ops.pricing.active_entities_loved_brands_scaled_code` AS ent ON ven.entity_id = ent.entity_id -- Filter only for active DH entities
WHERE DATE(update_timestamp) >= DATE("2023-01-01")
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5
;

-- Track the LB vendor composition on the entity level
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.entity_lb_vendor_composition_lb_performance_tracking` AS
SELECT
  ent.region,
  ven.entity_id,
  DATE(update_timestamp) AS lb_pipeline_run_date,
  COUNT(DISTINCT CASE WHEN is_lb_lm = "Y" THEN vendor_code END) AS lb_vendor_count,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT IF(is_lb_lm = "Y", vendor_code, NULL) IGNORE NULLS ORDER BY IF(is_lb_lm = "Y", vendor_code, NULL)), ", ") AS lb_vendor_codes,
  COUNT(DISTINCT CASE WHEN is_lb_lm = "N" THEN vendor_code END) AS nlb_vendor_count,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT IF(is_lb_lm = "N", vendor_code, NULL) IGNORE NULLS ORDER BY IF(is_lb_lm = "N", vendor_code, NULL)), ", ") AS nlb_vendor_codes,
FROM `dh-logistics-product-ops.pricing.final_vendor_list_all_data_loved_brands_scaled_code` ven
INNER JOIN `dh-logistics-product-ops.pricing.active_entities_loved_brands_scaled_code` AS ent ON ven.entity_id = ent.entity_id -- Filter only for active DH entities
WHERE DATE(update_timestamp) >= DATE("2023-01-01")
GROUP BY 1,2,3
ORDER BY 1,2,3
;