-- Declare some input variables
DECLARE start_date DATE;
SET start_date = DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);

-- Select countries where DPS subscriptions are active
CREATE OR REPLACE TABLE `logistics-data-storage-staging.temp_pricing.dps_subscription_incentive_investigation_agg_stats` AS
WITH entities_dps_sub AS (
  SELECT region, entity_id, COUNT(DISTINCT CASE WHEN has_subscription_discount = TRUE THEN platform_order_code END) AS order_count_with_sub
  FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`
  WHERE created_date >= start_date AND region IS NOT NULL
  GROUP BY 1,2
  HAVING order_count_with_sub >= 2000 -- Filter out any entity that has lower than 2000 subscription orders
),

order_data AS (
  SELECT
    region,
    entity_id,
    created_date,
    platform_order_code,
    dps_sessionid,
    dps_sessionid_created_at,
    vendor_id,
    dps_customer_tag,
    with_customer_condition,
    condition_id AS time_or_new_cust_condition_id,
    perseus_client_id,
    delivery_fee_local,
    dps_delivery_fee_local,
    dps_travel_time_fee_local,
    dps_surge_fee_local,
    basket_value_current_fee_value,
    dps_incentive_discount_local,
    dps_discount,
    is_df_discount_basket_value_deal,  	
    dps_standard_fee_local,
    dps_last_non_zero_df_local,

    scheme_id,
    vendor_price_scheme_type,
    assignment_id,
    has_subscription,
    has_subscription_discount,
    subscription_discount_type,
    components.travel_time_fee_id,
    components.basket_value_fee_id,
    partial_assignments.subscription_id,
  FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`
  WHERE TRUE
    AND entity_id IN (SELECT DISTINCT entity_id FROM entities_dps_sub)
    AND created_date >= start_date
    AND has_subscription_discount = TRUE -- Filter for orders that have a full or partial subscription discount
),

agg_stats AS (
  SELECT
    ent.segment AS region,
    ord.entity_id,
    ord.created_date,
    COUNT(DISTINCT platform_order_code) AS subscription_order_count,
    COUNT(DISTINCT CASE WHEN basket_value_current_fee_value != dps_incentive_discount_local THEN platform_order_code END) order_count_mismatch_bvcf_inc_disc,
    COUNT(DISTINCT CASE WHEN dps_standard_fee_local != dps_travel_time_fee_local THEN platform_order_code END) AS order_count_mismatch_tt_std_fee,
    COUNT(DISTINCT CASE WHEN (basket_value_current_fee_value != dps_incentive_discount_local) AND (dps_standard_fee_local != dps_travel_time_fee_local) THEN platform_order_code END) order_count_mismatch_both,
  FROM order_data ord
  LEFT JOIN `fulfillment-dwh-production.curated_data_shared_coredata.global_entities` ent ON ent.global_entity_id = ord.entity_id
  GROUP BY 1,2,3
)

SELECT
  *,
  ROUND(order_count_mismatch_bvcf_inc_disc / subscription_order_count, 4) AS order_count_mismatch_pct_bvcf_inc_disc,
  ROUND(order_count_mismatch_tt_std_fee / subscription_order_count, 4) AS order_count_mismatch_pct_tt_std_fee,
  ROUND(order_count_mismatch_both / subscription_order_count, 4) AS order_count_mismatch_pct_both,
FROM agg_stats
ORDER BY 1,2,3;

-- Create a list of problematic orders
CREATE OR REPLACE TABLE `logistics-data-storage-staging.temp_pricing.dps_subscription_incentive_investigation_problematic_orders` AS
WITH entities_dps_sub AS (
  SELECT region, entity_id, COUNT(DISTINCT CASE WHEN has_subscription_discount = TRUE THEN platform_order_code END) AS order_count_with_sub
  FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`
  WHERE created_date >= start_date AND region IS NOT NULL
  GROUP BY 1,2
  HAVING order_count_with_sub >= 2000 -- Filter out any entity that has lower than 2000 subscription orders
)

SELECT
  CASE
    WHEN (basket_value_current_fee_value != dps_incentive_discount_local) AND (dps_standard_fee_local != dps_travel_time_fee_local) THEN "both"
    WHEN (basket_value_current_fee_value != dps_incentive_discount_local) THEN "bvcf_inc_discount"
    WHEN (dps_standard_fee_local != dps_travel_time_fee_local) THEN "std_tt_fee"
  END AS problem_flag,
  region,
  entity_id,
  created_date,
  platform_order_code,
  dps_sessionid,
  dps_sessionid_created_at,
  vendor_id,
  dps_customer_tag,
  with_customer_condition,
  condition_id AS time_or_new_cust_condition_id,
  perseus_client_id,
  delivery_fee_local,
  dps_travel_time,
  dps_delivery_fee_local,
  dps_travel_time_fee_local,
  dps_surge_fee_local,
  basket_value_current_fee_value,
  dps_incentive_discount_local,
  dps_discount,
  is_df_discount_basket_value_deal,  	
  dps_standard_fee_local,
  dps_last_non_zero_df_local,

  scheme_id,
  vendor_price_scheme_type,
  assignment_id,
  has_subscription,
  has_subscription_discount,
  subscription_discount_type,
  components.travel_time_fee_id,
  components.basket_value_fee_id,
  partial_assignments.subscription_id,
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`
WHERE TRUE
  AND created_date >= start_date
  AND has_subscription_discount = TRUE -- Filter for orders that have a full or partial subscription discount
  AND entity_id IN (SELECT DISTINCT entity_id FROM entities_dps_sub)
  AND ((basket_value_current_fee_value != dps_incentive_discount_local) OR (dps_standard_fee_local != dps_travel_time_fee_local))
  AND order_id IS NOT NULL
