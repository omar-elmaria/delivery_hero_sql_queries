DECLARE asa_id_var INT64;
DECLARE entity_id_var STRING;
SET asa_id_var = 5;
SET entity_id_var = "FP_KH";

WITH lbs AS (
  SELECT DISTINCT entity_id, vendor_code, "LB" AS vendor_type
  FROM `logistics-data-storage-staging.long_term_pricing.final_vendor_list_all_data_loved_brands_scaled_code`
  WHERE TRUE
    AND asa_id = asa_id_var
    AND entity_id = entity_id_var
    AND update_timestamp = (SELECT MAX(update_timestamp) FROM `logistics-data-storage-staging.long_term_pricing.final_vendor_list_all_data_loved_brands_scaled_code`)
    AND is_lb_lm = "Y"
),

nlbs AS (
  SELECT DISTINCT entity_id, vendor_code, "NLB" AS vendor_type
  FROM `logistics-data-storage-staging.long_term_pricing.final_vendor_list_all_data_loved_brands_scaled_code`
  WHERE TRUE
    AND asa_id = asa_id_var
    AND entity_id = entity_id_var
    AND update_timestamp = (SELECT MAX(update_timestamp) FROM `logistics-data-storage-staging.long_term_pricing.final_vendor_list_all_data_loved_brands_scaled_code`)
    AND is_lb_lm = "N"
),

combined_vendors AS (
  SELECT *
  FROM lbs
  UNION ALL
  SELECT *
  FROM nlbs
),

orders AS (
  SELECT
    a.*,
    ven.vendor_type
  FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders` a
  LEFT JOIN combined_vendors AS ven ON a.entity_id = ven.entity_id AND a.vendor_id = ven.vendor_code 
  WHERE TRUE 
    AND a.entity_id = entity_id_var
    AND a.created_date BETWEEN DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY), INTERVAL 14 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    AND a.vendor_id IN (SELECT DISTINCT vendor_code FROM combined_vendors)
    -- AND a.assignment_id = asa_id_var
    AND a.vertical_type IN ("restaurants", "street_food", "caterers")
    AND a.vendor_price_scheme_type IN ('Experiment', 'Automatic scheme', 'Manual') -- orders coming from experiment, asa or manual scheme assignments
    -- AND a.test_variant IN ("Control", "Original") -- orders where the variant is control or original. Forget about variant orders
    AND is_sent -- successful orders
)

SELECT
  vendor_type,
  scheme_id,
  vendor_price_scheme_type,
  test_variant,
  COUNT(DISTINCT platform_order_code) AS total_orders,
  SUM(dps_travel_time_fee_local) / COUNT(DISTINCT platform_order_code) AS avg_tt_fee,
  SUM(delivery_fee_local) / COUNT(DISTINCT platform_order_code) AS avg_df
FROM orders
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4