DECLARE entity_id_var STRING;
DECLARE start_date DATE;
DECLARE end_date DATE;
SET entity_id_var = "FP_TH";
SET start_date = DATE("2023-07-01");
SET end_date = DATE("2023-07-10");

WITH lbs AS (
  SELECT DISTINCT entity_id, vendor_code, "LB" AS vendor_type
  FROM `logistics-data-storage-staging.long_term_pricing.final_vendor_list_all_data_loved_brands_scaled_code`
  WHERE TRUE
    AND entity_id = entity_id_var
    AND update_timestamp = (SELECT MAX(update_timestamp) FROM `logistics-data-storage-staging.long_term_pricing.final_vendor_list_all_data_loved_brands_scaled_code`)
    AND is_lb_lm = "Y"
),

nlbs AS (
  SELECT DISTINCT entity_id, vendor_code, "NLB" AS vendor_type
  FROM `logistics-data-storage-staging.long_term_pricing.final_vendor_list_all_data_loved_brands_scaled_code`
  WHERE TRUE
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
  FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` a
  LEFT JOIN combined_vendors AS ven ON a.entity_id = ven.entity_id AND a.vendor_id = ven.vendor_code 
  WHERE TRUE 
    AND a.entity_id = entity_id_var
    AND a.created_date BETWEEN start_date AND end_date
    AND a.vendor_id IN (SELECT DISTINCT vendor_code FROM combined_vendors)
    AND a.vertical_type IN ("restaurants", "street_food", "caterers")
    AND a.city_name IS NOT NULL
    AND is_sent -- successful orders
)

SELECT
  entity_id,
  city_name,
  vendor_type,
  SUM(COUNT(DISTINCT platform_order_code)) OVER (PARTITION BY entity_id, city_name) AS total_orders,
  COUNT(DISTINCT platform_order_code) AS orders_by_vendor_group,
  SUM(dps_travel_time_fee_local) / COUNT(DISTINCT platform_order_code) AS avg_tt_fee,
  SUM(delivery_fee_local) / COUNT(DISTINCT platform_order_code) AS avg_df
FROM orders
GROUP BY 1,2,3
ORDER BY 1,4 DESC