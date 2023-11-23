SELECT
  asa_id,
  asa_name,
  DATE(update_timestamp) AS date_timestamp,
  COUNT(DISTINCT CASE WHEN is_lb_lm = "Y" THEN vendor_code END) AS count_lbs,
  COUNT(DISTINCT CASE WHEN is_lb_lm = "N" THEN vendor_code END) AS count_nlbs,

  SUM(CASE WHEN is_lb_lm = "Y" THEN num_orders ELSE 0 END) AS order_count_lbs,
  SUM(CASE WHEN is_lb_lm = "N" THEN num_orders ELSE 0 END) AS order_count_nlbs,
  ROUND(SUM(CASE WHEN is_lb_lm = "Y" THEN num_orders ELSE 0 END) / SUM(num_orders), 3) AS order_share_lbs,
  ROUND(SUM(CASE WHEN is_lb_lm = "N" THEN num_orders ELSE 0 END) / SUM(num_orders), 3) AS order_share_nlbs,
FROM `dh-logistics-product-ops.pricing.final_vendor_list_all_data_loved_brands_scaled_code`
WHERE DATE(update_timestamp) >= DATE("2023-04-01") AND entity_id = "FP_TH" AND asa_id IN (438, 795, 1098)
GROUP BY 1,2,3
ORDER BY 1,2,3