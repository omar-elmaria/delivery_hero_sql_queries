CREATE OR REPLACE TABLE  `dh-logistics-product-ops.pricing.ph_orders_acid_vs_aas_analysis` AS
SELECT
  a.entity_id,
  a.adjust_id,
  a.perseus_id,
  a.created_timestamp,
  a.customer_account_id,
  a.created_date,
  a.order_placed_at,
  a.order_id,
  a.dps_customer_tag,
  o.analytical_customer_id,
  a.is_acquisition, -- True if the order is the first order of a customer, per analytical_customer_id.
  a.customer_order_rank, -- A standard column in "curated_data_shared_central_dwh.orders". It considers MP and OD, goes back to 2011 and shows Nth order by the customer on the platform. 
  a.vendor_id,
  a.platform_order_code_dps,
  CASE WHEN a.dps_customer_tag = 'New' AND a.is_acquisition = FALSE THEN 'Mismatch' ELSE 'Match' END AS mismatch_flag,

  -- FDNC parameters
  a.days_since_first_order_cust_acc_id,
  a.days_since_first_order_adjust_id,
  a.days_since_first_order_perseus_id,
  
  a.manual_order_rank_adjust, -- Order rank per adjust_id starting from 2019-10-11
  a.manual_order_rank_perseus, -- Order rank per perseus_id starting from 2019-10-11
  a.manual_order_rank_short, -- Order rank per customer_account_id starting from 2019-10-11 
  a.manual_order_rank_long -- Order rank per customer_account_id starting from 2011-02-02
FROM `dh-logistics-product-ops.pricing.new_cust_identification_false_positive_analysis_no_data_loss_all_pandora_with_adjust_and_perseus_multiple_fdnc` a
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` o ON a.entity_id = o.global_entity_id AND a.platform_order_code_dps = o.order_id
WHERE a.entity_id = 'FP_PH' AND a.created_date > DATE('2022-05-20') AND a.dps_customer_tag = 'New'
ORDER BY a.created_date;

SELECT -- From the 21st of May until the 19th of June
  COUNT(DISTINCT platform_order_code_dps) AS total_orders,
  COUNT(DISTINCT CASE WHEN dps_customer_tag = 'New' AND is_acquisition = FALSE THEN platform_order_code_dps ELSE NULL END) AS orders_aas_acid_mismatch,
  COUNT(DISTINCT CASE WHEN dps_customer_tag = 'New' AND is_acquisition = True THEN platform_order_code_dps ELSE NULL END) AS orders_aas_acid_match,
  ROUND(COUNT(DISTINCT CASE WHEN dps_customer_tag = 'New' AND is_acquisition = FALSE THEN platform_order_code_dps ELSE NULL END) / COUNT(DISTINCT platform_order_code_dps), 4) AS orders_aas_acid_mismatch_pct,
FROM `dh-logistics-product-ops.pricing.ph_orders_acid_vs_aas_analysis`