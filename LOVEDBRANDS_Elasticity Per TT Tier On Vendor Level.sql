-- Step 1: Calculate the elasticity per vendor
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.vendors_and_fees_per_asa_order_loved_brands_pairwise_simulation` AS
WITH tt_tiers_and_orders_per_vendor AS (
  SELECT
    a.region,
    a.entity_id,
    a.country_code,
    a.master_asa_id,
    a.asa_common_name,
    c.asa_id,
    c.asa_name,
    a.* EXCEPT(num_transactions, df_total, min_df_total_of_vendor, region, entity_id, country_code, master_asa_id, asa_common_name, tier_rank_vendor, num_tiers_vendor),
    a.num_tiers_vendor,
    a.tier_rank_vendor,
    a.df_total,
    a.min_df_total_of_vendor,
    LAG(a.df_total) OVER (PARTITION BY a.entity_id, c.asa_id, a.vendor_code ORDER BY a.df_total) AS previous_fee,
    COALESCE(COUNT(DISTINCT b.platform_order_code), 0) AS num_orders,
  FROM `dh-logistics-product-ops.pricing.cvr_per_df_bucket_vendor_level_loved_brands_scaled_code` a
  LEFT JOIN `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` b
    ON TRUE
      AND a.entity_id = b.entity_id
      AND a.country_code = b.country_code
      AND a.vendor_code = b.vendor_id
      AND a.df_total = b.dps_travel_time_fee_local
      AND b.created_date BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AND LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
      AND b.delivery_status = "completed" AND b.is_sent -- Completed AND Successful order
  LEFT JOIN `dh-logistics-product-ops.pricing.vendor_ids_per_asa_loved_brands_scaled_code` c ON a.entity_id = c.entity_id AND a.country_code = c.country_code AND a.vendor_code = c.vendor_code
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
),

add_previous_order_count AS (
  SELECT
    *,
    LAG(num_orders) OVER (PARTITION BY entity_id, asa_id, vendor_code ORDER BY df_total) AS previous_order_count
  FROM tt_tiers_and_orders_per_vendor
)

SELECT
  *,
  -- Case 1: Calculating elasticity w.r.t 0 DF would produce a value of infinity
  -- Case 2: Calculating elasticity with 1 TT tier is not possible
  -- Case 3: Calculating elasticity for the first travel time tier is not possible
  CASE
    WHEN previous_fee = 0 OR num_tiers_vendor = 1 OR tier_rank_vendor = 1 OR previous_order_count = 0 THEN NULL
    ELSE (num_orders / previous_order_count - 1) / (df_total / previous_fee - 1)
  END AS tier_elasticity
FROM add_previous_order_count
ORDER BY entity_id, vendor_code, df_total;

SELECT *
FROM `dh-logistics-product-ops.pricing.vendors_and_fees_per_asa_order_loved_brands_pairwise_simulation` a
WHERE num_tiers_vendor = 4
ORDER BY a.entity_id, a.vendor_code, a.df_total
LIMIT 100