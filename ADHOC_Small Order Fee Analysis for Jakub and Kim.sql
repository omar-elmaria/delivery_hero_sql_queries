CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.order_data_sof_analysis` AS
-- Step 1: Get the active entities
WITH entities AS (
    SELECT
        ent.region,
        p.entity_id,
        ent.country_iso,
        ent.country_name,
FROM `fulfillment-dwh-production.cl.entities` ent
LEFT JOIN UNNEST(platforms) p
INNER JOIN (SELECT DISTINCT entity_id FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`) dps ON p.entity_id = dps.entity_id 
WHERE TRUE
    AND p.entity_id NOT LIKE 'ODR%' -- Eliminate entities starting with DN_ as they are not part of DPS
    AND p.entity_id NOT LIKE 'DN_%' -- Eliminate entities starting with ODR (on-demand riders)
    AND p.entity_id NOT IN ('FP_DE', 'FP_JP') -- Eliminate JP and DE because they are not DH markets any more
    AND p.entity_id != 'TB_SA' -- Eliminate this incorrect entity_id for Saudi
    AND p.entity_id != 'HS_BH' -- Eliminate this incorrect entity_id for Bahrain
)

-- Step 3: Pull the orders data
SELECT 
  -- Identifiers and supplementary fields     
  -- Date and time
  a.created_date AS created_date_utc,

  -- Location of order
  ent.region,
  a.entity_id,
  a.country_code,

  -- Order/customer identifiers and session data
  a.platform_order_code,
  a.vertical_type,

  -- Vendor data and information on the delivery
  a.vendor_id,
  a.exchange_rate,

  -- Business KPIs (These are the components of profit)
  a.commission_base_local,
  a.commission_local,
  a.dps_minimum_order_value_local,
  a.joker_vendor_fee_local,
  COALESCE(a.service_fee_local, 0) AS service_fee_local,
  dwh.value.mov_customer_fee_local AS sof_local_cdwh,
  IF(
    a.gfv_local - a.dps_minimum_order_value_local >= 0, 0, 
    CASE WHEN dwh.value.mov_customer_fee_local = 0 THEN GREATEST(a.dps_minimum_order_value_local - a.gfv_local, 0) ELSE dwh.value.mov_customer_fee_local END
  ) AS sof_local_theor,
  IF(a.gfv_local - a.dps_minimum_order_value_local >= 0, 0, COALESCE(dwh.value.mov_customer_fee_local, (a.dps_minimum_order_value_local - a.gfv_local))) AS sof_local,
  a.delivery_costs_local,
  CASE
      WHEN ent.region IN ('Europe', 'Asia') THEN COALESCE( -- Get the delivery fee data of Pandora countries from Pandata tables
          pd.delivery_fee_local, 
          -- In 99 pct of cases, we won't need to use that fallback logic as pd.delivery_fee_local is reliable
          IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.delivery_fee_local)
      )
      -- If the order comes from a non-Pandora country, use delivery_fee_local
      WHEN ent.region NOT IN ('Europe', 'Asia') THEN (CASE WHEN a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE THEN 0 ELSE a.delivery_fee_local END)
  END AS actual_df_paid_by_customer,
  a.delivery_fee_local,
  a.gmv_local,
  a.gfv_local,

  -- Extra MOV data
  pd.difference_to_minimum_plus_vat_local,
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` a
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` dwh 
  ON TRUE 
    AND a.entity_id = dwh.global_entity_id
    AND a.platform_order_code = dwh.order_id -- There is no country_code field in this table
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` pd -- Contains info on the orders in Pandora countries
  ON TRUE 
    AND a.entity_id = pd.global_entity_id
    AND a.platform_order_code = pd.code 
    AND a.created_date = pd.created_date_utc -- There is no country_code field in this table
INNER JOIN entities ent ON a.entity_id = ent.entity_id -- Get the region associated with every entity_id
WHERE TRUE
  AND created_date BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AND LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) -- Last completed month of data
  AND delivery_status = "completed" AND a.is_sent; -- Completed AND Successful order

-- Step 2: Create a dataset to calculate the order weighted MOV
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.order_wtd_mov_sof_analysis` AS
WITH order_wtd_mov AS (
  SELECT
    region,
    entity_id,
    SUM(mov_eur * orders_at_mov) / SUM(orders_at_mov) AS entity_order_wtd_mov_eur
  FROM (
    SELECT
      region,
      entity_id,
      dps_minimum_order_value_local / exchange_rate AS mov_eur,
      COUNT(DISTINCT platform_order_code) AS orders_at_mov
    FROM `dh-logistics-product-ops.pricing.order_data_sof_analysis`
    GROUP BY 1,2,3
  )
  GROUP BY 1,2
)

SELECT 
  a.*,
  b.entity_avg_mov_eur
FROM order_wtd_mov a
LEFT JOIN (
  SELECT
    region,
    entity_id,
    AVG(dps_minimum_order_value_local / exchange_rate) AS entity_avg_mov_eur
  FROM `dh-logistics-product-ops.pricing.order_data_sof_analysis`
  GROUP BY 1,2
) b ON a.entity_id = b.entity_id
ORDER BY 1,2