-- Zones picked must be near each other geographically, have similar control schemes (or at least the same component ID), and similar order shares
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.zone_selection_switchback_testing_aa_test` AS
WITH entities AS (
  SELECT
    ent.region,
    p.entity_id,
    LOWER(ent.country_iso) AS country_code,
    ent.country_name
FROM `fulfillment-dwh-production.cl.entities` ent
LEFT JOIN UNNEST(platforms) p
INNER JOIN (SELECT DISTINCT entity_id FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`) dps ON p.entity_id = dps.entity_id 
WHERE TRUE
    AND p.entity_id NOT LIKE 'ODR%' -- Eliminate entities starting with ODR (on-demand riders)
    AND p.entity_id NOT LIKE 'DN_%' -- Eliminate entities starting with DN_ as they are not part of DPS
    AND p.entity_id NOT IN ('FP_DE', 'FP_JP', 'BM_VN', 'BM_KR') -- Eliminate irrelevant entity_ids in APAC
    AND p.entity_id NOT IN ('TB_SA', 'HS_BH', 'CG_QA', 'IN_AE', 'ZO_AE', 'IN_BH') -- Eliminate irrelevant entity_ids in MENA
    AND p.entity_id NOT IN ('TB_SA', 'HS_BH', 'CG_QA', 'IN_AE', 'ZO_AE', 'IN_BH') -- Eliminate irrelevant entity_ids in Europe
    AND p.entity_id NOT IN ('CD_CO') -- Eliminate irrelevant entity_ids in LATAM
    AND p.entity_id IN ('FP_PH', 'FP_TH')
),

geo_data AS (
  SELECT 
    co.region,
    p.entity_id,
    co.country_code,
    ci.name AS city_name,
    ci.id AS city_id,
    zo.name AS zone_name,
    zo.id AS zone_id,
    zo.shape AS zone_shape
FROM `fulfillment-dwh-production.cl.countries` co
LEFT JOIN UNNEST(co.platforms) p
LEFT JOIN UNNEST(co.cities) ci
LEFT JOIN UNNEST(ci.zones) zo
INNER JOIN entities ent ON p.entity_id = ent.entity_id AND co.country_code = ent.country_code 
WHERE TRUE 
    AND zo.is_active -- Active city
    AND ci.is_active -- Active zone
    AND CONCAT(p.entity_id, ' | ', ci.name) IN ('FP_TH | Trang', 'FP_TH | Roi et', 'FP_PH | Legazpi city', 'FP_PH | Calamba', 'FP_PH | Cebu', 'FP_PH | Davao')
)

SELECT 
    -- Identifiers and supplementary fields     
    -- Date and time
    a.created_date,
    a.order_placed_at,

    -- Location of order
    a.region,
    a.entity_id,
    a.country_code,
    a.city_name,
    a.city_id,
    a.zone_name,
    a.zone_id,

    -- Order/customer identifiers and session data
    a.variant,
    a.experiment_id AS test_id,
    a.perseus_client_id,
    a.ga_session_id,
    a.dps_sessionid,
    a.dps_customer_tag,
    a.order_id,
    a.platform_order_code,
    a.scheme_id,
    a.vendor_price_scheme_type,	-- The assignment type of the scheme to the vendor during the time of the order, such as 'Automatic', 'Manual', 'Campaign', and 'Country Fallback'.
    
    -- Vendor data and information on the delivery
    a.vendor_id,
    a.chain_id,
    a.chain_name,
    a.vertical_type,
    CASE 
      WHEN a.vendor_vertical_parent IS NULL THEN NULL 
      WHEN LOWER(a.vendor_vertical_parent) IN ('restaurant', 'restaurants') THEN 'restaurant'
      WHEN LOWER(a.vendor_vertical_parent) = 'shop' THEN 'shop'
      WHEN LOWER(a.vendor_vertical_parent) = 'darkstores' THEN 'darkstores'
    END AS vendor_vertical_parent,
    a.delivery_status,
    a.is_own_delivery,
    a.exchange_rate,

    -- Business KPIs (These are the components of profit)
    a.dps_delivery_fee_local,
    a.delivery_fee_local,
    a.commission_local,
    a.joker_vendor_fee_local,
    COALESCE(a.service_fee, 0) AS service_fee_local,
    dwh.value.mov_customer_fee_local AS sof_local_cdwh,
    IF(a.gfv_local - a.dps_minimum_order_value_local >= 0, 0, COALESCE(dwh.value.mov_customer_fee_local, (a.dps_minimum_order_value_local - a.gfv_local))) AS sof_local,
    CASE
        WHEN ent.region IN ('Europe', 'Asia') THEN COALESCE( -- Get the delivery fee data of Pandora countries from Pandata tables
            pd.delivery_fee_local, 
            -- In 99 pct of cases, we won't need to use that fallback logic as pd.delivery_fee_local is reliable
            IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.delivery_fee_local)
        )
        -- If the order comes from a non-Pandora country, use delivery_fee_local
        WHEN ent.region NOT IN ('Europe', 'Asia') THEN (CASE WHEN is_delivery_fee_covered_by_voucher = FALSE AND is_delivery_fee_covered_by_discount = FALSE THEN a.delivery_fee_local ELSE 0 END)
    END AS actual_df_paid_by_customer,

    -- Special fields
    a.is_delivery_fee_covered_by_discount, -- Needed in the profit formula
    a.is_delivery_fee_covered_by_voucher, -- Needed in the profit formula
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
LEFT JOIN geo_data zn 
  ON TRUE 
    AND a.entity_id = zn.entity_id 
    AND a.country_code = zn.country_code
    AND a.zone_id = zn.zone_id 
INNER JOIN entities ent ON a.entity_id = ent.entity_id -- Get the region associated with every entity_id
WHERE TRUE
    AND a.created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 32 DAY) -- Filter for all orders in the last 30 days
    AND a.is_own_delivery -- OD or MP (Comment out to include MP vendors in the non-TG)
    AND a.delivery_status = 'completed' -- Successful orders
    AND ST_CONTAINS(zn.zone_shape, ST_GEOGPOINT(dwh.delivery_location.longitude, dwh.delivery_location.latitude)); -- Filter for orders coming from the target zones