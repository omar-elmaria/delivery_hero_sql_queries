-- Step 1: Declare the input variables used throughout the script
DECLARE entity ARRAY <STRING>;
DECLARE city_name_var STRING;
DECLARE v_type STRING;
DECLARE start_date, end_date DATE;
SET entity = ['FP_PH'];
SET city_name_var = 'Misamis oriental';
SET v_type = 'restaurants'; -- Name of the test in the AB test dashboard/DPS experiments tab
SET (start_date, end_date) = (DATE('2022-01-01'), CURRENT_DATE()); -- Encompasses the entire duration of the hybrid test 

##-------------------------------------------------------------------------------------END OF INPUT SECTION-------------------------------------------------------------------------------------##

WITH delivery_costs AS (
    SELECT
        p.entity_id,
        p.order_id, 
        o.platform_order_code,
        SUM(p.costs) AS delivery_costs_local,
        SUM(p.costs_eur) AS delivery_costs_eur
    FROM `fulfillment-dwh-production.cl.utr_timings` p
    LEFT JOIN `fulfillment-dwh-production.cl.orders` o ON p.entity_id = o.entity.id AND p.order_id = o.order_id -- Use the platform_order_code in this table as a bridge to join the order_id from utr_timings to order_id from central_dwh.orders 
    WHERE 1=1
        AND p.created_date BETWEEN start_date AND end_date -- For partitioning elimination and speeding up the query
        AND o.created_date BETWEEN start_date AND end_date -- For partitioning elimination and speeding up the query
    GROUP BY 1,2,3
),

entities AS (
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
),

geo_data AS (
    SELECT 
    p.entity_id,
    country_code,
    ci.name AS city_name,
    ci.id AS city_id,
    zo.shape AS zone_shape, 
    zo.name AS zone_name,
    zo.id AS zone_id
FROM fulfillment-dwh-production.cl.countries co
LEFT JOIN UNNEST(co.platforms) p
LEFT JOIN UNNEST(co.cities) ci
LEFT JOIN UNNEST(ci.zones) zo
WHERE TRUE 
    AND zo.is_active -- Active city
    AND ci.is_active -- Active zone
    AND ci.name = city_name_var
),

raw_orders AS (
  SELECT 
    -- Identifiers and supplementary fields     
    -- Date and time
    a.created_date,
    DATE_TRUNC(a.created_date, MONTH) AS month,
    a.order_placed_at,

    -- Location of order
    a.region,
    a.entity_id,
    a.city_name,
    a.city_id,
    a.zone_name,
    a.zone_id,

    -- Order/customer identifiers and session data
    a.variant,
    a.experiment_id,
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
    a.vendor_vertical_parent,
    a.delivery_status,
    a.is_own_delivery,
    a.exchange_rate,

    -- Business KPIs (pick the ones that are applicable to your test)
    a.delivery_fee_local, -- The delivery fee amount of the dps session.
    a.dps_travel_time_fee_local, -- The (dps_delivery_fee - dps_surge_fee) of the dps session.
    a.dps_minimum_order_value_local AS mov_local, -- The minimum order value of the dps session.
    a.dps_surge_fee_local, -- The surge fee amount of the session.
    a.dps_delivery_fee_local,
    a.service_fee AS service_fee_local, -- The service fee amount of the session.
    a.gmv_local, -- The gmv (gross merchandise value) of the order placed from backend
    a.gfv_local, -- The gfv (gross food value) of the order placed from backend
    a.standard_fee, -- The standard fee for the session sent by DPS (Not a component of revenue. It's simply the fee from DBDF setup in DPS)
    a.commission_local,
    a.commission_base_local,
    a.joker_vendor_fee_local,
    ROUND(a.commission_local / NULLIF(a.commission_base_local, 0), 4) AS commission_rate,
    IF(a.gfv_local - a.dps_minimum_order_value_local >= 0, 0, COALESCE(dwh.value.mov_customer_fee_local, (a.dps_minimum_order_value_local - a.gfv_local))) AS sof_local,
    cst.delivery_costs_local,

    -- If an order had a basket value below MOV (i.e. small order fee was charged), add the small order fee calculated as MOV - GFV to the profit 
    COALESCE(
        pd.delivery_fee_local, 
        IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.dps_delivery_fee_local)
    ) + a.commission_local + a.joker_vendor_fee_local + COALESCE(a.service_fee, 0) + COALESCE(dwh.value.mov_customer_fee_local, IF(a.gfv_local < a.dps_minimum_order_value_local, (a.dps_minimum_order_value_local - a.gfv_local), 0)) AS revenue_local,

    COALESCE(
        pd.delivery_fee_local / a.exchange_rate, 
        IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.dps_delivery_fee_eur)
    ) + a.commission_eur + a.joker_vendor_fee_eur + COALESCE(a.service_fee / a.exchange_rate, 0) + COALESCE(dwh.value.mov_customer_fee_local / a.exchange_rate, IF(a.gfv_local < a.dps_minimum_order_value_local, (a.dps_minimum_order_value_local - a.gfv_local) / a.exchange_rate, 0)) AS revenue_eur,

    COALESCE(
        pd.delivery_fee_local, 
        IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.dps_delivery_fee_local)
    ) + a.commission_local + a.joker_vendor_fee_local + COALESCE(a.service_fee, 0) + COALESCE(dwh.value.mov_customer_fee_local, IF(a.gfv_local < a.dps_minimum_order_value_local, (a.dps_minimum_order_value_local - a.gfv_local), 0)) - cst.delivery_costs_local AS gross_profit_local,

    COALESCE(
        pd.delivery_fee_local / a.exchange_rate, 
        IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.dps_delivery_fee_eur)
    ) + a.commission_eur + a.joker_vendor_fee_eur + COALESCE(a.service_fee / a.exchange_rate, 0) + COALESCE(dwh.value.mov_customer_fee_local / a.exchange_rate, IF(a.gfv_local < a.dps_minimum_order_value_local, (a.dps_minimum_order_value_local - a.gfv_local) / a.exchange_rate, 0)) - cst.delivery_costs_local / a.exchange_rate AS gross_profit_eur,

    -- Logistics KPIs
    a.dps_mean_delay, -- A.K.A Average fleet delay --> Average lateness in minutes of an order placed at this time (Used by dashboard, das, dps). This data point is only available for OD orders.
    a.dps_mean_delay_zone_id, 
    a.dps_travel_time, -- The time (min) it takes rider to travel from vendor location coordinates to the customers. This data point is only available for OD orders.
    a.travel_time_distance_km, -- The distance (km) between the vendor location coordinates and customer location coordinates. This data point is only available for OD orders.
    a.delivery_distance_m, -- This is the "Delivery Distance" field in the overview tab in the AB test dashboard. The Manhattan distance (km) between the vendor location coordinates and customer location coordinates. This distance doesn't take into account potential stacked deliveries, and it's not the travelled distance. This data point is only available for OD orders.
    a.to_customer_time, -- The time difference between rider arrival at customer and the pickup time. This data point is only available for OD orders
    a.actual_DT,

    -- Centra DWH fields	
    dwh.value.delivery_fee_local AS delivery_fee_local_cdwh,	
    dwh.value.delivery_fee_vat_local AS delivery_fee_vat_local_cdwh,
    dwh.value.voucher_dh_local AS voucher_dh_local_cdwh,	
    dwh.value.voucher_other_local AS voucher_other_local_cdwh,	
    dwh.value.discount_dh_local AS discount_dh_local_cdwh,	
    dwh.value.discount_other_local AS discount_other_local_cdwh,	
    dwh.value.joker_customer_discount_local AS joker_customer_discount_local_cdwh,
    dwh.value.joker_vendor_fee_local AS joker_vendor_fee_local_cdwh,
    dwh.is_joker,
    dwh.value.gbv_local AS gfv_local_cdwh,
    dwh.value.customer_paid_local AS customer_paid_local_cdwh,
    dwh.value.mov_local AS mov_local_cdwh,
    dwh.value.mov_customer_fee_local AS sof_cdwh,
    dwh.payment_method,
    dwh.payment_type,

    -- Pandata fields
    pd.service_fee_total_local AS service_fee_total_local_pd,
    pd.container_price_local AS container_price_local_pd,
    pd.delivery_fee_local AS delivery_fee_local_pd,
    pd.delivery_fee_forced_local AS delivery_fee_forced_local_pd,
    pd.delivery_fee_original_local AS delivery_fee_original_local_pd,
    pd.delivery_fee_vat_rate AS delivery_fee_vat_rate_pd,	
    pd.product_vat_groups AS product_vat_groups_pd,	
    pd.vat_rate AS vat_rate_pd,
    pd.delivery_fee_vat_local AS delivery_fee_vat_local_pd,	
    pd.products_vat_amount_local AS products_vat_amount_local_pd,
    pd.vat_amount_local AS vat_amount_local_pd,

    -- Special fields
    CASE
        WHEN ent.region IN ('Europe', 'Asia') THEN COALESCE( -- Get the delivery fee data of Pandora countries from Pandata tables
            pd.delivery_fee_local, 
            IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.dps_delivery_fee_local)
        )
        WHEN ent.region NOT IN ('Europe', 'Asia') THEN (CASE WHEN is_delivery_fee_covered_by_voucher = FALSE AND is_delivery_fee_covered_by_discount = FALSE THEN a.delivery_fee_local ELSE 0 END) -- If the order comes from a non-Pandora country, use delivery_fee_local
    END AS actual_df_paid_by_customer,
    a.is_delivery_fee_covered_by_discount,
    a.is_delivery_fee_covered_by_voucher,
    CASE WHEN is_delivery_fee_covered_by_discount = FALSE AND is_delivery_fee_covered_by_voucher = FALSE THEN 'No DF Voucher' ELSE 'DF Voucher' END AS df_voucher_flag,
    CASE WHEN pdos.is_free_delivery_subscription_order = TRUE THEN 'Subscription FD Order' ELSE 'Non-Subscription FD Order' END AS fd_subscription_flag, -- Only two possible values --> True or False
    pd.minimum_delivery_value_local AS mov_pd
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` a
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` dwh ON a.entity_id = dwh.global_entity_id AND a.platform_order_code = dwh.order_id
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` pd ON a.entity_id = pd.global_entity_id AND a.platform_order_code = pd.code AND a.created_date = pd.created_date_utc -- Contains info on the orders in Pandora countries
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_sb_subscriptions` pdos ON pd.uuid = pdos.uuid AND pd.created_date_utc = pdos.created_date_utc
LEFT JOIN delivery_costs cst ON a.entity_id = cst.entity_id AND a.order_id = cst.order_id -- The table that stores the CPO
INNER JOIN entities ent ON a.entity_id = ent.entity_id -- INNER JOIN to only include active DH entities
LEFT JOIN geo_data zn ON a.entity_id = zn.entity_id AND a.zone_id = zn.zone_id -- Filter for orders in the target zones (combine this JOIN with the condition in the WHERE clause)
WHERE TRUE
    AND a.entity_id IN UNNEST(entity)
    AND a.created_date BETWEEN start_date AND end_date
    AND a.is_own_delivery -- OD or MP
    AND a.vertical_type = v_type -- Orders from a particular vertical (restuarants, groceries, darkstores, etc.)
    AND a.delivery_status = 'completed' -- Successful orders
    AND ST_CONTAINS(zn.zone_shape, ST_GEOGPOINT(dwh.delivery_location.longitude, dwh.delivery_location.latitude)) -- Filter for orders coming from the target zones
)

SELECT
  city_name_var,
  month,
  COUNT(DISTINCT order_id) AS order_count
FROM raw_orders
GROUP BY 1,2
ORDER BY 1,2