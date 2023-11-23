
-- Step 1: Declare the input variables used throughout the script
DECLARE od_status ARRAY <STRING>;
DECLARE start_date, end_date DATE;

SET od_status = ['OWN_DELIVERY'];
SET (start_date, end_date) = (DATE('2022-01-01'), CURRENT_DATE());

----------------------------------------------------------------END OF THE INPUT SECTION----------------------------------------------------------------

-- Step 4: Pull the business and logisitcal KPIs from "dps_sessions_mapped_to_orders_v2"
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.long_distance_delivery_analysis_raw_orders` AS
WITH delivery_costs AS (
    SELECT
        p.entity_id,
        p.order_id, 
        o.platform_order_code,
        SUM(p.delivery_costs) AS delivery_costs_local,
        SUM(p.delivery_costs_eur) AS delivery_costs_eur
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
)

SELECT 
    -- Identifiers and supplementary fields     
    -- Date and time
    a.created_date,
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
    a.perseus_client_id,
    a.ga_session_id,
    a.dps_sessionid,
    a.dps_customer_tag,
    a.order_id,
    a.platform_order_code,
    a.scheme_id,
    a.vendor_price_scheme_type,	-- The assignment type of the scheme to the vendor during the time of the order, such as 'Automatic', 'Manual' and 'Country Fallback'.
    
    -- Vendor data and information on the delivery
    a.vendor_id,
    a.chain_id,
    a.chain_name,
    a.vertical_type,
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
    a.mean_delay, -- A.K.A Average fleet delay --> Average lateness in minutes of an order placed at this time (Used by dashboard, das, dps). This data point is only available for OD orders.
    a.travel_time, -- The time (min) it takes rider to travel from vendor location coordinates to the customers. This data point is only available for OD orders.
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

    -- Date fields
    CONCAT(EXTRACT(YEAR FROM a.created_date), ' | ', 'CW ', EXTRACT(WEEK FROM a.created_date)) AS cw_year_order,
    EXTRACT(YEAR FROM a.created_date) AS year_order,
    EXTRACT(WEEK FROM a.created_date) AS cw_order,

    -- BIMA Incentives fields
    inc.is_dh_delivery,
    inc.customer_loyalty_segment,
    inc.subscriber_status,
    inc.incentive_type,
    inc.has_autoapplied_incentive,
    inc.has_item_inc,
    inc.has_voucher,
    inc.has_discount,
    inc.has_deliveryfee_inc,
    inc.deliveryfee_inc_source_of_funding,
    inc.delivery_fee_lc,
    i.deliveryfee_inc_lc,
    i.dh_deliveryfee_inc_lc,
    i.vendor_deliveryfee_inc_lc,
    i.thirdparty_deliveryfee_inc_lc,
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` a
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` dwh ON a.entity_id = dwh.global_entity_id AND a.platform_order_code = dwh.order_id
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` pd ON a.entity_id = pd.global_entity_id AND a.platform_order_code = pd.code AND a.created_date = pd.created_date_utc -- Contains info on the orders in Pandora countries
LEFT JOIN delivery_costs cst ON a.entity_id = cst.entity_id AND a.order_id = cst.order_id -- The table that stores the CPO
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_mkt.bima_incentives_reporting` inc ON a.entity_id = inc.global_entity_id AND a.platform_order_code = inc.order_id
LEFT JOIN UNNEST(incentive_spend) i
INNER JOIN entities ent ON a.entity_id = ent.entity_id -- INNER JOIN to only include active DH entities
WHERE TRUE
    AND a.created_date BETWEEN start_date AND end_date
    AND a.is_own_delivery -- OD or MP
    AND a.delivery_status = 'completed';

##----------------------------------------------------------------END OF THE RAW ORDERS SECTION----------------------------------------------------------------##

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.long_distance_delivery_analysis_raw_orders_with_ntiles` AS
WITH ntiles AS (
    SELECT
        region,
        entity_id,
        vertical_type,
        ROUND(distance_quantiles[OFFSET(0)], 3) AS distance_p0,
        ROUND(distance_quantiles[OFFSET(25)], 3) AS distance_p25,
        ROUND(distance_quantiles[OFFSET(33)], 3) AS distance_p33,
        ROUND(distance_quantiles[OFFSET(50)], 3) AS distance_p50,
        ROUND(distance_quantiles[OFFSET(66)], 3) AS distance_p66,
        ROUND(distance_quantiles[OFFSET(75)], 3) AS distance_p75,
        ROUND(distance_quantiles[OFFSET(85)], 3) AS distance_p85,
        ROUND(distance_quantiles[OFFSET(90)], 3) AS distance_p90,
        ROUND(distance_quantiles[OFFSET(95)], 3) AS distance_p95,
        ROUND(distance_quantiles[OFFSET(99)], 3) AS distance_p99,
        ROUND(distance_quantiles[OFFSET(100)], 3) AS distance_p100,
    FROM (
        SELECT
            region,
            entity_id,
            vertical_type,
            APPROX_QUANTILES(travel_time_distance_km, 100) AS distance_quantiles
        FROM `dh-logistics-product-ops.pricing.long_distance_delivery_analysis_raw_orders`
        GROUP BY 1,2,3
    )
)

SELECT 
    a.*,
    b.distance_p0,
    b.distance_p25,
    b.distance_p33,
    b.distance_p50,
    b.distance_p66,
    b.distance_p75,
    b.distance_p85,
    b.distance_p90,
    b.distance_p95,
    b.distance_p99,
    b.distance_p100
FROM `dh-logistics-product-ops.pricing.long_distance_delivery_analysis_raw_orders` a
LEFT JOIN ntiles b USING (region, entity_id, vertical_type);

##----------------------------------------------------------------END OF THE NTILES SECTION----------------------------------------------------------------##

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.long_distance_delivery_analysis_final_dataset` AS
SELECT
    region,
    entity_id,
    vertical_type,
    COUNT(DISTINCT order_id) AS Tot_Order_Count,
    COUNT(DISTINCT CASE WHEN travel_time_distance_km >= distance_p75 THEN order_id ELSE NULL END) AS Top_Quartile_Order_Count,
    COUNT(DISTINCT CASE WHEN travel_time_distance_km >= distance_p90 THEN order_id ELSE NULL END) AS Top_Decile_Order_Count,
    COUNT(DISTINCT CASE WHEN travel_time_distance_km >= distance_p99 THEN order_id ELSE NULL END) AS Top_One_Percent_Order_Count,
    COUNT(DISTINCT CASE WHEN travel_time_distance_km >= distance_p75 THEN order_id ELSE NULL END) / COUNT(DISTINCT order_id) AS Top_Quartile_Order_Share,
    COUNT(DISTINCT CASE WHEN travel_time_distance_km >= distance_p90 THEN order_id ELSE NULL END) / COUNT(DISTINCT order_id) AS Top_Decile_Order_Share,
    COUNT(DISTINCT CASE WHEN travel_time_distance_km >= distance_p99 THEN order_id ELSE NULL END) / COUNT(DISTINCT order_id) AS Top_One_Percent_Order_Share,
    AVG(distance_p75) AS Delivery_Dist_75th_Percentile,
    AVG(distance_p85) AS Delivery_Dist_85th_Percentile,
    AVG(distance_p90) AS Delivery_Dist_90th_Percentile,
    AVG(distance_p95) AS Delivery_Dist_95th_Percentile,
    AVG(distance_p99) AS Delivery_Dist_99th_Percentile,
    AVG(distance_p100) AS Delivery_Dist_100th_Percentile,
FROM `dh-logistics-product-ops.pricing.long_distance_delivery_analysis_raw_orders_with_ntiles`
GROUP BY 1,2,3
ORDER BY 1,2,3;

##----------------------------------------------------------------END OF THE FINAL DATASET SECTION----------------------------------------------------------------##

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.long_distance_delivery_analysis_distance_intervals` AS
SELECT  
    region,
    entity_id,
    vertical_type,
    b.Tot_Order_Count AS orders_per_ent_vert,
    COUNT(DISTINCT CASE WHEN a.travel_time_distance_km >= 2 THEN order_id ELSE NULL END) AS orders_higher_than_2km,
    COUNT(DISTINCT CASE WHEN a.travel_time_distance_km >= 2.5 THEN order_id ELSE NULL END) AS orders_higher_than_2point5km,
    COUNT(DISTINCT CASE WHEN a.travel_time_distance_km >= 3 THEN order_id ELSE NULL END) AS orders_higher_than_3km,
    COUNT(DISTINCT CASE WHEN a.travel_time_distance_km >= 3.5 THEN order_id ELSE NULL END) AS orders_higher_than_3point5km,
    COUNT(DISTINCT CASE WHEN a.travel_time_distance_km >= 4 THEN order_id ELSE NULL END) AS orders_higher_than_4km,
    COUNT(DISTINCT CASE WHEN a.travel_time_distance_km >= 4.5 THEN order_id ELSE NULL END) AS orders_higher_than_4point5km,
    COUNT(DISTINCT CASE WHEN a.travel_time_distance_km >= 5 THEN order_id ELSE NULL END) AS orders_higher_than_5km,

    COUNT(DISTINCT CASE WHEN a.travel_time_distance_km >= 2 AND a.actual_df_paid_by_customer = 0 THEN order_id ELSE NULL END) AS df_incentivized_orders_higher_than_2km,
    COUNT(DISTINCT CASE WHEN a.travel_time_distance_km >= 2.5 AND a.actual_df_paid_by_customer = 0 THEN order_id ELSE NULL END) AS df_incentivized_orders_higher_than_2point5km,
    COUNT(DISTINCT CASE WHEN a.travel_time_distance_km >= 3 AND a.actual_df_paid_by_customer = 0 THEN order_id ELSE NULL END) AS df_incentivized_orders_higher_than_3km,
    COUNT(DISTINCT CASE WHEN a.travel_time_distance_km >= 3.5 AND a.actual_df_paid_by_customer = 0 THEN order_id ELSE NULL END) AS df_incentivized_orders_higher_than_3point5km,
    COUNT(DISTINCT CASE WHEN a.travel_time_distance_km >= 4 AND a.actual_df_paid_by_customer = 0 THEN order_id ELSE NULL END) AS df_incentivized_orders_higher_than_4km,
    COUNT(DISTINCT CASE WHEN a.travel_time_distance_km >= 4.5 AND a.actual_df_paid_by_customer = 0 THEN order_id ELSE NULL END) AS df_incentivized_orders_higher_than_4point5km,
    COUNT(DISTINCT CASE WHEN a.travel_time_distance_km >= 5 AND a.actual_df_paid_by_customer = 0 THEN order_id ELSE NULL END) AS df_incentivized_orders_higher_than_5km,
FROM `dh-logistics-product-ops.pricing.long_distance_delivery_analysis_raw_orders` a
LEFT JOIN `dh-logistics-product-ops.pricing.long_distance_delivery_analysis_final_dataset` b USING (region, entity_id, vertical_type)
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4