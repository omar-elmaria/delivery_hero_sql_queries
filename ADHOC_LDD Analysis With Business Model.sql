-- INVESTIGATE WHY YOU GET NEGATIVE LDD FEES (IS IT BECAUSE DPS_TT_FEE_LOCAL IS WRONG?)
-- Step 1: Declare the input variables used throughout the script
DECLARE entity ARRAY <STRING>;
DECLARE od_status ARRAY <STRING>;
DECLARE start_date, end_date DATE;

SET entity = ['FP_HK', 'FP_TW', 'FP_MY'];
SET od_status = ['OWN_DELIVERY'];
SET (start_date, end_date) = (DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY), CURRENT_DATE());

##-------------------------------------------------------------------END OF INPUT SECTION-------------------------------------------------------------------##

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ldd_analysis_distance_thresholds` (entity_id STRING, p_d_dist_threshold FLOAT64, conv_factor_min_per_km_op1 FLOAT64, p_d_dist_threshold_in_min_op1 FLOAT64);
INSERT `dh-logistics-product-ops.pricing.ldd_analysis_distance_thresholds` -- Can also use `fulfillment-dwh-production.dl.dynamic_pricing_travel_time_formula` for the conv factor
VALUES 
('FP_BD', 2.5, 6.55, 16.375),
('FP_HK', 2, 7.45, 14.9),
('FP_JP', 4, 4, 16),
('FP_KH', 5, 3.31, 16.55),
('FP_LA', 5, 2.57, 12.85),
('FP_MM', 4, 4.48, 17.92),
('FP_MY', 6, 2.61, 15.66),
('FP_PH', 3.5, 3.67, 12.845),
('FP_PK', 3.5, 4.15, 14.525),
('FP_SG', 3, 4.52, 13.56),
('FP_TH', 5, 2.56, 12.8),
('FP_TW', 4, 3.04, 12.16);

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ldd_analysis_raw_orders` AS
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
    a.service_fee / a.exchange_rate AS service_fee_eur,
    a.gmv_local, -- The gmv (gross merchandise value) of the order placed from backend
    a.gmv_eur, -- The gmv (gross merchandise value) of the order placed from backend
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
    a.delivery_distance_m, -- A.K.A P_D Distance. This is the "Delivery Distance" field in the overview tab in the AB test dashboard. The Manhattan distance (km) between the vendor location coordinates and customer location coordinates. This distance doesn't take into account potential stacked deliveries, and it's not the travelled distance. This data point is only available for OD orders.
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
    ldd.distance_threshold_in_km * 1000 AS p_d_dist_threshold_m_op1,
    op1.conv_factor_min_per_km_op1,
    op1.p_d_dist_threshold_in_min_op1,

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
LEFT JOIN entities ent ON a.entity_id = ent.entity_id
LEFT JOIN `fulfillment-dwh-production.pandata_report.gsheet_logistics_long_distance_threshold` ldd ON a.country_code = ldd.lg_country_code
LEFT JOIN `dh-logistics-product-ops.pricing.ldd_analysis_distance_thresholds` op1 ON a.entity_id = op1.entity_id
WHERE TRUE
    AND a.entity_id IN UNNEST(entity)
    AND a.created_date BETWEEN start_date AND end_date
    AND a.is_own_delivery -- OD or MP
    AND a.delivery_status = 'completed';

##-------------------------------------------------------------------END OF RAW ORDERS SECTION-------------------------------------------------------------------##

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ldd_analysis_p_d_dist_ntiles` AS
SELECT
    entity_id,
    ROUND(p_d_dist_quantiles[OFFSET(75)], 3) AS p_d_dist_p75,
FROM (
    SELECT
        entity_id,
        APPROX_QUANTILES(delivery_distance_m, 100) AS p_d_dist_quantiles
    FROM `dh-logistics-product-ops.pricing.ldd_analysis_raw_orders`
    GROUP BY 1
);

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ldd_analysis_raw_orders` AS
SELECT 
    a.*,
    b.p_d_dist_p75,
FROM `dh-logistics-product-ops.pricing.ldd_analysis_raw_orders` a
LEFT JOIN `dh-logistics-product-ops.pricing.ldd_analysis_p_d_dist_ntiles`  b USING (entity_id);

##-------------------------------------------------------------------END OF NTILES SECTION-------------------------------------------------------------------##

-- Add the incentivized vs. non-incentized flag
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ldd_analysis_raw_orders` AS
SELECT 
    *,
    CASE WHEN 
        (
            NOT(
                (subscriber_status IN ('subscriber', 'churned-subscriber') AND actual_df_paid_by_customer = 0) -- Non-subscriber condition 
                OR 
                ((df_voucher_flag = 'DF Voucher' OR dps_customer_tag = 'New') AND actual_df_paid_by_customer = 0) -- Non-incentivized order condition
            )
        ) THEN 'Non_Sub_Nor_Inc'

        WHEN
        (
            (subscriber_status IN ('subscriber', 'churned-subscriber') AND actual_df_paid_by_customer = 0) -- Subscriber condition 
            OR 
            ((df_voucher_flag = 'DF Voucher' OR dps_customer_tag = 'New') AND actual_df_paid_by_customer = 0) -- Incentivized order condition
        ) THEN 'Sub_and_Inc'
        ELSE NULL
    END AS Incentivization_Status,

    CASE WHEN delivery_distance_m > p_d_dist_threshold_m_op1 THEN 'LDD Order' ELSE 'Non-LDD Order' END AS LDD_Status_Option_1,
    CASE WHEN delivery_distance_m > p_d_dist_p75 THEN 'LDD Order' ELSE 'Non-LDD Order' END AS LDD_Status_Option_2,
    CASE WHEN delivery_distance_m > 2000 THEN 'LDD Order' ELSE 'Non-LDD Order' END AS LDD_Status_Option_3,
FROM `dh-logistics-product-ops.pricing.ldd_analysis_raw_orders`;

##-------------------------------------------------------------------END OF FLAGS SECTION-------------------------------------------------------------------##

-- Get the DBDF setups of all schemes in the target countries
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ldd_analysis_price_scheme_tt_tiers` AS
SELECT 
    a.*,
    RANK() OVER (PARTITION BY scheme_id, component_id ORDER BY threshold) AS tier,
    op1.p_d_dist_threshold * 1000 AS p_d_dist_threshold_m_op1,
    op1.conv_factor_min_per_km_op1,
    op1.p_d_dist_threshold_in_min_op1,

    op2.p_d_dist_p75 AS p_d_dist_threshold_m_op2,
    (op2.p_d_dist_p75 / 1000) * op1.conv_factor_min_per_km_op1 AS p_d_dist_threshold_in_min_op2,

    2000 AS p_d_dist_threshold_m_op3,
    (2000 / 1000) * op1.conv_factor_min_per_km_op1 AS p_d_dist_threshold_in_min_op3,

FROM (
    SELECT DISTINCT 
        v.entity_id,
        d.scheme_id,
        d.scheme_name,
        pc.travel_time_config.id AS component_id,
        pc.travel_time_config.name AS component_name,
        CASE WHEN pc.travel_time_config.threshold IS NULL THEN 9999999 ELSE pc.travel_time_config.threshold END AS threshold,
        CASE 
            WHEN pc.travel_time_config.threshold IS NULL THEN 9999999 
            ELSE ROUND(FLOOR(pc.travel_time_config.threshold) + (pc.travel_time_config.threshold - FLOOR(pc.travel_time_config.threshold)) * 60/100, 2) 
        END AS threshold_in_min_and_sec,
        pc.travel_time_config.fee
    FROM `fulfillment-dwh-production.cl.vendors_v2` v
    LEFT JOIN UNNEST(zones) z
    LEFT JOIN UNNEST(dps) d
    LEFT JOIN UNNEST(d.vendor_config) vc
    LEFT JOIN UNNEST(vc.pricing_config) pc
    WHERE TRUE 
        -- AND v.entity_id IN UNNEST(entity)
        AND v.entity_id IN ('FP_MY', 'FP_HK', 'FP_TW') -- If you want to specify countries manually
        AND d.is_active
) a
LEFT JOIN `dh-logistics-product-ops.pricing.ldd_analysis_distance_thresholds` op1 ON a.entity_id = op1.entity_id -- Option 1
LEFT JOIN `dh-logistics-product-ops.pricing.ldd_analysis_p_d_dist_ntiles` op2 ON a.entity_id = op2.entity_id -- Option 3
ORDER BY 1,2,4;

-- Add the base + LDD fees to the raw orders table
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ldd_analysis_raw_orders_with_base_and_ldd_fees` AS
WITH max_base_fee_per_price_scheme AS (
    SELECT
        entity_id,
        scheme_id,
        COALESCE(
            MAX(CASE WHEN threshold <= p_d_dist_threshold_in_min_op1 THEN fee ELSE NULL END), -- Produces some NULLs because there is NOT always a threshold <= p_d_dist_threshold_in_min_op1 for all price schemes 
            MIN(CASE WHEN threshold > p_d_dist_threshold_in_min_op1 THEN fee ELSE NULL END)
        ) AS max_base_tt_fee_per_price_scheme_local_op1, -- OPTION 1: Maximum of DF paid when there is no LDD extra charge bec. the order is non-LDD

        COALESCE(
            MAX(CASE WHEN threshold <= p_d_dist_threshold_in_min_op2 THEN fee ELSE NULL END), -- Produces some NULLs because there is NOT always a threshold <= p_d_dist_threshold_in_min_op1 for all price schemes 
            MIN(CASE WHEN threshold > p_d_dist_threshold_in_min_op2 THEN fee ELSE NULL END)
        ) AS max_base_tt_fee_per_price_scheme_local_op2, -- OPTION 2: Maximum of DF paid when there is no LDD extra charge bec. the order is non-LDD

        COALESCE(
            MAX(CASE WHEN threshold <= p_d_dist_threshold_in_min_op3 THEN fee ELSE NULL END), -- Produces some NULLs because there is NOT always a threshold <= p_d_dist_threshold_in_min_op1 for all price schemes 
            MIN(CASE WHEN threshold > p_d_dist_threshold_in_min_op3 THEN fee ELSE NULL END)
        ) AS max_base_tt_fee_per_price_scheme_local_op3, -- OPTION 3: Maximum of DF paid when there is no LDD extra charge bec. the order is non-LDD
    FROM `dh-logistics-product-ops.pricing.ldd_analysis_price_scheme_tt_tiers`
    GROUP BY 1,2
),

max_base_fee_per_inactive_price_scheme AS (
    SELECT
        entity_id,
        ROUND(AVG(max_base_tt_fee_per_price_scheme_local_op1), 2) AS max_base_tt_fee_per_inactive_price_scheme_local_op1,
        ROUND(AVG(max_base_tt_fee_per_price_scheme_local_op2), 2) AS max_base_tt_fee_per_inactive_price_scheme_local_op2,
        ROUND(AVG(max_base_tt_fee_per_price_scheme_local_op3), 2) AS max_base_tt_fee_per_inactive_price_scheme_local_op3,
    FROM max_base_fee_per_price_scheme
    GROUP BY 1
)

SELECT 
    a.*,
    COALESCE(b.max_base_tt_fee_per_price_scheme_local_op1, d.max_base_tt_fee_per_inactive_price_scheme_local_op1) AS max_base_tt_fee_local_op1,
    COALESCE(b.max_base_tt_fee_per_price_scheme_local_op2, d.max_base_tt_fee_per_inactive_price_scheme_local_op2) AS max_base_tt_fee_local_op2,
    COALESCE(b.max_base_tt_fee_per_price_scheme_local_op3, d.max_base_tt_fee_per_inactive_price_scheme_local_op3) AS max_base_tt_fee_local_op3,
    CASE 
        WHEN delivery_distance_m <= p_d_dist_threshold_m_op1 THEN 0 -- Non-LDD 
        ELSE a.dps_travel_time_fee_local - COALESCE(b.max_base_tt_fee_per_price_scheme_local_op1, d.max_base_tt_fee_per_inactive_price_scheme_local_op1) 
    END AS ldd_fee_local_op1, -- OPTION 1: COALESCE caters for cases where we don't have a base fee due to inactive schemes or few tiers

    CASE 
        WHEN delivery_distance_m <= op2.p_d_dist_p75 THEN 0 -- Non-LDD 
        ELSE a.dps_travel_time_fee_local - COALESCE(b.max_base_tt_fee_per_price_scheme_local_op2, d.max_base_tt_fee_per_inactive_price_scheme_local_op2) 
    END AS ldd_fee_local_op2, -- OPTION 2: COALESCE caters for cases where we don't have a base fee due to inactive schemes or few tiers

    CASE 
        WHEN delivery_distance_m <= 2000 THEN 0 -- Non-LDD 
        ELSE a.dps_travel_time_fee_local - COALESCE(b.max_base_tt_fee_per_price_scheme_local_op3, d.max_base_tt_fee_per_inactive_price_scheme_local_op3) 
    END AS ldd_fee_local_op3, -- OPTION 3: COALESCE caters for cases where we don't have a base fee due to inactive schemes or few tiers
FROM `dh-logistics-product-ops.pricing.ldd_analysis_raw_orders` a
LEFT JOIN max_base_fee_per_price_scheme b ON a.entity_id = b.entity_id AND a.scheme_id = b.scheme_id
-- LEFT JOIN `dh-logistics-product-ops.pricing.ldd_analysis_distance_thresholds` c ON a.entity_id = c.entity_id
LEFT JOIN max_base_fee_per_inactive_price_scheme d ON a.entity_id = d.entity_id
LEFT JOIN `dh-logistics-product-ops.pricing.ldd_analysis_p_d_dist_ntiles` op2 ON a.entity_id = op2.entity_id;

-- Impute LDD fees for NULL scheme IDs
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ldd_analysis_raw_orders_with_base_and_ldd_fees` AS -- UPDATE this table for NULL ldd_fee_local_op1
SELECT 
    a.*,
    b.ldd_fee_country_avg_local_op1,
    b.ldd_fee_country_avg_local_op2,
    b.ldd_fee_country_avg_local_op3,
FROM `dh-logistics-product-ops.pricing.ldd_analysis_raw_orders_with_base_and_ldd_fees` a 
LEFT JOIN (
    SELECT
        entity_id,
        ROUND(AVG(CASE WHEN ldd_fee_local_op1 > 0 AND ldd_fee_local_op1 IS NOT NULL THEN ldd_fee_local_op1 ELSE NULL END), 2) AS ldd_fee_country_avg_local_op1,
        ROUND(AVG(CASE WHEN ldd_fee_local_op2 > 0 AND ldd_fee_local_op2 IS NOT NULL THEN ldd_fee_local_op2 ELSE NULL END), 2) AS ldd_fee_country_avg_local_op2,
        ROUND(AVG(CASE WHEN ldd_fee_local_op3 > 0 AND ldd_fee_local_op3 IS NOT NULL THEN ldd_fee_local_op3 ELSE NULL END), 2) AS ldd_fee_country_avg_local_op3,
    FROM `dh-logistics-product-ops.pricing.ldd_analysis_raw_orders_with_base_and_ldd_fees`
    GROUP BY 1
) b ON a.entity_id = b.entity_id;

UPDATE `dh-logistics-product-ops.pricing.ldd_analysis_raw_orders_with_base_and_ldd_fees` 
SET 
    ldd_fee_local_op1 = ldd_fee_country_avg_local_op1,
    ldd_fee_local_op2 = ldd_fee_country_avg_local_op2,
    ldd_fee_local_op3 = ldd_fee_country_avg_local_op3
WHERE ldd_fee_local_op1 IS NULL;

##-------------------------------------------------------------------END OF BASE + LDD SECTION-------------------------------------------------------------------##

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ldd_analysis_agg_data` AS
WITH ldd_orders_prep_data AS (
    SELECT
        entity_id,
        -- Option 1 (Subscription + Incentivised - Long Distance - based on logistics definition)
        COUNT(DISTINCT CASE WHEN LDD_Status_Option_1 = 'Non-LDD Order' THEN order_id ELSE NULL END) AS Non_LDD_Orders_Log_Def_op1,
        COUNT(DISTINCT CASE WHEN LDD_Status_Option_1 = 'LDD Order' THEN order_id ELSE NULL END) AS LDD_Orders_Log_Def_op1,
        COUNT(
            DISTINCT CASE WHEN Incentivization_Status = 'Non_Sub_Nor_Inc' AND LDD_Status_Option_1 = 'LDD Order'
            THEN order_id ELSE NULL END
        ) AS LDD_Orders_Non_Sub_Nor_Inc_op1,

        COUNT(
            DISTINCT CASE WHEN Incentivization_Status = 'Sub_and_Inc' AND LDD_Status_Option_1 = 'LDD Order'
            THEN order_id ELSE NULL END
        ) AS LDD_Orders_Sub_and_Inc_op1,

        -- Option 2 (25% of all orders)
        COUNT(DISTINCT CASE WHEN LDD_Status_Option_2 = 'Non-LDD Order' THEN order_id ELSE NULL END) AS Non_LDD_Orders_Log_Def_op2,
        COUNT(DISTINCT CASE WHEN LDD_Status_Option_2 = 'LDD Order' THEN order_id ELSE NULL END) AS LDD_Orders_Log_Def_op2,
        COUNT(
            DISTINCT CASE WHEN Incentivization_Status = 'Non_Sub_Nor_Inc' AND LDD_Status_Option_2 = 'LDD Order'
            THEN order_id ELSE NULL END
        ) AS LDD_Orders_Non_Sub_Nor_Inc_op2,

        COUNT(
            DISTINCT CASE WHEN Incentivization_Status = 'Sub_and_Inc' AND LDD_Status_Option_2 = 'LDD Order'
            THEN order_id ELSE NULL END
        ) AS LDD_Orders_Sub_and_Inc_op2,

        -- Option 3 (> 2 km all inc and sub orders)
        COUNT(DISTINCT CASE WHEN LDD_Status_Option_3 = 'Non-LDD Order' THEN order_id ELSE NULL END) AS Non_LDD_Orders_Log_Def_op3,
        COUNT(DISTINCT CASE WHEN LDD_Status_Option_3 = 'LDD Order' THEN order_id ELSE NULL END) AS LDD_Orders_Log_Def_op3,
        COUNT(
            DISTINCT CASE WHEN Incentivization_Status = 'Non_Sub_Nor_Inc' AND LDD_Status_Option_3 = 'LDD Order'
            THEN order_id ELSE NULL END
        ) AS LDD_Orders_Non_Sub_Nor_Inc_op3,

        COUNT(
            DISTINCT CASE WHEN Incentivization_Status = 'Sub_and_Inc' AND LDD_Status_Option_3 = 'LDD Order'
            THEN order_id ELSE NULL END
        ) AS LDD_Orders_Sub_and_Inc_op3,
    FROM `dh-logistics-product-ops.pricing.ldd_analysis_raw_orders_with_base_and_ldd_fees`
    GROUP BY 1
),

potential_ldd_rev_with_base_fee_differentiation AS (
    SELECT
        a.entity_id,
        -- LD fee for incentivzed orders (Local)
        -- Option 1 (Subscription + Incentivised - Long Distance - based on logistics definition)
        COUNT(
            DISTINCT CASE WHEN 
                Incentivization_Status = 'Sub_and_Inc'
                AND
                LDD_Status_Option_1 = 'LDD Order' -- Long distance condition
                AND 
                ldd_fee_local_op1 > 0 -- Eliminate -ve ldd fees (data issue) AND 0 ldd fees (already excluded by the 2nd distance condition)
            THEN order_id ELSE NULL END 
        ) AS Num_Orders_Sub_and_Inc_With_LD_Extra_Fee_op1,

        ROUND(SUM(
            CASE WHEN 
                Incentivization_Status = 'Sub_and_Inc'
                AND
                LDD_Status_Option_1 = 'LDD Order' -- Long distance condition
                AND 
                ldd_fee_local_op1 > 0 -- Eliminate -ve ldd fees (data issue) AND 0 ldd fees (already excluded by the 2nd distance condition)
            THEN ldd_fee_local_op1 ELSE NULL END 
        ), 2) AS Tot_LD_Extra_Fee_Sub_and_Inc_Orders_Local_op1,

        -- LD fee for incentivzed orders (Eur)
        ROUND(SUM(
            CASE WHEN 
                Incentivization_Status = 'Sub_and_Inc'
                AND
                LDD_Status_Option_1 = 'LDD Order' -- Long distance condition
                AND 
                ldd_fee_local_op1 > 0 -- Eliminate -ve ldd fees (data issue) AND 0 ldd fees (already excluded by the 2nd distance condition)
            THEN ldd_fee_local_op1 / exchange_rate ELSE NULL END 
        ), 2) AS Tot_LD_Extra_Fee_Sub_and_Inc_Orders_Eur_op1,

        -- Option 2 (25% of all orders)
        COUNT(
            DISTINCT CASE WHEN 
                Incentivization_Status = 'Sub_and_Inc'
                AND
                LDD_Status_Option_2 = 'LDD Order' -- Long distance condition
                AND 
                ldd_fee_local_op2 > 0 -- Eliminate -ve ldd fees (data issue) AND 0 ldd fees (already excluded by the 2nd distance condition)
            THEN order_id ELSE NULL END 
        ) AS Num_Orders_Sub_and_Inc_With_LD_Extra_Fee_op2,

        ROUND(SUM(
            CASE WHEN 
                Incentivization_Status = 'Sub_and_Inc'
                AND
                LDD_Status_Option_2 = 'LDD Order' -- Long distance condition
                AND 
                ldd_fee_local_op2 > 0 -- Eliminate -ve ldd fees (data issue) AND 0 ldd fees (already excluded by the 2nd distance condition)
            THEN ldd_fee_local_op2 ELSE NULL END 
        ), 2) AS Tot_LD_Extra_Fee_Sub_and_Inc_Orders_Local_op2,

        -- LD fee for incentivzed orders (Eur)
        ROUND(SUM(
            CASE WHEN 
                Incentivization_Status = 'Sub_and_Inc'
                AND
                LDD_Status_Option_2 = 'LDD Order' -- Long distance condition
                AND 
                ldd_fee_local_op2 > 0 -- Eliminate -ve ldd fees (data issue) AND 0 ldd fees (already excluded by the 2nd distance condition)
            THEN ldd_fee_local_op2 / exchange_rate ELSE NULL END 
        ), 2) AS Tot_LD_Extra_Fee_Sub_and_Inc_Orders_Eur_op2,

        -- Option 3 (> 2 km all inc and sub orders)
        COUNT(
            DISTINCT CASE WHEN 
                Incentivization_Status = 'Sub_and_Inc'
                AND
                LDD_Status_Option_3 = 'LDD Order' -- Long distance condition
                AND 
                ldd_fee_local_op3 > 0 -- Eliminate -ve ldd fees (data issue) AND 0 ldd fees (already excluded by the 2nd distance condition)
            THEN order_id ELSE NULL END 
        ) AS Num_Orders_Sub_and_Inc_With_LD_Extra_Fee_op3,

        ROUND(SUM(
            CASE WHEN 
                Incentivization_Status = 'Sub_and_Inc'
                AND
                LDD_Status_Option_3 = 'LDD Order' -- Long distance condition
                AND 
                ldd_fee_local_op3 > 0 -- Eliminate -ve ldd fees (data issue) AND 0 ldd fees (already excluded by the 2nd distance condition)
            THEN ldd_fee_local_op3 ELSE NULL END 
        ), 2) AS Tot_LD_Extra_Fee_Sub_and_Inc_Orders_Local_op3,

        -- LD fee for incentivzed orders (Eur)
        ROUND(SUM(
            CASE WHEN 
                Incentivization_Status = 'Sub_and_Inc'
                AND
                LDD_Status_Option_3 = 'LDD Order' -- Long distance condition
                AND 
                ldd_fee_local_op3 > 0 -- Eliminate -ve ldd fees (data issue) AND 0 ldd fees (already excluded by the 2nd distance condition)
            THEN ldd_fee_local_op3 / exchange_rate ELSE NULL END 
        ), 2) AS Tot_LD_Extra_Fee_Sub_and_Inc_Orders_Eur_op3,
    FROM `dh-logistics-product-ops.pricing.ldd_analysis_raw_orders_with_base_and_ldd_fees` a
    GROUP BY 1
)

SELECT
    -- Grouping Variable
    a.entity_id,

    -- General fields
    COUNT(DISTINCT order_id) AS Orders_L30D,
    ROUND(SUM(gmv_eur), 2) AS Tot_GMV_Eur,
    ROUND(SUM(gmv_local), 2) AS Tot_GMV_Local,
    ROUND(SUM(service_fee_eur), 2) AS Tot_Service_Fee_Rev_L30D_Eur,
    ROUND(SUM(service_fee_local), 2) AS Tot_Service_Fee_Rev_L30D_Local,
    ROUND(SUM(service_fee_eur) / SUM(gmv_eur), 4) AS Service_Fee_Perc_of_GMV_L30D,
    COUNT(DISTINCT CASE WHEN subscriber_status IN ('subscriber', 'churned-subscriber') AND actual_df_paid_by_customer = 0 THEN order_id ELSE NULL END) AS Subscription_Orders_L30D,
    ROUND(COUNT(DISTINCT CASE WHEN subscriber_status IN ('subscriber', 'churned-subscriber') AND actual_df_paid_by_customer = 0 THEN order_id ELSE NULL END) / COUNT(DISTINCT order_id), 4) AS Subscription_Order_Share_L30D,
    COUNT(DISTINCT CASE WHEN (df_voucher_flag = 'DF Voucher' OR dps_customer_tag = 'New') AND actual_df_paid_by_customer = 0 THEN order_id ELSE NULL END) AS FDNC_and_Voucher_Orders_L30D,
    ROUND(COUNT(DISTINCT CASE WHEN (df_voucher_flag = 'DF Voucher' OR dps_customer_tag = 'New') AND actual_df_paid_by_customer = 0 THEN order_id ELSE NULL END) / COUNT(DISTINCT order_id), 4) AS FDNC_and_Voucher_Order_Share_L30D,

    'Separator_1' AS Separator_1,

    -- Option 1 (Subscription + incentivised (FDNC & Vouchers) - Long Distance - based on logistics definition)
    -- Prep Data
    b.Non_LDD_Orders_Log_Def_op1,
    b.LDD_Orders_Log_Def_op1,
    b.LDD_Orders_Non_Sub_Nor_Inc_op1,
    b.LDD_Orders_Sub_and_Inc_op1,

    -- Avg DF Data
    -- Avg DF Non-Sub/Inc LDD (Eur)
    ROUND(SUM(
        CASE WHEN 
            Incentivization_Status = 'Non_Sub_Nor_Inc'
            AND
            LDD_Status_Option_1 = 'LDD Order' -- Long distance condition
        THEN dps_travel_time_fee_local / exchange_rate ELSE NULL END 
    ) / b.LDD_Orders_Non_Sub_Nor_Inc_op1, 2) AS Avg_DF_Non_Sub_Nor_Inc_LDD_Eur_op1,

    -- Avg DF Non-Sub/Inc LDD (Local)
    ROUND(SUM(
        CASE WHEN 
            Incentivization_Status = 'Non_Sub_Nor_Inc'
            AND
            LDD_Status_Option_1 = 'LDD Order' -- Long distance condition
        THEN dps_travel_time_fee_local ELSE NULL END 
    ) / b.LDD_Orders_Non_Sub_Nor_Inc_op1, 2) AS Avg_DF_Non_Sub_Nor_Inc_LDD_Local_op1,

    ------------------------------------------------------------------------------------------------------------------------------------------------------

    -- Total DF Rev Data
    -- DF Rev Lost (Eur)
    ROUND(SUM(
        CASE WHEN 
            Incentivization_Status = 'Non_Sub_Nor_Inc'
            AND
            LDD_Status_Option_1 = 'LDD Order' -- Long distance condition
        THEN dps_travel_time_fee_local / exchange_rate ELSE NULL END 
    ) / b.LDD_Orders_Non_Sub_Nor_Inc_op1, 2) * b.LDD_Orders_Sub_and_Inc_op1 AS Total_DF_Rev_Lost_Eur_op1,

    -- DF Rev Lost (Local)
    ROUND(SUM(
        CASE WHEN 
            Incentivization_Status = 'Non_Sub_Nor_Inc'
            AND
            LDD_Status_Option_1 = 'LDD Order' -- Long distance condition
        THEN dps_travel_time_fee_local ELSE NULL END
    ) / b.LDD_Orders_Non_Sub_Nor_Inc_op1, 2) * b.LDD_Orders_Sub_and_Inc_op1 AS Total_DF_Rev_Lost_Local_op1,

    ------------------------------------------------------------------------------------------------------------------------------------------------------

    -- Potential LD Extra Rev from introducing base and LDD fee
    c.Num_Orders_Sub_and_Inc_With_LD_Extra_Fee_op1,
    c.Tot_LD_Extra_Fee_Sub_and_Inc_Orders_Local_op1,
    c.Tot_LD_Extra_Fee_Sub_and_Inc_Orders_Eur_op1,

    'Separator_2' AS Separator_2,

    ##--------------------------------------------------------------END OF OPTION 1 METRICS--------------------------------------------------------------##

    -- Option 2 (25% of all orders)
    -- Prep Data
    b.Non_LDD_Orders_Log_Def_op2,
    b.LDD_Orders_Log_Def_op2,
    b.LDD_Orders_Non_Sub_Nor_Inc_op2,
    b.LDD_Orders_Sub_and_Inc_op2,

    -- Avg DF Data
    -- Avg DF Non-Sub/Inc LDD (Eur)
    ROUND(SUM(
        CASE WHEN 
            Incentivization_Status = 'Non_Sub_Nor_Inc'
            AND
            LDD_Status_Option_2 = 'LDD Order' -- Long distance condition
        THEN dps_travel_time_fee_local / exchange_rate ELSE NULL END 
    ) / b.LDD_Orders_Non_Sub_Nor_Inc_op2, 2) AS Avg_DF_Non_Sub_Nor_Inc_LDD_Eur_op2,

    -- Avg DF Non-Sub/Inc LDD (Local)
    ROUND(SUM(
        CASE WHEN 
            Incentivization_Status = 'Non_Sub_Nor_Inc'
            AND
            LDD_Status_Option_2 = 'LDD Order' -- Long distance condition
        THEN dps_travel_time_fee_local ELSE NULL END 
    ) / b.LDD_Orders_Non_Sub_Nor_Inc_op2, 2) AS Avg_DF_Non_Sub_Nor_Inc_LDD_Local_op2,

    ------------------------------------------------------------------------------------------------------------------------------------------------------

    -- Total DF Rev Data
    -- DF Rev Lost (Eur)
    ROUND(SUM(
        CASE WHEN 
            Incentivization_Status = 'Non_Sub_Nor_Inc'
            AND
            LDD_Status_Option_2 = 'LDD Order' -- Long distance condition
        THEN dps_travel_time_fee_local / exchange_rate ELSE NULL END 
    ) / b.LDD_Orders_Non_Sub_Nor_Inc_op2, 2) * b.LDD_Orders_Sub_and_Inc_op2 AS Total_DF_Rev_Lost_Eur_op2,

    -- DF Rev Lost (Local)
    ROUND(SUM(
        CASE WHEN 
            Incentivization_Status = 'Non_Sub_Nor_Inc'
            AND
            LDD_Status_Option_2 = 'LDD Order' -- Long distance condition
        THEN dps_travel_time_fee_local ELSE NULL END
    ) / b.LDD_Orders_Non_Sub_Nor_Inc_op2, 2) * b.LDD_Orders_Sub_and_Inc_op2 AS Total_DF_Rev_Lost_Local_op2,

    ------------------------------------------------------------------------------------------------------------------------------------------------------

    -- Potential LD Extra Rev from introducing base and LDD fee
    c.Num_Orders_Sub_and_Inc_With_LD_Extra_Fee_op2,
    c.Tot_LD_Extra_Fee_Sub_and_Inc_Orders_Local_op2,
    c.Tot_LD_Extra_Fee_Sub_and_Inc_Orders_Eur_op2,

    'Separator_3' AS Separator_3,

    ##--------------------------------------------------------------END OF OPTION 2 METRICS--------------------------------------------------------------##

    -- Option 3 
    -- Prep Data
    b.Non_LDD_Orders_Log_Def_op3,
    b.LDD_Orders_Log_Def_op3,
    b.LDD_Orders_Non_Sub_Nor_Inc_op3,
    b.LDD_Orders_Sub_and_Inc_op3,

    -- Avg DF Data
    -- Avg DF Non-Sub/Inc LDD (Eur)
    ROUND(SUM(
        CASE WHEN 
            Incentivization_Status = 'Non_Sub_Nor_Inc'
            AND
            LDD_Status_Option_3 = 'LDD Order' -- Long distance condition
        THEN dps_travel_time_fee_local / exchange_rate ELSE NULL END 
    ) / b.LDD_Orders_Non_Sub_Nor_Inc_op3, 2) AS Avg_DF_Non_Sub_Nor_Inc_LDD_Eur_op3,

    -- Avg DF Non-Sub/Inc LDD (Local)
    ROUND(SUM(
        CASE WHEN 
            Incentivization_Status = 'Non_Sub_Nor_Inc'
            AND
            LDD_Status_Option_3 = 'LDD Order' -- Long distance condition
        THEN dps_travel_time_fee_local ELSE NULL END 
    ) / b.LDD_Orders_Non_Sub_Nor_Inc_op3, 2) AS Avg_DF_Non_Sub_Nor_Inc_LDD_Local_op3,

    ------------------------------------------------------------------------------------------------------------------------------------------------------

    -- Total DF Rev Data
    -- DF Rev Lost (Eur)
    ROUND(SUM(
        CASE WHEN 
            Incentivization_Status = 'Non_Sub_Nor_Inc'
            AND
            LDD_Status_Option_3 = 'LDD Order' -- Long distance condition
        THEN dps_travel_time_fee_local / exchange_rate ELSE NULL END 
    ) / b.LDD_Orders_Non_Sub_Nor_Inc_op3, 2) * b.LDD_Orders_Sub_and_Inc_op3 AS Total_DF_Rev_Lost_Eur_op3,

    -- DF Rev Lost (Local)
    ROUND(SUM(
        CASE WHEN 
            Incentivization_Status = 'Non_Sub_Nor_Inc'
            AND
            LDD_Status_Option_3 = 'LDD Order' -- Long distance condition
        THEN dps_travel_time_fee_local ELSE NULL END
    ) / b.LDD_Orders_Non_Sub_Nor_Inc_op3, 2) * b.LDD_Orders_Sub_and_Inc_op3 AS Total_DF_Rev_Lost_Local_op3,

    ------------------------------------------------------------------------------------------------------------------------------------------------------

    -- Potential LD Extra Rev from introducing base and LDD fee
    c.Num_Orders_Sub_and_Inc_With_LD_Extra_Fee_op3,
    c.Tot_LD_Extra_Fee_Sub_and_Inc_Orders_Local_op3,
    c.Tot_LD_Extra_Fee_Sub_and_Inc_Orders_Eur_op3,

    ##--------------------------------------------------------------END OF OPTION 3 METRICS--------------------------------------------------------------##
FROM `dh-logistics-product-ops.pricing.ldd_analysis_raw_orders_with_base_and_ldd_fees` a
LEFT JOIN ldd_orders_prep_data b ON a.entity_id = b.entity_id
LEFT JOIN potential_ldd_rev_with_base_fee_differentiation c ON a.entity_id = c.entity_id
GROUP BY 1, b.Non_LDD_Orders_Log_Def_op1, b.LDD_Orders_Log_Def_op1, b.LDD_Orders_Non_Sub_Nor_Inc_op1, b.LDD_Orders_Sub_and_Inc_op1, c.Num_Orders_Sub_and_Inc_With_LD_Extra_Fee_op1, c.Tot_LD_Extra_Fee_Sub_and_Inc_Orders_Local_op1, c.Tot_LD_Extra_Fee_Sub_and_Inc_Orders_Eur_op1,
b.Non_LDD_Orders_Log_Def_op2, b.LDD_Orders_Log_Def_op2, b.LDD_Orders_Non_Sub_Nor_Inc_op2, b.LDD_Orders_Sub_and_Inc_op2, c.Num_Orders_Sub_and_Inc_With_LD_Extra_Fee_op2, c.Tot_LD_Extra_Fee_Sub_and_Inc_Orders_Local_op2, c.Tot_LD_Extra_Fee_Sub_and_Inc_Orders_Eur_op2,
b.Non_LDD_Orders_Log_Def_op3, b.LDD_Orders_Log_Def_op3, b.LDD_Orders_Non_Sub_Nor_Inc_op3, b.LDD_Orders_Sub_and_Inc_op3, c.Num_Orders_Sub_and_Inc_With_LD_Extra_Fee_op3, c.Tot_LD_Extra_Fee_Sub_and_Inc_Orders_Local_op3, c.Tot_LD_Extra_Fee_Sub_and_Inc_Orders_Eur_op3
ORDER BY 1;

SELECT * FROM `dh-logistics-product-ops.pricing.ldd_analysis_agg_data`