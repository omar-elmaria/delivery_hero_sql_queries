-- An Experiment with two target groups and no conditions at all
WITH temptbl AS (
    SELECT
        'MY_20220218_R_B0_P_LovedBrandsKlangValley' AS test_name,
        perseus_client_id,
        COUNT(DISTINCT target_group) Count_TGs_Per_Cust,
        COUNT(DISTINCT order_id) Count_Orders_Per_Cust,
        COUNT(DISTINCT variant) Count_Variants_Per_Cust,
    FROM `dh-logistics-product-ops.pricing.ab_test_individual_orders_loved_brands_my_klang_valley`
    GROUP BY 1,2
)

SELECT
    test_name,
    COUNT(DISTINCT CASE WHEN Count_Variants_Per_Cust = 0 THEN perseus_client_id ELSE NULL END) AS Customers_with_null_variant,
    COUNT(DISTINCT CASE WHEN Count_Variants_Per_Cust = 1 THEN perseus_client_id ELSE NULL END) AS Customers_with_one_variant,
    COUNT(DISTINCT CASE WHEN Count_Variants_Per_Cust > 1 THEN perseus_client_id ELSE NULL END) AS Customers_with_multiple_variant,
    
    SUM(CASE WHEN Count_Variants_Per_Cust = 0 THEN Count_Orders_Per_Cust ELSE NULL END) AS Orders_from_Customers_with_null_variant,
    SUM(CASE WHEN Count_Variants_Per_Cust = 1 THEN Count_Orders_Per_Cust ELSE NULL END) AS Orders_from_Customers_with_one_variant,
    SUM(CASE WHEN Count_Variants_Per_Cust > 1 THEN Count_Orders_Per_Cust ELSE NULL END) AS Orders_from_Customers_with_multiple_variants,
FROM temptbl
GROUP BY 1
ORDER BY 1;

#############################################################################################################################################################################################################
#####-------------------------------------------------------------------------------------------END OF PART 1-------------------------------------------------------------------------------------------#####
#############################################################################################################################################################################################################

-- Three Experiment with multiple target groups and a customer condition (single FDNC)
WITH temptbl AS (
    SELECT
        test_name,
        perseus_client_id,
        COUNT(DISTINCT target_group) Count_TGs_Per_Cust,
        COUNT(DISTINCT order_id) Count_Orders_Per_Cust,
        COUNT(DISTINCT variant) Count_Variants_Per_Cust,
    FROM `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_raw_orders` -- Contains three tests 'TH_2022_02_25_FDNC_ChiangMai', 'TH_2022_02_25_FDNC_Phuket', 'TH_2022_02_25_FDNC_Ratchaburi'
    GROUP BY 1,2
)

SELECT
    test_name,
    COUNT(DISTINCT CASE WHEN Count_Variants_Per_Cust = 0 THEN perseus_client_id ELSE NULL END) AS Customers_with_null_variant,
    COUNT(DISTINCT CASE WHEN Count_Variants_Per_Cust = 1 THEN perseus_client_id ELSE NULL END) AS Customers_with_one_variant,
    COUNT(DISTINCT CASE WHEN Count_Variants_Per_Cust > 1 THEN perseus_client_id ELSE NULL END) AS Customers_with_multiple_variant,
    
    SUM(CASE WHEN Count_Variants_Per_Cust = 0 THEN Count_Orders_Per_Cust ELSE NULL END) AS Orders_from_Customers_with_null_variant,
    SUM(CASE WHEN Count_Variants_Per_Cust = 1 THEN Count_Orders_Per_Cust ELSE NULL END) AS Orders_from_Customers_with_one_variant,
    SUM(CASE WHEN Count_Variants_Per_Cust > 1 THEN Count_Orders_Per_Cust ELSE NULL END) AS Orders_from_Customers_with_multiple_variants,
FROM temptbl
GROUP BY 1
ORDER BY 1;

-- Customers with more than one variant in FDNC experiments with multiple target groups and a customer condition (FDNC)
SELECT * 
FROM `dh-logistics-product-ops.pricing.fdnc_dashboard_ab_test_raw_orders` 
WHERE test_name = 'TH_2022_02_25_FDNC_ChiangMai' AND perseus_client_id IN ('1583077702690.750161726898466881.gh4AbxzNLv', '1609860234697.251939855583561763.FXTvVcbCNQ')
ORDER BY test_name, perseus_client_id;

#############################################################################################################################################################################################################
#####-------------------------------------------------------------------------------------------END OF PART 2-------------------------------------------------------------------------------------------#####
#############################################################################################################################################################################################################

-- An experiment with multiple target groups and a time condition
WITH temptbl AS (
    SELECT
        test_name,
        perseus_client_id,
        COUNT(DISTINCT target_group) Count_TGs_Per_Cust,
        COUNT(DISTINCT order_id) Count_Orders_Per_Cust,
        COUNT(DISTINCT variant) Count_Variants_Per_Cust,
    FROM `dh-logistics-product-ops.pricing.time_condition_exp_ab_test_raw_orders` -- Contains one test
    GROUP BY 1,2
)

SELECT
    test_name,
    COUNT(DISTINCT CASE WHEN Count_Variants_Per_Cust = 0 THEN perseus_client_id ELSE NULL END) AS Customers_with_null_variant,
    COUNT(DISTINCT CASE WHEN Count_Variants_Per_Cust = 1 THEN perseus_client_id ELSE NULL END) AS Customers_with_one_variant,
    COUNT(DISTINCT CASE WHEN Count_Variants_Per_Cust > 1 THEN perseus_client_id ELSE NULL END) AS Customers_with_multiple_variant,
    
    SUM(CASE WHEN Count_Variants_Per_Cust = 0 THEN Count_Orders_Per_Cust ELSE NULL END) AS Orders_from_Customers_with_null_variant,
    SUM(CASE WHEN Count_Variants_Per_Cust = 1 THEN Count_Orders_Per_Cust ELSE NULL END) AS Orders_from_Customers_with_one_variant,
    SUM(CASE WHEN Count_Variants_Per_Cust > 1 THEN Count_Orders_Per_Cust ELSE NULL END) AS Orders_from_Customers_with_multiple_variants,
FROM temptbl
GROUP BY 1
ORDER BY 1;

-----------------------------------------------PULLING THE RAW ORDERS FOR THE HK TEST-----------------------------------------------

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.time_condition_exp_ab_test_zone_ids` AS
SELECT DISTINCT
    entity_id,
    test_name,
    zone_id
FROM `fulfillment-dwh-production.cl.dps_experiment_setups` a
CROSS JOIN UNNEST(a.zone_ids) AS zone_id
WHERE TRUE
    AND test_name = 'HK_20220404_R_BDJ_P_PeakHourAdjustment'
ORDER BY 1,2;

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.time_condition_exp_ab_test_target_groups` AS
SELECT DISTINCT
    entity_id,
    test_name,
    vendor_group_id,
    vendor_id,
    CONCAT('Target Group ', DENSE_RANK() OVER (PARTITION BY entity_id, test_name ORDER BY vendor_group_id)) AS target_group
FROM `fulfillment-dwh-production.cl.dps_experiment_setups` a
CROSS JOIN UNNEST(a.matching_vendor_ids) AS vendor_id
WHERE TRUE
    AND test_name = 'HK_20220404_R_BDJ_P_PeakHourAdjustment'
ORDER BY 1,2,3,4;

-- Step 4: Pull the business and logisitcal KPIs from "dps_sessions_mapped_to_orders_v2" INNER JOINED on "dps_ab_test_orders_v2" to get the relevant test orders
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.time_condition_exp_ab_test_raw_orders` AS
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
        AND p.created_date BETWEEN DATE('2022-04-04') AND CURRENT_DATE() -- For partitioning elimination and speeding up the query
        AND o.created_date BETWEEN DATE('2022-04-04') AND CURRENT_DATE() -- For partitioning elimination and speeding up the query
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
    ov2.test_name,
    ov2.treatment,
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
    COALESCE(tg.target_group, 'Non_TG') AS target_group,
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
    ROW_NUMBER() OVER (PARTITION BY ent.region, a.entity_id, ov2.test_name, a.variant, a.perseus_client_id ORDER BY a.order_placed_at) AS order_rank,

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
-- IMPORTANT NOTE: INNER JOIN because we want to pull the relevant test data
INNER JOIN `dh-logistics-product-ops.pricing.time_condition_exp_ab_test_zone_ids` zn ON a.entity_id = zn.entity_id AND a.zone_id = zn.zone_id -- Filter for orders from vendors in the target zones
INNER JOIN `fulfillment-dwh-production.cl.dps_ab_test_orders_v2` ov2 ON a.entity_id = ov2.entity_id AND a.order_id = ov2.order_id -- Filter for orders that belong to the tests chosen at the start of the test
INNER JOIN entities ent ON a.entity_id = ent.entity_id -- INNER JOIN to only include active DH entities
LEFT JOIN `dh-logistics-product-ops.pricing.time_condition_exp_ab_test_target_groups` tg ON ov2.entity_id = tg.entity_id AND ov2.test_name = tg.test_name AND ov2.vendor_id = tg.vendor_id
WHERE TRUE
    AND a.entity_id = 'FP_HK'
    AND a.created_date BETWEEN DATE('2022-04-04') AND CURRENT_DATE()
    -- AND a.variant NOT IN ('Original') AND a.variant IS NOT NULL
    AND a.is_own_delivery -- OD or MP
    AND a.delivery_status = 'completed'
    AND ov2.test_name = 'HK_20220404_R_BDJ_P_PeakHourAdjustment' -- Successful orders
    AND perseus_client_id IS NOT NULL AND perseus_client_id != ''; -- Eliminate blank and NULL perseus client IDs
    -- AND a.vertical_type IN UNNEST(v_type) -- Orders from a particular vertical (restuarants, groceries, darkstores, etc.). Commented out as some FDNC tests may target verticals different from restaurants
