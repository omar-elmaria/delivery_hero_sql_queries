-- NOTE: V2 includes the multiple FDNC counter-factual "manual_order_rank_aas_grouped_mult_fdnc" for the countries which currently use the feature AND an improved way to filter for qualified orders to speed up querying (INNER JOIN instead of CASE WHEN) 
-- 20220512: Added the Google Data Studio metrics (FP ratio and FP rate)
-- V3 contains the new subscription table

-- Step 1: Declare the input variables used throughout the script
DECLARE v_type, od_status STRING;
DECLARE start_date, end_date DATE;
SET (v_type, od_status) = ('restaurants', 'OWN_DELIVERY'); -- Name of the test in the AB test dashboard/DPS experiments tab
SET (start_date, end_date) = (DATE('2022-01-01'), DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY));

##----------------------------------------------------------------END OF THE INPUT SECTION----------------------------------------------------------------##

-- Step 2: Identify vendors that have the **customer condition** configured to them in DPS. These are the vendors for which DPS will ask for info from the CIA service
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.fdnc_impact_analysis_vendor_ids_with_cust_condition` AS
WITH pandora_entities AS ( -- A sub-query to get Pandora entities
    SELECT DISTINCT
        cl.region,	
        o.global_entity_id 
    FROM `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` o
    INNER JOIN `fulfillment-dwh-production.cl.countries` cl ON RIGHT(o.global_entity_id,2) = cl.country_iso AND cl.country_iso IS NOT NULL 
    WHERE (o.global_entity_id LIKE 'FP%' OR o.global_entity_id IN ('PO_FI', 'OP_SE', 'FP_DE', 'FP_RO', 'FO_NO', 'DJ_CZ', 'FP_SK', 'EF_GR', 'FY_CY', 'MJM_AT')) AND o.global_entity_id NOT IN ('FP_JP', 'FP_DE') -- The second condition provides the European entities
),

num_orders_with_cust_tags AS ( -- A sub-query to count the number of orders per vendor with 'New', 'Existing', or 'NULL' customer_tag
    SELECT
        entity_id,
        vendor_id,
        COUNT(DISTINCT CASE WHEN dps_customer_tag = 'New' THEN order_id ELSE NULL END) AS orders_with_new_tag,
        COUNT(DISTINCT CASE WHEN dps_customer_tag = 'Existing' THEN order_id ELSE NULL END) AS orders_with_existing_tag,
        COUNT(DISTINCT CASE WHEN dps_customer_tag IS NULL THEN order_id ELSE NULL END) AS orders_with_null_tag,
    FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` a
    INNER JOIN pandora_entities b ON a.entity_id = b.global_entity_id
    WHERE TRUE
        AND a.created_date BETWEEN start_date AND end_date
        AND a.is_own_delivery -- OD or MP
        AND a.vertical_type = v_type -- Orders from a particular vertical (restuarants, groceries, darkstores, etc.)
        AND a.delivery_status = 'completed' -- Successful orders
    GROUP BY 1,2
)

SELECT *
FROM num_orders_with_cust_tags
WHERE orders_with_new_tag >= 1 OR orders_with_existing_tag >= 1;

##----------------------------------------------------------------END OF THE VENDOR SELECTION SECTION----------------------------------------------------------------##

-- Step 3: Filter only for orders that will be evaluated (i.e. orders from vendors that had the customer condition ON)
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.fdnc_impact_analysis_relevant_orders_from_vendors_with_cust_condition` AS
SELECT 
    a.*,
FROM `dh-logistics-product-ops.pricing.new_cust_identification_false_positive_analysis_no_data_loss_all_pandora_with_adjust_and_perseus_all_orders_multiple_fdnc` a
INNER JOIN `dh-logistics-product-ops.pricing.fdnc_impact_analysis_vendor_ids_with_cust_condition` b ON a.entity_id = b.entity_id AND a.vendor_id = b.vendor_id;

##----------------------------------------------------------------END OF THE RELEVANT ORDERS SECTION----------------------------------------------------------------##

-- Step 4.1: Active DH entities
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.fdnc_impact_analysis_active_entities` AS 
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
    AND p.entity_id NOT IN ('CD_CO'); -- Eliminate irrelevant entity_ids in LATAM

-- Step 4.2: Get the active OD restaurant vendors in each entity
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.fdnc_impact_analysis_active_od_resto_vendor_count` AS 
WITH tot_ven_entity AS (
    SELECT DISTINCT
        v.entity_id,
        COUNT(DISTINCT v.vendor_code) AS Total_vendors_entity
    FROM `fulfillment-dwh-production.cl.vendors_v2` v
    LEFT JOIN UNNEST(v.vendor) ven
    LEFT JOIN UNNEST(v.dps) dps
    LEFT JOIN UNNEST(v.hurrier) hur
    LEFT JOIN UNNEST(v.zones) z
    CROSS JOIN UNNEST(delivery_provider) AS delivery_type -- "delivery_provider" is an array that sometimes contains multiple elements, so we need to unnest it and break it down to its individual components
    WHERE TRUE 
        AND v.is_active -- Active vendors
        AND v.vertical_type = v_type -- Restaurants vertical only
        AND delivery_type = od_status -- Filter for OD vendors
    GROUP BY 1
)

-- Step 5: Find the share of active OD restaurant vendors with FDNC feature
SELECT
    ent.region,
    a.entity_id,
    b.Total_vendors_entity,
    COUNT(DISTINCT a.vendor_id) AS FDNC_vendors,
    COUNT(DISTINCT a.vendor_id) / b.Total_vendors_entity AS FDNC_vendor_share,
FROM `dh-logistics-product-ops.pricing.fdnc_impact_analysis_vendor_ids_with_cust_condition` a
LEFT JOIN tot_ven_entity b ON a.entity_id = b.entity_id
LEFT JOIN `dh-logistics-product-ops.pricing.fdnc_impact_analysis_active_entities` ent ON a.entity_id = ent.entity_id -- INNER JOIN to only include active DH entities
GROUP BY 1,2, b.Total_vendors_entity
ORDER BY 1,2;

##----------------------------------------------------------------END OF SHARE OF VENDORS WITH FDNC SECTION----------------------------------------------------------------##

-- Raw orders dataset
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.fdnc_impact_analysis_raw_orders` AS
WITH is_fd_sub_order AS (
    SELECT 
        ent.region,
        b.dps_sessionid,
        b.ga_session_id,
        a.* EXCEPT(
            is_acquisition, customer_order_rank, customer_order_rank_grouped, manual_order_rank_short, manual_order_rank_short_grouped, manual_order_rank_long, manual_order_rank_long_grouped,
            days_since_first_order_cust_acc_id, days_since_first_order_adjust_id, days_since_first_order_perseus_id, manual_order_rank_adjust, manual_order_rank_perseus,
            manual_order_rank_aas_grouped, manual_order_rank_aas_grouped_mult_fdnc
        ),
        manual_order_rank_short, manual_order_rank_short_grouped, manual_order_rank_long, manual_order_rank_long_grouped,
        manual_order_rank_adjust, manual_order_rank_perseus,
        manual_order_rank_aas_grouped, manual_order_rank_aas_grouped_mult_fdnc,
        GREATEST(IFNULL(a.days_since_first_order_adjust_id,0), a.days_since_first_order_cust_acc_id, IFNULL(a.days_since_first_order_perseus_id,0), 0) AS days_since_first_order_counter_factual,
        GREATEST(IFNULL(a.manual_order_rank_adjust,0), a.manual_order_rank_long, IFNULL(a.manual_order_rank_perseus,0), 0) AS order_rank_counter_factual,
        days_since_first_order_cust_acc_id, days_since_first_order_adjust_id, days_since_first_order_perseus_id,
        CASE WHEN a.manual_order_rank_aas_grouped_mult_fdnc = '> 1 - Old' AND a.dps_customer_tag = 'New' THEN 'Yes' ELSE 'No' END AS Incorrectly_Identified_New_Cust,
        pd.delivery_fee_local AS actual_df_paid_by_customer_local,
        pd.delivery_fee_local / b.exchange_rate AS actual_df_paid_by_customer_eur,
        CASE WHEN b.is_delivery_fee_covered_by_discount = FALSE AND b.is_delivery_fee_covered_by_voucher = FALSE THEN 'No DF Voucher/Discount' ELSE 'DF Voucher/Discount' END AS df_voucher_flag,
        CASE WHEN dps_standard_fee_local = 0 THEN 'Std Fee = 0' WHEN dps_standard_fee_local > 0 THEN 'Std Fee > 0' ELSE NULL END AS standard_fee_flag, -- Only two possible values --> > 0, or NULL
        -- CASE WHEN basket_value_current_fee_value = 0 THEN 'Basket Current Fee Value = 0' ELSE 'Basket Current Fee Value != 0' END AS basket_current_fee_value_flag, -- Only three possible values --> -ve, 0, or NULL
        COALESCE(pdos_ap.is_free_delivery_subscription_order, pdos_eu.is_free_delivery_subscription_order) AS is_free_delivery_subscription_order,
        b.order_id AS order_id_smto,
        b.experiment_id,
        b.variant,
        b.scheme_id,
    FROM `dh-logistics-product-ops.pricing.fdnc_impact_analysis_relevant_orders_from_vendors_with_cust_condition` a
    LEFT JOIN `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` b ON a.entity_id = b.entity_id AND a.order_id = b.platform_order_code
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` pd ON a.entity_id = pd.global_entity_id AND a.order_id = pd.code AND a.created_date = pd.created_date_utc -- Contains info on the orders in Pandora countries
    LEFT JOIN `fulfillment-dwh-production.pandata_report.regional_apac_pd_orders_agg_sb_subscriptions` pdos_ap
        ON pd.uuid = pdos_ap.uuid AND pd.created_date_utc = pdos_ap.created_date_utc AND (pdos_ap.created_date_utc BETWEEN start_date AND end_date) -- The last condition is needed for partition elimination
    LEFT JOIN `fulfillment-dwh-production.pandata_report.regional_eu__pd_orders_agg_sb_subscriptions` pdos_eu ON pd.uuid = pdos_eu.uuid AND pd.created_date_utc = pdos_eu.created_date_utc
    LEFT JOIN `dh-logistics-product-ops.pricing.fdnc_impact_analysis_active_entities` ent ON a.entity_id = ent.entity_id
    WHERE TRUE 
        AND a.created_date BETWEEN start_date AND end_date
        AND b.order_id IS NOT NULL
)

SELECT 
    *,
    CASE WHEN is_free_delivery_subscription_order = TRUE THEN 'Subscription FD Order' ELSE 'Non-Subscription FD Order' END AS fd_subscription_flag, -- Only two possible values --> True or False
FROM is_fd_sub_order;

-- Add one more column to the raw orders dataset
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.fdnc_impact_analysis_raw_orders` AS
SELECT 
    *,
    CASE WHEN 
        (dps_customer_tag = 'New' 
        AND Incorrectly_Identified_New_Cust = 'Yes' 
        -- Conditions where the customer would've paid a 0 DF anyway
        AND df_voucher_flag = 'No DF Voucher/Discount'
        AND standard_fee_flag = 'Std Fee > 0'
        AND fd_subscription_flag = 'Non-Subscription FD Order'
        AND actual_df_paid_by_customer_local = 0)
    THEN 'Yes' 
    ELSE 'No' END AS Is_Incorrectly_Granted_FDNC,
FROM `dh-logistics-product-ops.pricing.fdnc_impact_analysis_raw_orders`;

-- Aggregated dataset
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.fdnc_impact_analysis_tableau_dataset` AS
SELECT
    a.region,
    a.entity_id,
    a.created_date,
    -- Number of orders
    SUM(CASE WHEN order_id_smto IS NOT NULL THEN 1 ELSE 0 END) AS Orders_wout_mapping_issues,
    SUM(CASE WHEN order_id_smto IS NULL THEN 1 ELSE 0 END) AS Orders_with_mapping_issues,
    COUNT(DISTINCT order_id) AS Total_Order_Count,
    COUNT(DISTINCT CASE WHEN dps_customer_tag = 'New' THEN order_id ELSE NULL END) AS New_Customer_Order_Count,
    COUNT(DISTINCT CASE
        WHEN 
            dps_customer_tag = 'New' 
            AND Incorrectly_Identified_New_Cust = 'Yes' 
            -- Conditions where the customer would've paid a 0 DF anyway
            AND df_voucher_flag = 'No DF Voucher/Discount'
            AND actual_df_paid_by_customer_local = 0 
        THEN order_id 
    ELSE NULL END) AS Incorrectly_Granted_FDNC_Order_Count_Old_Logic, -- Without extra conditions

    COUNT(DISTINCT CASE
        WHEN 
            dps_customer_tag = 'New' 
            AND Incorrectly_Identified_New_Cust = 'Yes' 
            -- Conditions where the customer would've paid a 0 DF anyway
            AND df_voucher_flag = 'No DF Voucher/Discount'
            AND standard_fee_flag = 'Std Fee > 0'
            AND fd_subscription_flag = 'Non-Subscription FD Order'
            -- AND basket_current_fee_value_flag = 'Basket Current Fee Value != 0'
            AND actual_df_paid_by_customer_local = 0 
        THEN order_id 
    ELSE NULL END) AS Incorrectly_Granted_FDNC_Order_Count,
    
    -- DF Revenue
    SUM(CASE WHEN actual_df_paid_by_customer_local > 0 THEN actual_df_paid_by_customer_eur ELSE NULL END) AS Total_DF_Non_FD_Orders,
    COUNT(DISTINCT CASE WHEN actual_df_paid_by_customer_local > 0 THEN order_id ELSE NULL END) AS Order_Count_Non_FD_Orders,

    -- MULTIPLE FDNC METRICS (Correctly_Identified_New_Cust_Mult_FDNC and Incorrectly_Identified_New_Cust_Mult_FDNC are not needed anymore because we calculate them above)
    -- Number of Orders
    COUNT(DISTINCT CASE WHEN manual_order_rank_aas_grouped_mult_fdnc = '> 1 - Old' AND dps_customer_tag = 'Existing' THEN order_id ELSE NULL END) AS Correctly_Identified_Old_Cust_Mult_FDNC, -- True negatives
    COUNT(DISTINCT CASE WHEN manual_order_rank_aas_grouped_mult_fdnc = '1 - New' AND dps_customer_tag = 'Existing' THEN order_id ELSE NULL END) AS Incorrectly_Identified_Old_Cust_Mult_FDNC, -- False negatives

    -- Vendor Count
    b.Total_vendors_entity AS Total_OD_resto_vendors,
    b.FDNC_vendors AS FDNC_OD_resto_vendors,
FROM `dh-logistics-product-ops.pricing.fdnc_impact_analysis_raw_orders` a
LEFT JOIN `dh-logistics-product-ops.pricing.fdnc_impact_analysis_active_od_resto_vendor_count` b ON a.entity_id = b.entity_id
GROUP BY 1,2,3, b.Total_vendors_entity, b.FDNC_vendors, b.FDNC_vendor_share;

-- Add the Data Studio metrics (FP Rate and FP Ratio)
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.fdnc_impact_analysis_tableau_dataset` AS
SELECT 
    *,
    
    -- FP and FN Rates
    Incorrectly_Granted_FDNC_Order_Count / -- False positives
    NULLIF(
        Correctly_Identified_Old_Cust_Mult_FDNC + -- True negatives
        Incorrectly_Granted_FDNC_Order_Count -- False positives
    , 0) AS Daily_FP_Rate_Mult_FDNC,
    
    Incorrectly_Identified_Old_Cust_Mult_FDNC / -- False negatives
    NULLIF(
        (New_Customer_Order_Count - Incorrectly_Granted_FDNC_Order_Count) + -- True Positives
        Incorrectly_Identified_Old_Cust_Mult_FDNC -- False negatives
    , 0) AS Daily_FN_Rate_Mult_FDNC,

    -- FP Ratio
    Incorrectly_Granted_FDNC_Order_Count / NULLIF((New_Customer_Order_Count - Incorrectly_Granted_FDNC_Order_Count), 0) AS FP_Ratio_Mult_FDNC
FROM `dh-logistics-product-ops.pricing.fdnc_impact_analysis_tableau_dataset`
ORDER BY 1,2,3;