-- NOTE: V7 includes the correctly configured multiple FDNC condition for the countries which currently use the feature AND an improved way to filter for qualified orders to speed up querying (INNER JOIN instead of CASE WHEN)
-- V8 --> Replaced `datachef-production.facts.orders` with `dhub-cdp.facts.orders` and removed the condition "AND customer_channel IN ('ios', 'android')"
-- PART 1: Identify vendors that are assigned to **automatic scheme assignments (ASA)** and have the **customer condition** configured to them in DPS. These are the vendors for which DPS will ask for info from the CIA service

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.pandora_entities` AS
SELECT DISTINCT
    cl.region,	
    o.global_entity_id 
FROM `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` o
INNER JOIN `fulfillment-dwh-production.cl.countries` cl ON RIGHT(o.global_entity_id,2) = cl.country_iso AND cl.country_iso IS NOT NULL 
WHERE (o.global_entity_id LIKE 'FP%' OR o.global_entity_id IN ('PO_FI', 'OP_SE', 'FP_DE', 'FP_RO', 'FO_NO', 'DJ_CZ', 'FP_SK', 'EF_GR', 'FY_CY', 'MJM_AT')) AND o.global_entity_id NOT IN ('FP_JP', 'FP_DE'); -- The second condition provides the European entities

-- Pull the ASA IDs with a non-null customer condition
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.new_cust_identification_vendor_ids_with_cust_condition_all_pandora_multiple_fdnc` AS
WITH asa_ids AS (
    SELECT
        * 
    FROM `fulfillment-dwh-production.dl.dynamic_pricing_vendor_group_price_config`
    WHERE TRUE 
        AND global_entity_id IN (SELECT global_entity_id FROM `dh-logistics-product-ops.pricing.pandora_entities`) -- HERE
        AND customer_condition_id IS NOT NULL
),

-- Pull the data of vendors under the ASA IDs extracted in the first sub-query above
vendor_list AS (
    SELECT DISTINCT
        v.entity_id,
        v.vendor_code,
        delivery_type,
        dps.assignment_type,
        CASE WHEN dps.assignment_type = 'Manual' THEN 'A' WHEN dps.assignment_type = 'Automatic' THEN 'B' WHEN dps.assignment_type = 'Country Fallback' THEN 'C' END AS assignment_type_sorting_col,
    FROM `fulfillment-dwh-production.cl.vendors_v2` v
    LEFT JOIN UNNEST(vendor) ven
    LEFT JOIN UNNEST(dps) dps
    LEFT JOIN UNNEST(zones) z
    INNER JOIN asa_ids asa ON v.entity_id = asa.global_entity_id AND dps.scheme_id = asa.price_scheme_id -- INNER JOIN is a MUST here
    CROSS JOIN UNNEST(delivery_provider) AS delivery_type -- "delivery_provider" is an array that sometimes contains multiple elements, so we need to unnest it and break it down to its individual components
    WHERE TRUE
        AND v.entity_id IN (SELECT global_entity_id FROM `dh-logistics-product-ops.pricing.pandora_entities`) -- HERE
        AND v.is_active -- Filter for active vendors only
        AND dps.assignment_type IS NOT NULL -- Eliminate records where the assignment type is not stored
),

vendor_list_prio_logic AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY entity_id, vendor_code ORDER BY assignment_type_sorting_col) AS RowNum -- Sorts the scheme assignments according to the prio logic --> Manual, Automatic, Country Fallback
    FROM vendor_list
),

vendor_list_filtered AS (
    SELECT *
    FROM vendor_list_prio_logic
    WHERE RowNum = 1 -- Selects the highest prio scheme assignment
)

SELECT
    a.*,
    b.location 
FROM vendor_list_filtered a
LEFT JOIN `fulfillment-dwh-production.cl.vendors_v2` b USING(entity_id, vendor_code)
WHERE assignment_type = 'Automatic' -- Selects vendors with ASA ONLY as these are the ones that have the customer condition linked to them. Vendors with manual scheme assignments do NOT have this customer condition, meaning that DPS does not ask CIA for info for these vendors
ORDER By 1,2,3;

--------------------------------------------------------------------------------------END OF PART 1--------------------------------------------------------------------------------------

-- PART 2: Pull the orders associated with the vendors in "pricing.new_cust_identification_vendor_ids_with_cust_condition_all_pandora_multiple_fdnc" and get the purchase history per customer account

-- Pull the customer account IDs in ALL countries from Oct 15th until today
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.new_cust_identification_cust_ids_all_pandora_multiple_fdnc` AS
SELECT DISTINCT 
    o.global_entity_id,
    o.customer_account_id
FROM `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` o
WHERE TRUE
    --AND o.is_own_delivery IS TRUE -- Uncomment if you want to focus on OD orders only
    --AND o.is_qcommerce = FALSE -- Uncomment if you want to filter for the restaurants vertical
    AND o.global_entity_id IN (SELECT global_entity_id FROM `dh-logistics-product-ops.pricing.pandora_entities`) -- HERE: Malaysia and SG
    AND DATE(o.placed_at) BETWEEN DATE('2021-10-15') AND CURRENT_DATE() -- Set the timeline from Oct 15th
    AND o.customer_account_id IS NOT NULL -- Customer account ID is not empty
    AND o.is_sent; -- Successful orders

-- Pull the order history in ALL countries
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.new_cust_identification_false_positive_analysis_no_data_loss_all_pandora_multiple_fdnc` AS
WITH temptbl AS (
    SELECT
        o.global_entity_id AS entity_id,
        o.customer_account_id,
        DATE(o.placed_at) AS created_date, -- Order date
        o.placed_at AS order_placed_at, -- Order time stamp
        o.order_id,
        CASE 
            WHEN dps.platform_order_code IS NULL THEN 'Not Mapped'
            WHEN dps.platform_order_code IS NOT NULL AND dps_customer_tag IS NULL THEN 'NULL'
            ELSE dps_customer_tag
        END AS dps_customer_tag, -- A flag that distinguishes between new and old users. The oldest date in dps_sessions_mapped_to_orders_v2 is Aug 1st 2020, so dps_customer_tag does not exist before this date
        o.is_acquisition, -- Another flag that distinguishes between new and old users. Can be used when dps_customer_tag is faulty
        o.customer_order_rank, -- A standard column in "curated_data_shared_central_dwh.orders". It considers MP and OD and goes back to 2011
        o.vendor_id,
        dps.order_id AS order_id_dps,
        dps.platform_order_code AS platform_order_code_dps
    FROM `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` o
    LEFT JOIN `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` dps ON dps.entity_id = o.global_entity_id AND dps.platform_order_code = o.order_id
    INNER JOIN `dh-logistics-product-ops.pricing.new_cust_identification_cust_ids_all_pandora_multiple_fdnc` cus ON o.global_entity_id = cus.global_entity_id AND o.customer_account_id = cus.customer_account_id
    WHERE TRUE 
        --AND o.is_own_delivery IS TRUE -- Uncomment if you want to focus on OD orders only
        --AND o.customer_account_id IN (SELECT customer_account_id FROM `dh-logistics-product-ops.pricing.new_cust_identification_cust_ids_all_pandora_multiple_fdnc`)
        --AND o.is_qcommerce = FALSE -- Uncomment if you want to filter for the restaurants vertical
        AND o.global_entity_id IN (SELECT global_entity_id FROM `dh-logistics-product-ops.pricing.pandora_entities`) -- HERE: MY and SG
        AND DATE(o.placed_at) >= DATE('2011-02-02') -- HERE: Set the timeline from way back in the past (2011-02-01). This is the oldest date in "curated_data_shared_central_dwh.orders"
        AND o.is_sent -- Successful order
),

temptbl_with_manual_rank AS (
    SELECT 
        *,
        -- In addition to the already existing order rank column, we create a user-defined order rank column that begins counting orders from 2019-10-11. 
        -- This is because CIA would not consider orders associated with a customer_account_id before a particular date, which we assume to be 2 years ago
        -- In the final analysis, we compare the rate of FPs and FNs using both the "customer_order_rank" and the user-defined order rank
        CASE WHEN created_date < DATE('2019-10-11') THEN 0 ELSE ROW_NUMBER() OVER (PARTITION BY entity_id, customer_account_id, CASE WHEN created_date < DATE('2019-10-11') THEN 1 ELSE 0 END ORDER BY order_placed_at) END AS manual_order_rank_short,
        ROW_NUMBER() OVER (PARTITION BY entity_id, customer_account_id ORDER BY order_placed_at) AS manual_order_rank_long,
    FROM temptbl
)

SELECT 
    *,
    CASE WHEN customer_order_rank = 1 THEN "1 - New" ELSE "> 1 - Old" END AS customer_order_rank_grouped,
    CASE WHEN manual_order_rank_short = 0 THEN 'NA' WHEN manual_order_rank_short = 1 THEN "1 - New" ELSE "> 1 - Old" END AS manual_order_rank_short_grouped,
    CASE WHEN manual_order_rank_long = 0 THEN 'NA' WHEN manual_order_rank_long = 1 THEN "1 - New" ELSE "> 1 - Old" END AS manual_order_rank_long_grouped,
FROM temptbl_with_manual_rank
ORDER BY 1,2,4;

-----------------------------------------------------------------------------------------------------------------------------------------------------

-- Adding perseus_id and adjust_id to the dataset (USING Mariia's way), which takes into consideration account associations

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.new_cust_identification_false_positive_analysis_no_data_loss_all_pandora_with_adjust_and_perseus_all_orders_multiple_fdnc` AS
WITH add_adjust_perseus AS (
    SELECT
        a.entity_id,
        b.adjust_id,
        b.perseus_id,
        CAST(b.placed_at_local AS TIMESTAMP) AS created_timestamp,
        a.* EXCEPT(entity_id)
    FROM `dh-logistics-product-ops.pricing.new_cust_identification_false_positive_analysis_no_data_loss_all_pandora_multiple_fdnc` a
    LEFT JOIN `dhub-cdp.facts.orders` b 
    ON a.entity_id = b.global_entity_id AND a.order_id = b.order_id AND a.created_date >= DATE('2021-01-01')
),

order_ranks_adjust AS (
    SELECT 
        entity_id, 
        order_id,
        adjust_id,
        created_timestamp,
        CASE 
            WHEN created_date < DATE('2021-01-01') THEN 0 
            ELSE ROW_NUMBER() OVER (
                PARTITION BY 
                    entity_id, 
                    adjust_id,
                    CASE WHEN created_date < DATE('2021-01-01') THEN 1 ELSE 0 END -- Starts counting from 2021-01-01 because that's when perseus and adjust started getting populated
                ORDER BY order_placed_at
            ) 
        END AS manual_order_rank_adjust
    FROM add_adjust_perseus
    WHERE adjust_id IS NOT NULL 
),

order_ranks_perseus AS (
        SELECT
        entity_id, 
        order_id,
        perseus_id,
        created_timestamp,
        CASE 
            WHEN created_date < DATE('2021-01-01') THEN 0 
            ELSE ROW_NUMBER() OVER (
                PARTITION BY 
                    entity_id, 
                    perseus_id,
                    CASE WHEN created_date < DATE('2021-01-01') THEN 1 ELSE 0 END -- Starts counting from 2021-01-01 because that's when perseus and adjust started getting populated
                ORDER BY order_placed_at
            ) END AS manual_order_rank_perseus
    FROM add_adjust_perseus
    WHERE perseus_id IS NOT NULL
),

days_since_first_order_for_multiple_fdnc AS (
    SELECT 
        a.*,
        ROUND(DATE_DIFF(a.order_placed_at, d.order_placed_at, HOUR) / 24, 2) AS days_since_first_order_cust_acc_id, -- We calculate the differences between placed_at and first_order_placed_at in hours, then divide by 24 so that we can calculate the exact no. of days since the 1st order. We do that for the three identifiers we have
        ROUND(DATE_DIFF(a.order_placed_at, e.created_timestamp, HOUR) / 24, 2) AS days_since_first_order_adjust_id, -- We calculate the differences between placed_at and first_order_placed_at in hours, then divide by 24 so that we can calculate the exact no. of days since the 1st order. We do that for the three identifiers we have
        ROUND(DATE_DIFF(a.order_placed_at, f.created_timestamp, HOUR) / 24, 2) AS days_since_first_order_perseus_id, -- We calculate the differences between placed_at and first_order_placed_at in hours, then divide by 24 so that we can calculate the exact no. of days since the 1st order. We do that for the three identifiers we have
    FROM add_adjust_perseus a
    -- Multiple FDNC joins (join the first order's info on the entire dataset)
    LEFT JOIN add_adjust_perseus d ON a.entity_id = d.entity_id AND a.customer_account_id = d.customer_account_id AND d.manual_order_rank_long = 1
    LEFT JOIN order_ranks_adjust e ON a.entity_id = e.entity_id AND a.adjust_id = e.adjust_id AND e.manual_order_rank_adjust = 1
    LEFT JOIN order_ranks_perseus f ON a.entity_id = f.entity_id AND a.perseus_id = f.perseus_id AND f.manual_order_rank_perseus = 1
)

SELECT 
    a.*,
    b.manual_order_rank_adjust,
    c.manual_order_rank_perseus,
    CASE 
        WHEN GREATEST(IFNULL(b.manual_order_rank_adjust,0), a.manual_order_rank_long, IFNULL(c.manual_order_rank_perseus,0)) = 0 THEN 'NA' 
        WHEN GREATEST(IFNULL(b.manual_order_rank_adjust,0), a.manual_order_rank_long, IFNULL(c.manual_order_rank_perseus,0), 0) = 1 THEN "1 - New" ELSE "> 1 - Old" 
    END AS manual_order_rank_aas_grouped,
    -- Example of a multiple FDNC condition
    CASE 
        WHEN a.entity_id IN ('FP_TH', 'FP_PH') THEN 
            (CASE 
                WHEN GREATEST(IFNULL(b.manual_order_rank_adjust,0), a.manual_order_rank_long, IFNULL(c.manual_order_rank_perseus,0)) = 0 THEN 'NA' 
                WHEN 
                    GREATEST(IFNULL(a.days_since_first_order_adjust_id,0), a.days_since_first_order_cust_acc_id, IFNULL(a.days_since_first_order_perseus_id,0), 0) <= 5 -- Within 5 days
                    AND
                    GREATEST(IFNULL(b.manual_order_rank_adjust,0), a.manual_order_rank_long, IFNULL(c.manual_order_rank_perseus,0), 0) <= 2 -- Two free deliveries
                THEN "1 - New" 
                ELSE "> 1 - Old" 
            END)
    ELSE ( -- Single FDNC for all other countries without the multiple FDNC condition
        CASE 
            WHEN GREATEST(IFNULL(b.manual_order_rank_adjust,0), a.manual_order_rank_long, IFNULL(c.manual_order_rank_perseus,0)) = 0 THEN 'NA' 
            WHEN GREATEST(IFNULL(b.manual_order_rank_adjust,0), a.manual_order_rank_long, IFNULL(c.manual_order_rank_perseus,0), 0) = 1 THEN "1 - New" ELSE "> 1 - Old" 
        END
    )
    END AS manual_order_rank_aas_grouped_mult_fdnc,
FROM days_since_first_order_for_multiple_fdnc a
LEFT JOIN order_ranks_adjust b ON a.entity_id = b.entity_id AND a.order_id = b.order_id
LEFT JOIN order_ranks_perseus c ON a.entity_id = c.entity_id AND a.order_id = c.order_id
ORDER BY 1,2,4;

-----------------------------------------------------------------------------------------------------------------------------------------------------

-- Filter only for orders that will be evaluated (i.e. orders from vendors that had the customer condition ON)
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.new_cust_identification_false_positive_analysis_no_data_loss_all_pandora_with_adjust_and_perseus_multiple_fdnc` AS
SELECT 
    a.*,
FROM `dh-logistics-product-ops.pricing.new_cust_identification_false_positive_analysis_no_data_loss_all_pandora_with_adjust_and_perseus_all_orders_multiple_fdnc` a
INNER JOIN `dh-logistics-product-ops.pricing.new_cust_identification_vendor_ids_with_cust_condition_all_pandora_multiple_fdnc` b ON a.entity_id = b.entity_id AND a.vendor_id = b.vendor_code;

-- Create the table that shows the TPs, FPs, TNs, and FNs, in addition to the FP, FNs rate, and FP ratio
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.cust_identification_results_all_pandora_multiple_fdnc` AS
SELECT
    entity_id,
    created_date,
    -- Number of Orders
    COUNT(DISTINCT CASE WHEN manual_order_rank_aas_grouped = '1 - New' AND dps_customer_tag = 'New' THEN order_id ELSE NULL END) AS Correctly_Identified_New_Cust,
    COUNT(DISTINCT CASE WHEN manual_order_rank_aas_grouped = '> 1 - Old' AND dps_customer_tag = 'New' THEN order_id ELSE NULL END) AS Incorrectly_Identified_New_Cust,
    COUNT(DISTINCT CASE WHEN manual_order_rank_aas_grouped = '> 1 - Old' AND dps_customer_tag = 'Existing' THEN order_id ELSE NULL END) AS Correctly_Identified_Old_Cust,
    COUNT(DISTINCT CASE WHEN manual_order_rank_aas_grouped = '1 - New' AND dps_customer_tag = 'Existing' THEN order_id ELSE NULL END) AS Incorrectly_Identified_Old_Cust,
    
    -- FP and FN Rates
    COUNT(DISTINCT CASE WHEN manual_order_rank_aas_grouped = '> 1 - Old' AND dps_customer_tag = 'New' THEN order_id ELSE NULL END) /
    NULLIF(
        COUNT(DISTINCT CASE WHEN manual_order_rank_aas_grouped = '> 1 - Old' AND dps_customer_tag = 'Existing' THEN order_id ELSE NULL END) +
        COUNT(DISTINCT CASE WHEN manual_order_rank_aas_grouped = '> 1 - Old' AND dps_customer_tag = 'New' THEN order_id ELSE NULL END)
    , 0) AS Daily_FP_Rate,
    
    COUNT(DISTINCT CASE WHEN manual_order_rank_aas_grouped = '1 - New' AND dps_customer_tag = 'Existing' THEN order_id ELSE NULL END) / 
    NULLIF(
        COUNT(DISTINCT CASE WHEN manual_order_rank_aas_grouped = '1 - New' AND dps_customer_tag = 'New' THEN order_id ELSE NULL END) +
        COUNT(DISTINCT CASE WHEN manual_order_rank_aas_grouped = '1 - New' AND dps_customer_tag = 'Existing' THEN order_id ELSE NULL END)
    , 0) AS Daily_FN_Rate,

    -- FP Ratio
    COUNT(DISTINCT CASE WHEN manual_order_rank_aas_grouped = '> 1 - Old' AND dps_customer_tag = 'New' THEN order_id ELSE NULL END) / NULLIF(COUNT(DISTINCT CASE WHEN manual_order_rank_aas_grouped = '1 - New' AND dps_customer_tag = 'New' THEN order_id ELSE NULL END), 0) AS FP_Ratio,

    -- MULTIPLE FDNC METRICS
    -- Number of Orders
    COUNT(DISTINCT CASE WHEN manual_order_rank_aas_grouped_mult_fdnc = '1 - New' AND dps_customer_tag = 'New' THEN order_id ELSE NULL END) AS Correctly_Identified_New_Cust_Mult_FDNC,
    COUNT(DISTINCT CASE WHEN manual_order_rank_aas_grouped_mult_fdnc = '> 1 - Old' AND dps_customer_tag = 'New' THEN order_id ELSE NULL END) AS Incorrectly_Identified_New_Cust_Mult_FDNC,
    COUNT(DISTINCT CASE WHEN manual_order_rank_aas_grouped_mult_fdnc = '> 1 - Old' AND dps_customer_tag = 'Existing' THEN order_id ELSE NULL END) AS Correctly_Identified_Old_Cust_Mult_FDNC,
    COUNT(DISTINCT CASE WHEN manual_order_rank_aas_grouped_mult_fdnc = '1 - New' AND dps_customer_tag = 'Existing' THEN order_id ELSE NULL END) AS Incorrectly_Identified_Old_Cust_Mult_FDNC,
    
    -- FP and FN Rates
    COUNT(DISTINCT CASE WHEN manual_order_rank_aas_grouped_mult_fdnc = '> 1 - Old' AND dps_customer_tag = 'New' THEN order_id ELSE NULL END) /
    NULLIF(
        COUNT(DISTINCT CASE WHEN manual_order_rank_aas_grouped_mult_fdnc = '> 1 - Old' AND dps_customer_tag = 'Existing' THEN order_id ELSE NULL END) +
        COUNT(DISTINCT CASE WHEN manual_order_rank_aas_grouped_mult_fdnc = '> 1 - Old' AND dps_customer_tag = 'New' THEN order_id ELSE NULL END)
    , 0) AS Daily_FP_Rate_Mult_FDNC,
    
    COUNT(DISTINCT CASE WHEN manual_order_rank_aas_grouped_mult_fdnc = '1 - New' AND dps_customer_tag = 'Existing' THEN order_id ELSE NULL END) / 
    NULLIF(
        COUNT(DISTINCT CASE WHEN manual_order_rank_aas_grouped_mult_fdnc = '1 - New' AND dps_customer_tag = 'New' THEN order_id ELSE NULL END) +
        COUNT(DISTINCT CASE WHEN manual_order_rank_aas_grouped_mult_fdnc = '1 - New' AND dps_customer_tag = 'Existing' THEN order_id ELSE NULL END)
    , 0) AS Daily_FN_Rate_Mult_FDNC,

    -- FP Ratio
    COUNT(DISTINCT CASE WHEN manual_order_rank_aas_grouped_mult_fdnc = '> 1 - Old' AND dps_customer_tag = 'New' THEN order_id ELSE NULL END) / NULLIF(COUNT(DISTINCT CASE WHEN manual_order_rank_aas_grouped_mult_fdnc = '1 - New' AND dps_customer_tag = 'New' THEN order_id ELSE NULL END), 0) AS FP_Ratio_Mult_FDNC
FROM `dh-logistics-product-ops.pricing.new_cust_identification_false_positive_analysis_no_data_loss_all_pandora_with_adjust_and_perseus_multiple_fdnc`
WHERE created_date >= DATE('2021-10-15') AND created_date <= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
GROUP BY 1,2
ORDER BY 1,2;