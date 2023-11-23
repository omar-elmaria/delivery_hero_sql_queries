CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.num_vendors_scheme_assignments_all_entities` AS
WITH 
entities AS (
    SELECT
        region,
        p.entity_id
    FROM `fulfillment-dwh-production.cl.entities`
    LEFT JOIN UNNEST(platforms) p
    WHERE (p.entity_id NOT LIKE 'ODR%') AND (p.entity_id NOT LIKE 'DN_%') AND (p.entity_id NOT IN ('FP_DE', 'FP_JP'))  -- Eliminate entities starting with ODR (on-demand riders) OR DN_ as they are not part of DPS
    ORDER BY 1,2
),

vendor_list AS (
    SELECT
        v.entity_id,
        v.vendor_code,
        delivery_provider,
        dps.assignment_type,
        v.vertical_type,
        CASE WHEN v.entity_id = 'YS_TR' AND (v.vertical_type != 'darkstores' OR v.vertical_type IS NULL) THEN 'Eliminate' ELSE 'Keep' END AS vertical_type_flag -- We want to track ONLY darkstore vendors in TR because restaurant vendors are not yet part of DPS
    FROM `fulfillment-dwh-production.cl.vendors_v2` v
    LEFT JOIN UNNEST(vendor) ven
    LEFT JOIN UNNEST(dps) dps
    WHERE TRUE
        AND v.is_active -- Filter for active vendors only
        AND dps.assignment_type IS NOT NULL -- Eliminate records where the assignment type is not stores
    ORDER BY 1,2
),

vendor_list_unnested AS (
    SELECT DISTINCT
        e.region,
        v.* EXCEPT(delivery_provider),
        CASE WHEN v.assignment_type = 'Manual' THEN 'A' WHEN v.assignment_type = 'Automatic' THEN 'B' WHEN v.assignment_type = 'Country Fallback' THEN 'C' END AS assignment_type_sorting_col, -- QA'ing column
    FROM vendor_list v
    CROSS JOIN UNNEST(delivery_provider) AS delivery_type -- "delivery_provider" is an array that sometimes contains multiple elements, so we need to unnest it and break it down to its individual components
    INNER JOIN entities e USING(entity_id) -- Use an INNER JOIN to eliminate the ODR entities
    WHERE TRUE 
        AND delivery_type = 'OWN_DELIVERY' -- Filter for OD vendors
        AND vertical_type_flag = 'Keep' -- Remove the TR vendors that don't belong to darkstores
),

vendor_list_prio_logic AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY region, entity_id, vendor_code ORDER BY assignment_type_sorting_col) AS RowNum -- Sorts the scheme assignments according to the prio logic --> Manual, Automatic, Country Fallback
    FROM vendor_list_unnested
),

vendor_list_asa_with_msa_stg AS (
    SELECT 
        *,
        SUM(RowNum) OVER (PARTITION BY region, entity_id, vendor_code) AS RowNum_Sum 
    FROM vendor_list_prio_logic 
    WHERE assignment_type_sorting_col IN ('A', 'B')
),

vendor_list_asa_with_msa AS (
    SELECT 
        region,
        entity_id,
        COUNT(DISTINCT CASE WHEN RowNum_Sum = 3 THEN vendor_code END) AS Vendor_Count_ASA_And_MSA
    FROM vendor_list_asa_with_msa_stg
    GROUP BY 1,2
    ORDER BY 1,2
),

vendor_list_filtered AS (
    SELECT *
    FROM vendor_list_prio_logic
    WHERE RowNum = 1 -- Selects the highest prio scheme assignment
    ORDER By 1,2,3,6
),

counts AS (
    SELECT -- A query to generate the **number** of vendors on manual, automatic, AND country fallback scheme assignments
        lis.region,
        lis.entity_id,
        COUNT(DISTINCT CASE WHEN lis.assignment_type = 'Automatic' THEN lis.vendor_code END) AS Vendor_Count_ASA_Only,
        COUNT(DISTINCT CASE WHEN lis.assignment_type = 'Manual' THEN lis.vendor_code END) AS Vendor_Count_MSA_Only,
        am.Vendor_Count_ASA_And_MSA,
        COUNT(DISTINCT CASE WHEN lis.assignment_type = 'Country Fallback' THEN lis.vendor_code END) AS Vendor_Count_Country_Fallback_Only,
        COUNT(DISTINCT lis.vendor_code) AS Total_Vendors
    FROM vendor_list_filtered lis
    LEFT JOIN vendor_list_asa_with_msa am USING(region, entity_id)
    GROUP BY 1,2, am.Vendor_Count_ASA_And_MSA
    ORDER BY 1 DESC,2
),

counts_and_shares AS ( -- A query to generate the **share** of vendors on manual, automatic, AND country fallback scheme assignments
    SELECT 
        *,
        ROUND(Vendor_Count_ASA_Only / Total_Vendors, 3) AS Vendors_ASA_Only_Share,
        ROUND(Vendor_Count_MSA_Only / Total_Vendors, 3) AS Vendors_MSA_Only_Share,
        ROUND(Vendor_Count_ASA_And_MSA / Total_Vendors, 3) AS Vendors_ASA_And_MSA_Share,
        ROUND(Vendor_Count_Country_Fallback_Only / Total_Vendors, 3) AS Vendors_Country_Fallback_Only_Share,
    FROM counts
    ORDER BY 1 DESC,2
)

SELECT * FROM counts_and_shares; -- Populates the table "num_vendors_scheme_assignments_all_entities"

--------------------------------------------------------END OF PART 1 (Weekly Snapshot)--------------------------------------------------------

-- Stores a weekly record of the ASA share
INSERT INTO `dh-logistics-product-ops.pricing.vendors_asa_share_over_time_all_entities` (
    SELECT 
        CURRENT_DATE() AS DataPullDate,
        region,
        entity_id,
        Vendors_ASA_Only_Share,
        Vendors_ASA_And_MSA_Share
    FROM `dh-logistics-product-ops.pricing.num_vendors_scheme_assignments_all_entities`
    ORDER BY 2,3,1
);

--------------------------------------------------------END OF PART 2 (Historical Log)--------------------------------------------------------