-- PART 1: Identify vendors that are assigned to **automatic scheme assignments (ASA)** and have the **customer condition** configured to them in DPS. These are the vendors for which DPS will ask for info from the CIA service

-- Pull the ASA IDs with a non-null customer condition
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.new_cust_identification_vendor_ids_with_cust_condition` AS
WITH asa_ids AS (
    SELECT
        * 
    FROM `fulfillment-dwh-production.dl.dynamic_pricing_vendor_group_price_config`
    WHERE TRUE 
        AND global_entity_id IN ('FP_MY', 'FP_SG') -- HERE
        AND customer_condition_id IS NOT NULL
    ORDER BY 2,3
),

-- Pull all the vendor_ids that have scheme IDs under the ASA IDs that were pulled in the sub-query above 
vendor_list_stg AS (
    SELECT DISTINCT 
        entity_id,
        vendor_code
    FROM `fulfillment-dwh-production.cl.vendors_v2` v
    LEFT JOIN UNNEST(vendor) ven
    LEFT JOIN UNNEST(dps) dps
    LEFT JOIN UNNEST(zones) z
    INNER JOIN asa_ids asa ON v.entity_id = asa.global_entity_id AND dps.scheme_id = asa.price_scheme_id -- INNER JOIN is a MUST here
    WHERE TRUE
        AND v.entity_id IN ('FP_MY', 'FP_SG') -- HERE
        AND v.is_active -- Filter for active vendors only
        AND dps.assignment_type IS NOT NULL -- Eliminate records where the assignment type is not stored
        AND dps.assignment_type != 'Country Fallback' -- Eliminate records where the assignment type = 'Country Fallback'
        --AND dps.scheme_id = 454 -- Uncomment if you want to filter for a specific scheme ID
),

-- Pull the data of vendors under the ASA IDs extracted in the first sub-query above
vendor_list AS (
    SELECT
        v.entity_id,
        v.vendor_code,
        delivery_provider,
        dps.assignment_type
    FROM `fulfillment-dwh-production.cl.vendors_v2` v
    LEFT JOIN UNNEST(vendor) ven
    LEFT JOIN UNNEST(dps) dps
    LEFT JOIN UNNEST(zones) z
    INNER JOIN vendor_list_stg stg ON v.entity_id = stg.entity_id AND v.vendor_code = stg.vendor_code -- INNER JOIN is a MUST here
    WHERE TRUE
        AND v.entity_id IN ('FP_MY', 'FP_SG') -- HERE
        AND v.is_active -- Filter for active vendors only
        AND dps.assignment_type IS NOT NULL -- Eliminate records where the assignment type is not stored
        --AND dps.scheme_id = 454 -- Uncomment if you want to filter for a specific scheme ID
    ORDER BY 1,2
),

-- Delivery provider is an array that needs to be unnested to have a flat table. We also add a sorting column that will be used to find the scheme ID with the highest priority.
vendor_list_unnested AS (
    SELECT DISTINCT
        v.* EXCEPT(delivery_provider),
        CASE WHEN v.assignment_type = 'Manual' THEN 'A' WHEN v.assignment_type = 'Automatic' THEN 'B' WHEN v.assignment_type = 'Country Fallback' THEN 'C' END AS assignment_type_sorting_col,
    FROM vendor_list v
    CROSS JOIN UNNEST(delivery_provider) AS delivery_type -- "delivery_provider" is an array that sometimes contains multiple elements, so we need to unnest it and break it down to its individual components
    WHERE TRUE 
        --AND delivery_type = 'OWN_DELIVERY' -- Uncomment if you want to filter for OD vendors ONLY
),

vendor_list_prio_logic AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY entity_id, vendor_code ORDER BY assignment_type_sorting_col) AS RowNum -- Sorts the scheme assignments according to the prio logic --> Manual, Automatic, Country Fallback
    FROM vendor_list_unnested
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

-- PART 2: Pull the orders associated with the vendors in "pricing.new_cust_identification_vendor_ids_with_cust_condition" and rank them by customer_account_id and order placement date. 
-- We use the order rank per customer_account_id as the ground truth of whether or not an order should be tagged by CIA with "New" or "Old"

-- Pull the customer account IDs in ALL MY and SG from Sep 17th 2021 (after the Pandora backfill) until today
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.new_cust_identification_cust_ids_my_and_sg` AS
SELECT DISTINCT 
    o.global_entity_id,
    o.customer_account_id
FROM `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` o
WHERE TRUE
    --AND o.is_own_delivery IS TRUE -- Uncomment if you want to focus on OD orders only
    --AND o.is_qcommerce = FALSE -- Uncomment if you want to filter for the restaurants vertical
    AND o.global_entity_id IN ('FP_MY', 'FP_SG') -- HERE: Malaysia and SG
    AND DATE(o.placed_at) BETWEEN DATE('2021-09-29') AND CURRENT_DATE() -- Set the timeline from the start of the Perlis FDNC test
    AND o.is_sent; -- Successful orders

-- City data --> Pull the polygon coordinates of all zones in **Perlis**. 
-- The final table should be easily filtered for "Perlis", "FP_MY", or "FP_SG", so we need the geo info of Perlis to pull the **orders** and **customer account IDs** in Perlis
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.new_cust_identification_city_data` AS
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
    AND entity_id IN ('FP_MY') -- HERE: Filter for the entity of interest
    AND ci.name = 'Perlis' -- HERE: Filter for the city of interest
    AND zo.id IN (362, 363, 610, 253, 254); -- HERE: Specify certain zones if needed

-- Pull the customer account IDs in Perlis ONLY from Sep 17th 2021 (after the Pandora backfill) until today
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.new_cust_identification_cust_ids_perlis_only` AS
SELECT DISTINCT 
    o.global_entity_id,
    o.customer_account_id
FROM `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` o
INNER JOIN `dh-logistics-product-ops.pricing.new_cust_identification_city_data` ct ON o.global_entity_id = ct.entity_id AND ST_CONTAINS(ct.zone_shape, ST_GEOGPOINT(o.delivery_location.longitude, o.delivery_location.latitude))
WHERE TRUE
    --AND o.is_own_delivery IS TRUE -- Uncomment if you want to focus on OD orders only
    --AND o.is_qcommerce = FALSE -- Uncomment if you want to filter for the restaurants vertical
    AND DATE(o.placed_at) BETWEEN DATE('2021-09-17') AND DATE('2021-09-28') -- Set the timeline from the start of the Perlis FDNC test
    AND o.is_sent; -- Successful orders

-- Pull the order history in ALL MY and SG
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.new_cust_identification_false_positive_analysis_no_data_loss` AS
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
    LEFT JOIN `fulfillment-dwh-production.cl.orders` ord ON ord.entity.id = o.global_entity_id AND ord.global_order_id = o.order_id -- A bridge table to join 1st and 3rd tables
    LEFT JOIN `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` dps ON dps.entity_id = ord.entity.id AND dps.order_id = ord.order_id
    INNER JOIN `dh-logistics-product-ops.pricing.new_cust_identification_cust_ids_my_and_sg` cus ON o.global_entity_id = cus.global_entity_id AND o.customer_account_id = cus.customer_account_id
    WHERE TRUE 
        --AND o.is_own_delivery IS TRUE -- Uncomment if you want to focus on OD orders only
        --AND o.customer_account_id IN (SELECT customer_account_id FROM `dh-logistics-product-ops.pricing.new_cust_identification_cust_ids_my_and_sg`)
        --AND o.is_qcommerce = FALSE -- Uncomment if you want to filter for the restaurants vertical
        AND o.global_entity_id IN ('FP_MY', 'FP_SG') -- HERE: MY and SG
        AND DATE(o.placed_at) >= DATE('2011-02-02') -- HERE: Set the timeline from way back in the past (2011-02-01). This is the oldest date in "curated_data_shared_central_dwh.orders"
        AND o.is_sent -- Successful order

    UNION ALL

-- Order history in Perlis ONLY
    SELECT DISTINCT -- DISTINCT here is pretty important because overlapping zones can occur, which would produce duplicates when JOINING to `pricing.new_cust_identification_city_data`
        'Perlis' AS entity_id,
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
    INNER JOIN `dh-logistics-product-ops.pricing.new_cust_identification_city_data` ct ON o.global_entity_id = ct.entity_id AND ST_CONTAINS(ct.zone_shape, ST_GEOGPOINT(o.delivery_location.longitude, o.delivery_location.latitude)) -- To filter for Perlis zones ONLY
    LEFT JOIN `fulfillment-dwh-production.cl.orders` ord ON ord.entity.id = o.global_entity_id AND ord.global_order_id = o.order_id -- A bridge table to join 1st and 3rd tables
    LEFT JOIN `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` dps ON dps.entity_id = ord.entity.id AND dps.order_id = ord.order_id
    INNER JOIN `dh-logistics-product-ops.pricing.new_cust_identification_cust_ids_perlis_only` cus ON o.global_entity_id = cus.global_entity_id AND o.customer_account_id = cus.customer_account_id
    WHERE TRUE
        --AND o.is_own_delivery IS TRUE -- Uncomment if you want to focus on OD orders only
        --AND o.customer_account_id IN (SELECT customer_account_id FROM `dh-logistics-product-ops.pricing.new_cust_identification_cust_ids_perlis_only`)
        --AND o.is_qcommerce = FALSE -- Uncomment if you want to filter for the restaurants vertical
        AND o.global_entity_id = 'FP_MY' -- HERE: Perlis is filtered for using the INNER JOIN
        AND DATE(o.placed_at) >= DATE('2011-02-02') -- HERE: Set the timeline from way back in the past (2011-02-01). This is the oldest date in "curated_data_shared_central_dwh.orders"
        AND o.is_sent -- Successful orders
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

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.new_cust_identification_false_positive_analysis_no_data_loss_with_adjust_and_perseus_all_orders` AS
WITH add_adjust_perseus AS (
    SELECT
        a.entity_id,
        b.adjust_id,
        b.perseus_id,
        a.* EXCEPT(entity_id)
    FROM `dh-logistics-product-ops.pricing.new_cust_identification_false_positive_analysis_no_data_loss` a
    LEFT JOIN `datachef-production.facts.orders` b 
    ON (CASE WHEN a.entity_id = 'Perlis' THEN 'FP_MY' ELSE entity_id END) = b.global_entity_id AND a.order_id = b.order_id AND a.created_date >= DATE('2021-01-01') AND customer_channel IN ('ios', 'android')
),

order_ranks_adjust AS (
        SELECT 
        entity_id, 
        order_id,
        CASE 
            WHEN created_date < DATE('2021-01-01') THEN 0 
            ELSE ROW_NUMBER() OVER (
                PARTITION BY 
                    entity_id, 
                    adjust_id,
                    CASE WHEN created_date < DATE('2021-01-01') THEN 1 ELSE 0 END -- Starts counting from 2021-01-01 because that's when perseus and adjust started getting populated
                ORDER BY order_placed_at
            ) END AS manual_order_rank_adjust
    FROM add_adjust_perseus
    WHERE adjust_id IS NOT NULL 
),

order_ranks_perseus AS (
        SELECT
        entity_id, 
        order_id,
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
)


SELECT 
    *,
    CASE WHEN GREATEST(IFNULL(manual_order_rank_adjust,0), manual_order_rank_long, IFNULL(manual_order_rank_perseus,0)) = 0 THEN 'NA' WHEN  GREATEST(IFNULL(manual_order_rank_adjust,0), manual_order_rank_long, IFNULL(manual_order_rank_perseus,0), 0) = 1  THEN "1 - New" ELSE "> 1 - Old" END AS manual_order_rank_aas_grouped,
FROM add_adjust_perseus
LEFT JOIN order_ranks_adjust USING(entity_id, order_id)
LEFT JOIN order_ranks_perseus USING(entity_id, order_id)
ORDER BY 1,2,4;

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Filter only for orders that will be evaluated (i.e. orders from vendors in MY and SG that had the customer condition ON)
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.new_cust_identification_false_positive_analysis_no_data_loss_with_adjust_and_perseus` AS
WITH eval_orders AS (
    SELECT 
        *,
        CASE 
            WHEN 
                entity_id = 'FP_MY' AND vendor_id IN (SELECT vendor_code FROM `dh-logistics-product-ops.pricing.new_cust_identification_vendor_ids_with_cust_condition` WHERE entity_id = 'FP_MY')
                OR 
                entity_id = 'FP_SG' AND vendor_id IN (SELECT vendor_code FROM `dh-logistics-product-ops.pricing.new_cust_identification_vendor_ids_with_cust_condition` WHERE entity_id = 'FP_SG')
                OR entity_id = 'Perlis'
            THEN 1 
            ELSE 0 
        END AS Flag
    FROM `dh-logistics-product-ops.pricing.new_cust_identification_false_positive_analysis_no_data_loss_with_adjust_and_perseus_all_orders`
)

SELECT * 
FROM eval_orders 
WHERE Flag = 1 -- Filtering for orders that should be evaluated (i.e. orders from vendors that have the customer condition ON)
