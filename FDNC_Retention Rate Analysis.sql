DECLARE start_date DATE;
SET start_date = '2021-08-06';

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.my_perlis_fdnc_test_retention_rate_analysis` AS

WITH cust_ord_history AS (
    SELECT 
        dps.perseus_client_id, -- A unique user ID by device
        dps.created_date, -- Order date
        dps.order_placed_at, -- Order time stamp
        dps.order_id,
        dps.platform_order_code,
        dps.dps_customer_tag, -- A flag that distinguishes between new and old users
        o.is_acquisition, -- Another flag that distinguishes between new and old users. Can be used when dps_customer_tag is faulty
        ROW_NUMBER() OVER (PARTITION BY dps.perseus_client_id ORDER BY dps.order_placed_at) AS order_rank, -- Ranks the orders that one customer has made by the date on which they were placed
        CASE WHEN is_acquisition THEN 1 ELSE 0 END AS is_acquisition_rank, -- If is_acquisition is TRUE, then 1. Otherwise, 0. This will be used later to filter out records where is_acquisition is TRUE after the customer's first order, a case which shouldn't happen 
        dps.variant,
        dps.delivery_fee_eur,
        dps.gfv_eur,
        EXTRACT(ISOWEEK FROM dps.created_date) AS calendar_week -- Calendar week in which the order was placed
    FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` dps 
    LEFT JOIN `fulfillment-dwh-production.cl.orders` ord ON dps.entity_id = ord.entity.id AND dps.order_id = ord.order_id -- A bridge table to join 1st and 3rd tables
    LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` o ON ord.entity.id = o.global_entity_id AND ord.platform_order_code = o.order_id
    WHERE dps.entity_id = 'FP_MY' -- Malaysia
        AND dps.city_name = 'Perlis' -- Filter for the city of Perlis where the FDNC test in running
        AND dps.zone_id IN (362, 363, 610, 253, 254) -- Filter for the 5 zones where the FDNC test in running
        AND dps.is_own_delivery IS TRUE -- Focus on OD orders only
        AND dps.perseus_client_id IS NOT NULL -- Remove records where perseus_client_id are missing because we need this field to determine the purchase history of individual customers 
        AND dps.created_date BETWEEN start_date AND DATE('2021-09-13') -- Set the timeline from the start of the Perlis FDNC test
        AND dps.vertical_type = 'restaurants' -- Filter for the restaurants vertical as it's the only one included in the FDNC test
        AND dps.travel_time > 2 -- The standard price scheme in Perlis offers free delivery for travel times less than 2 minutes. We filter out orders which were delivered in under two minutes to focus on customer identification-related free deliveries
        AND is_acquisition IS NOT NULL -- Remove any records where is_acquisition is NULL because it's the main field that we are using here to identify new and old customers
        AND dps.variant IN ('Control', 'Variation1') -- Filter out other variant groups (e.g. original, variation2, etc)
    ORDER BY 1,8
),

first_time_customers AS ( -- First time customer IDs (defined as perseus client IDs where is_acquisition = TRUE and order_rank = 1). Gets you the IDs of all newly acquired customers, regardless of whether or not they made repeat orders
    SELECT DISTINCT perseus_client_id
    FROM cust_ord_history
    WHERE CONCAT(CAST(order_rank AS STRING), '|', CAST(is_acquisition AS STRING)) = '1|true'
),

cust_ord_history_with_flags AS ( -- A query that gets all orders of first-time customers and adds flags to simplify filtering
    SELECT 
        *,
        SUM(order_rank) OVER (PARTITION BY perseus_client_id) AS sum_order_rank, -- A flag to tell us if the first-time customers returned to the app to order again (if it is > 1, then the customer returned). Used for QA'ing
        SUM(is_acquisition_rank) OVER (PARTITION BY perseus_client_id) AS sum_is_acquisition_rank -- A flag to tell us if is_acquisition incorrectly identifies a customer as new after his first order. If it is > 1, then is_acquisition identified a customer as new more than once, which shouldn't happen. This filter shouldn't cause a lot of records to be dropped as is_acquisition is a relatively robust field
    FROM cust_ord_history
    WHERE perseus_client_id IN (SELECT perseus_client_id FROM first_time_customers) -- Select the first-time customers from the pervious table which contains the perseus client IDs of those customers
),

cust_ord_history_with_flags_is_acq_filter AS ( -- A sub-query that drops records for any perseus client IDs that were incorrectly identified as new more than once
    SELECT *
    FROM cust_ord_history_with_flags 
    WHERE sum_is_acquisition_rank = 1 -- Drop any records where is_acquisition incorrectly identifies a customer as new after their first order (i.e. keep the ones where sum_is_acquisition_rank = 1)
),

cust_ord_history_with_flags_is_acq_df_filters AS ( -- A sub-query that drops any records for perseus client IDs that were incorrectly charged based on their identification and group assignment
    SELECT 
        *,
        CASE 
            WHEN is_acquisition AND order_rank = 1 AND variant = 'Variation1' AND delivery_fee_eur != 0 THEN 'Drop' -- A new customer in variation should receive free delivery
            WHEN is_acquisition AND order_rank = 1 AND variant = 'Control' AND delivery_fee_eur = 0 THEN 'Drop' -- A new customer in control should NOT receive free delivery
            WHEN is_acquisition = FALSE AND order_rank > 1 AND variant = 'Variation1' AND delivery_fee_eur = 0 THEN 'Drop' -- An old customer in variation should NOT receive free delivery
            WHEN is_acquisition = FALSE AND order_rank > 1 AND variant = 'Control' AND delivery_fee_eur = 0 THEN 'Drop' -- An old customer in control should NOT receive free delivery
            ELSE 'Keep'
        END AS DF_correctness_flag,

        CASE 
            WHEN is_acquisition AND order_rank = 1 AND variant = 'Variation1' AND delivery_fee_eur != 0 THEN 1 -- A new customer in variation should receive free delivery
            WHEN is_acquisition AND order_rank = 1 AND variant = 'Control' AND delivery_fee_eur = 0 THEN 1 -- A new customer in control should NOT receive free delivery
            WHEN is_acquisition = FALSE AND order_rank > 1 AND variant = 'Variation1' AND delivery_fee_eur = 0 THEN 1 -- An old customer in variation should NOT receive free delivery
            WHEN is_acquisition = FALSE AND order_rank > 1 AND variant = 'Control' AND delivery_fee_eur = 0 THEN 1 -- An old customer in control should NOT receive free delivery
            ELSE 0
        END AS DF_correctness_flag_num,
    FROM cust_ord_history_with_flags_is_acq_filter
),

cust_ord_history_with_flags_is_acq_df_filters_with_num AS ( --A sub-query that sums DF_correctness_flag_num for every perseus_client_id
    SELECT 
        *,
        SUM(DF_correctness_flag_num) OVER (PARTITION BY perseus_client_id) AS sum_DF_correctness_flag_num
    FROM cust_ord_history_with_flags_is_acq_df_filters
),

cust_ord_history_cleaned AS (
    SELECT * 
    FROM cust_ord_history_with_flags_is_acq_df_filters_with_num
    WHERE sum_DF_correctness_flag_num = 0 -- Filters out records for all perseus client IDs where the charging of DF was incorrect with respect to the customer type
    ORDER BY 1,8
),

-------------------------------------------DATA CLEANING FOR RETENTION RATE CALCULATIONS IS DONE-------------------------------------------------------------

-- Retention rate analyses

first_visit_stg AS ( -- A sub-query that identifies the calendar week in which a customer placed their first order. Split by control/variation
    SELECT 
        variant,
        perseus_client_id, 
        MIN(calendar_week) AS first_week
    FROM cust_ord_history_cleaned
    GROUP BY 1,2
    ORDER BY 2,1
),

first_visit AS ( -- A sub-query that adds the first delivery fee that a new customer paid for their order. Split by control/variation
    SELECT 
        a.variant,
        a.perseus_client_id,
        a.first_week,
        b.delivery_fee_eur
    FROM first_visit_stg a
    LEFT JOIN cust_ord_history_cleaned b ON a.variant = b.variant AND a.perseus_client_id = b.perseus_client_id AND a.first_week = b.calendar_week AND b.order_rank = 1 -- The last condition ensures that we pull the first order in case a new customer ordered several times in the same week
    ORDER BY 2,1
),

new_users AS ( -- A sub-query that identifies the number of newly acquired customers per assignment group AND calendary week. Newly acquired customers in a particular CW are defined as those who made their first order in that CW
    SELECT 
        variant,
        first_week,
        COUNT(DISTINCT perseus_client_id) AS new_users,
        ROUND(AVG(delivery_fee_eur), 2) AS avg_df_new_users
    FROM first_visit 
    GROUP BY 1,2
),

order_log AS ( -- A query that logs the calendar weeks in which customer orders were made. The third column shows the week in which a customers made their first order. The 4th column shows the calendar week of ALL orders of a particular customer (including the first order)
    SELECT DISTINCT
        v.variant,
        v.perseus_client_id,
        v.delivery_fee_eur,
        f.first_week,
        EXTRACT(ISOWEEK FROM created_date) AS visit_CW
    FROM cust_ord_history_cleaned v
    LEFT JOIN first_visit f ON v.perseus_client_id = f.perseus_client_id AND v.variant = f.variant
    ORDER BY 1,2
)

SELECT
    n.variant,
    n.first_week,
    n.new_users,
    n.avg_df_new_users,
    ret.retention_week,
    COUNT(DISTINCT v.perseus_client_id) AS retained_users_after_acq_wk,
    ROUND(COUNT(DISTINCT v.perseus_client_id) / n.new_users, 3) retention_pct_after_acq_wk,
    ROUND(AVG(v.delivery_fee_eur), 2) AS avg_df_retained_users
FROM new_users n
CROSS JOIN (SELECT DISTINCT calendar_week AS retention_week FROM cust_ord_history_cleaned) ret -- Cross joins all calendar weeks to "first_week" so that we have smth like this --> first_week | retention week: 31 | 31, 31 | 32, 31 | 33, 31 | 34, etc.
LEFT JOIN order_log v ON n.variant = v.variant AND n.first_week = v.first_week AND ret.retention_week = v.visit_CW 
GROUP BY 1,2,3,4,5
ORDER BY 1,2,5;

UPDATE `dh-logistics-product-ops.pricing.my_perlis_fdnc_test_retention_rate_analysis`
SET 
    retained_users_after_acq_wk = NULL,
    retention_pct_after_acq_wk = NULL
WHERE retained_users_after_acq_wk = 0 OR retention_pct_after_acq_wk = 0;

-------------------------------------------RETENTION RATE CALCULATIONS ARE DONE-------------------------------------------------------------

-- New Customer Split
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.my_perlis_fdnc_test_user_split` AS
WITH cust_ord_history AS (
    SELECT 
        dps.perseus_client_id, -- A unique user ID by device
        dps.created_date, -- Order date
        dps.order_placed_at, -- Order time stamp
        dps.order_id,
        dps.platform_order_code,
        dps.dps_customer_tag, -- A flag that distinguishes between new and old users
        o.is_acquisition, -- Another flag that distinguishes between new and old users. Can be used when dps_customer_tag is faulty
        ROW_NUMBER() OVER (PARTITION BY dps.perseus_client_id ORDER BY dps.order_placed_at) AS order_rank, -- Ranks the orders that one customer has made by the date on which they were placed
        CASE WHEN is_acquisition THEN 1 ELSE 0 END AS is_acquisition_rank, -- If is_acquisition is TRUE, then 1. Otherwise, 0. This will be used later to filter out records where is_acquisition is TRUE after the customer's first order, a case which shouldn't happen.
        dps.variant,
        dps.delivery_fee_eur,
        dps.gfv_eur,
        EXTRACT(ISOWEEK FROM dps.created_date) AS calendar_week -- Calendar week in which the order was placed
    FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` dps 
    LEFT JOIN `fulfillment-dwh-production.cl.orders` ord ON dps.entity_id = ord.entity.id AND dps.order_id = ord.order_id -- A bridge table to join 1st and 3rd tables
    LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` o ON ord.entity.id = o.global_entity_id AND ord.platform_order_code = o.order_id
    WHERE dps.entity_id = 'FP_MY' -- Malaysia
        AND dps.city_name = 'Perlis' -- Filter for the city of Perlis where the FDNC test in running
        AND dps.zone_id IN (362, 363, 610, 253, 254) -- Filter for the 5 zones where the FDNC test in running
        AND dps.is_own_delivery IS TRUE -- Focus on OD orders only
        AND dps.perseus_client_id IS NOT NULL -- Remove records where perseus_client_id are missing because we need this field to determine the purchase history of individual customers 
        AND dps.created_date BETWEEN DATE('2021-08-06') AND DATE('2021-09-13') -- Set the timeline from the start of the Perlis FDNC test
        AND dps.vertical_type = 'restaurants' -- Filter for the restaurants vertical as it's the only one included in the FDNC test
        AND dps.travel_time > 2 -- The standard price scheme in Perlis offers free delivery for travel times less than 2 minutes. We filter out orders which were delivered in under two minutes to focus on customer identification-related free deliveries
        AND is_acquisition IS NOT NULL -- Remove any records where is_acquisition is NULL because it's the main field that we are using here to identify new and old customers
        AND dps.variant IN ('Control', 'Variation1') -- Filter out other variant groups (e.g. original, variation2, etc)
    ORDER BY 1,8
),

first_time_customers AS ( -- First time customer IDs (defined as perseus client IDs where is_acquisition = TRUE and order_rank = 1). Gets you the IDs of all newly acquired customers, regardless of whether or not they made repeat orders
    SELECT DISTINCT perseus_client_id
    FROM cust_ord_history
    WHERE CONCAT(CAST(order_rank AS STRING), '|', CAST(is_acquisition AS STRING)) = '1|true'
),

old_customers AS ( -- Old customer IDs (defined as perseus client IDs where is_acquisition = FALSE and order_rank = 1). Gets you the IDs of all old customers, regardless of whether or not they made repeat orders
    SELECT DISTINCT perseus_client_id
    FROM cust_ord_history
    WHERE CONCAT(CAST(order_rank AS STRING), '|', CAST(is_acquisition AS STRING)) = '1|false'
),

first_time_cust_ord_history_with_flags AS ( -- A query that gets all orders of first-time customers and adds a flag to simplify filtering
    SELECT 
        *,
        SUM(is_acquisition_rank) OVER (PARTITION BY perseus_client_id) AS sum_is_acquisition_rank -- A flag to tell us if is_acquisition incorrectly identifies a customer as new after his first order. If it is > 1, then is_acquisition identified a customer as new more than once, which shouldn't happen. This filter shouldn't cause a lot of records to be dropped as is_acquisition is a relatively robust field
    FROM cust_ord_history
    WHERE perseus_client_id IN (SELECT perseus_client_id FROM first_time_customers) -- Select the first-time customers from the table which contains the perseus client IDs of those customers
),

old_cust_ord_history_with_flags AS ( -- A query that gets all orders of old customers and adds flags to simplify filtering
    SELECT 
        *,
        SUM(is_acquisition_rank) OVER (PARTITION BY perseus_client_id) AS sum_is_acquisition_rank -- A flag to tell us if is_acquisition incorrectly identifies a customer as new after his first order. If it is = 1, then is_acquisition identified a customer as new after they had already been identified as old, which shouldn't happen. This filter shouldn't cause a lot of records to be dropped as is_acquisition is a relatively robust field
    FROM cust_ord_history
    WHERE perseus_client_id IN (SELECT perseus_client_id FROM old_customers) -- Select the old customers from the table which contains the perseus client IDs of those customers
),

cust_ord_history_with_flags_is_acq_filter AS ( -- A sub-query that drops records of any perseus client IDs that were incorrectly identified as new more than once OR identified as new after they had already been identified as old
    SELECT *
    FROM first_time_cust_ord_history_with_flags
    WHERE sum_is_acquisition_rank = 1 -- Drop any records of perseus client IDs where is_acquisition incorrectly identifies a NEW customer as new after their first order (i.e. keep the ones where sum_is_acquisition_rank = 1)

    UNION ALL 
    
    SELECT *
    FROM old_cust_ord_history_with_flags
    WHERE sum_is_acquisition_rank = 0 -- Drop any records of perseus client IDs where is_acquisition incorrectly identifies an OLD customer as new after they had already been identified as old (i.e. keep the ones where sum_is_acquisition_rank = 0)
),

cust_ord_history_with_flags_is_acq_df_filters AS ( -- A sub-query that drops any records for perseus client IDs that were incorrectly charged based on their identification and group assignment
    SELECT 
        *,
        CASE 
            WHEN is_acquisition AND order_rank = 1 AND variant = 'Variation1' AND delivery_fee_eur != 0 THEN 'Drop' -- A new customer in variation should receive free delivery
            WHEN is_acquisition AND order_rank = 1 AND variant = 'Control' AND delivery_fee_eur = 0 THEN 'Drop' -- A new customer in control should NOT receive free delivery
            WHEN is_acquisition = FALSE AND order_rank > 1 AND variant = 'Variation1' AND delivery_fee_eur = 0 THEN 'Drop' -- An old customer in variation should NOT receive free delivery
            WHEN is_acquisition = FALSE AND order_rank > 1 AND variant = 'Control' AND delivery_fee_eur = 0 THEN 'Drop' -- An old customer in control should NOT receive free delivery
            ELSE 'Keep'
        END AS DF_correctness_flag,

        CASE 
            WHEN is_acquisition AND order_rank = 1 AND variant = 'Variation1' AND delivery_fee_eur != 0 THEN 1 -- A new customer in variation should receive free delivery
            WHEN is_acquisition AND order_rank = 1 AND variant = 'Control' AND delivery_fee_eur = 0 THEN 1 -- A new customer in control should NOT receive free delivery
            WHEN is_acquisition = FALSE AND order_rank > 1 AND variant = 'Variation1' AND delivery_fee_eur = 0 THEN 1 -- An old customer in variation should NOT receive free delivery
            WHEN is_acquisition = FALSE AND order_rank > 1 AND variant = 'Control' AND delivery_fee_eur = 0 THEN 1 -- An old customer in control should NOT receive free delivery
            ELSE 0
        END AS DF_correctness_flag_num,
    FROM cust_ord_history_with_flags_is_acq_filter
),

cust_ord_history_with_flags_is_acq_df_filters_with_num AS ( --A sub-query that sums DF_correctness_flag_num for every perseus_client_id
    SELECT 
        *,
        SUM(DF_correctness_flag_num) OVER (PARTITION BY perseus_client_id) AS sum_DF_correctness_flag_num
    FROM cust_ord_history_with_flags_is_acq_df_filters
),

cust_ord_history_cleaned AS (
    SELECT * 
    FROM cust_ord_history_with_flags_is_acq_df_filters_with_num
    WHERE sum_DF_correctness_flag_num = 0 -- Filters out records of all perseus client IDs where the charging of DF was incorrect with respect to the customer type
    ORDER BY 1,8
)

-- New customer split
SELECT 
    variant,
    calendar_week,
    COUNT(DISTINCT CASE WHEN is_acquisition THEN perseus_client_id ELSE NULL END) AS New_Cust_Count,
    COUNT(DISTINCT CASE WHEN is_acquisition = FALSE THEN perseus_client_id ELSE NULL END) AS Old_Cust_Count,
    COUNT(DISTINCT perseus_client_id) AS All_Cust_Count,
    ROUND(COUNT(DISTINCT CASE WHEN is_acquisition THEN perseus_client_id ELSE NULL END) / COUNT(DISTINCT perseus_client_id), 3) AS New_Cust_Share,
FROM cust_ord_history_cleaned
GROUP BY 1,2
ORDER BY 1,2;

-------------------------------------------CUSTOMER SPLIT CALCULATIONS ARE DONE-------------------------------------------------------------

-- The total number of Customers that were part of the Control and Variation
WITH city_data AS (
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
        AND entity_id = 'FP_MY'
        AND ci.name = 'Perlis'
        AND zo.id IN (362, 363, 610, 253, 254)
)
 
SELECT  
    customer.variant,
    COUNT(DISTINCT customer.user_id) AS user_count,
    COUNT(DISTINCT customer.session.id) AS session_count
FROM `fulfillment-dwh-production.cl.dynamic_pricing_user_sessions` s
LEFT JOIN UNNEST(vendors) ven
INNER JOIN city_data cd ON s.entity_id = cd.entity_id
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.vendors` v ON s.entity_id = v.global_entity_id AND ven.id = v.vendor_id
WHERE TRUE 
    AND s.entity_id = 'FP_MY'
    AND v.is_own_delivery IS TRUE -- Focus on OD orders only
    AND customer.user_id IS NOT NULL -- Remove records where customer_id is missing 
    AND created_date BETWEEN DATE('2021-08-06') AND DATE('2021-09-13')
    AND v.store_type_l2 = 'restaurants' -- Filter for the restaurants vertical as it's the only one included in the FDNC test
    AND ven.delivery_fee.travel_time > 2 -- Exclude sessions where free travel time was less than 2 minutes
    AND customer.variant in ("Variation1", "Control")
    AND ST_CONTAINS(cd.zone_shape, customer.location) IS TRUE -- Filter for the zones
GROUP BY 1