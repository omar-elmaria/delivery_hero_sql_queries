/*
This script curates the "Loved Brands" for all countries and ASAs
V3 includes the patch that links the parent to its child
V3_Linted_Deployed to GitHub is the same as V3 but with the formatting rules applied
V4_Linted_Deployed includes a fix for how CVR3 is calculated --> Exclude event_time from the numerator
V5_Linted_Deployed has a new way of calculating elasticities based on linear regression
*/

###---------------------------------------------------------------------------------------END OF SCRIPT DESCRIPTION---------------------------------------------------------------------------------------###

-- Step 0: Declare inputs that will be used throughout the script
DECLARE sessions_ntile_thr, orders_ntile_thr, cvr3_ntile_thr FLOAT64;

SET (sessions_ntile_thr, orders_ntile_thr, cvr3_ntile_thr) = (0.75, 0.75, 0.75); -- We filter first for the top 25% of vendors before applying the Loved Brands logic

###---------------------------------------------------------------------------------------END OF INPUTS SECTON-------------------------------------------------------------------------------------------###

-- Step 1: Obtain a list of all active entities in Delivery Hero
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.active_entities_loved_brands_scaled_code` AS
WITH dps AS (SELECT DISTINCT entity_id FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`)

SELECT
    ent.region,
    LOWER(ent.country_iso) AS country_code,
    p.entity_id,
    ent.country_name
FROM `fulfillment-dwh-production.cl.entities` AS ent
LEFT JOIN UNNEST(platforms) AS p
INNER JOIN dps ON p.entity_id = dps.entity_id
WHERE TRUE
    AND p.entity_id NOT LIKE "ODR%" -- Eliminate entities starting with ODR (on-demand riders)
    AND p.entity_id NOT LIKE "DN_%" -- Eliminate entities starting with DN_ as they are not part of DPS
    AND p.entity_id NOT IN ("FP_DE", "FP_JP", "BM_VN", "BM_KR") -- Eliminate irrelevant entity_ids in APAC
    AND p.entity_id NOT IN ("TB_SA", "HS_BH", "CG_QA", "IN_AE", "ZO_AE", "IN_BH") -- Eliminate irrelevant entity_ids in MENA
    AND p.entity_id NOT IN ("TB_SA", "HS_BH", "CG_QA", "IN_AE", "ZO_AE", "IN_BH") -- Eliminate irrelevant entity_ids in Europe
    AND p.entity_id NOT IN ("CD_CO") -- Eliminate irrelevant entity_ids in LATAM
;

###---------------------------------------------------------------------------------------END OF STEP 1---------------------------------------------------------------------------------------###

-- Step 2: Get a list of all cities and zones in each country. This table can be joined to "dps_sessions_mapped_to_ga_sessions" and the "zone_shape" geo field can be used to filter for sessions in the zones that are of interest to us
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.city_data_loved_brands_scaled_code` AS
SELECT
    co.region,
    p.entity_id,
    co.country_code,
    ci.name AS city_name,
    ci.id AS city_id,
    zo.name AS zone_name,
    zo.id AS zone_id,
    zo.shape AS zone_shape
FROM `fulfillment-dwh-production.cl.countries` AS co
LEFT JOIN UNNEST(co.platforms) AS p
LEFT JOIN UNNEST(co.cities) AS ci
LEFT JOIN UNNEST(ci.zones) AS zo
INNER JOIN `dh-logistics-product-ops.pricing.active_entities_loved_brands_scaled_code` AS ent ON p.entity_id = ent.entity_id AND co.country_code = ent.country_code
WHERE TRUE
    AND zo.is_active -- Active city
    AND ci.is_active -- Active zone
;

###---------------------------------------------------------------------------------------END OF STEP 2---------------------------------------------------------------------------------------###

-- Step 3.1: Get the ASA setups in each country. This table displays the full setup of the ASA including conditions
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.asa_setups_loved_brands_scaled_code` AS
SELECT DISTINCT
  ent.region,
  asa.entity_id,
  asa.country_code,

  asa.asa_id,
  asa.asa_name,
  asa.priority AS asa_priority,
  TIMESTAMP_TRUNC(asa.active_from, SECOND) AS asa_setup_active_from,
  MAX(TIMESTAMP_TRUNC(asa.active_from, SECOND)) OVER (PARTITION BY asa.entity_id, asa.country_code, asa.asa_id) AS asa_setup_active_from_max_date,
  TIMESTAMP_TRUNC(asa.active_to, SECOND) AS asa_setup_active_to,

  vendor_code,
  pc.scheme_id,
  asa.vendor_group_id AS vendor_group_price_config_id,
  pc.priority AS condition_priority,
  pc.schedule_id AS time_condition_id,
  pc.customer_condition_id,
  assigned_vendors_count AS vendor_count_caught_by_asa,
  n_schemes
FROM `dh-logistics-product-ops.pricing.dps_asa_full_config_versions` AS asa
INNER JOIN `dh-logistics-product-ops.pricing.active_entities_loved_brands_scaled_code` AS ent ON asa.entity_id = ent.entity_id -- Filter only for active DH entities
LEFT JOIN UNNEST(sorted_assigned_vendor_ids) AS vendor_code
LEFT JOIN UNNEST(asa.asa_price_config) AS pc
WHERE asa.active_to IS NULL AND asa.priority > 0 -- Get the most up-to-date ASA assignment setup
ORDER BY ent.region, asa.entity_id, asa.country_code, asa.asa_id, asa.priority, vendor_code, pc.priority, pc.scheme_id
;

###---------------------------------------------------------------------------------------END OF STEP TEMPORARY STEP FOR TESTING---------------------------------------------------------------------------------------###

-- Step 3.2: Identify the ASAs that have been clustered
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.parent_child_asa_linking_loved_brands_scaled_code` AS
WITH original_asa AS (
    SELECT DISTINCT
        entity_id,
        country_code,
        asa_name,
        asa_id
    FROM `dh-logistics-product-ops.pricing.asa_setups_loved_brands_scaled_code`
),

parent_and_child_asa AS (
    SELECT
        a.*,
        b.entity_id AS entity_id_linked_asa,
        b.country_code AS country_code_linked_asa,
        b.asa_name AS linked_asa
    FROM original_asa AS a
    CROSS JOIN original_asa AS b
)

SELECT
    a.*,
    -- If child = parent + "_LB" OR parent + "_LB" = child
    CASE WHEN LOWER(a.asa_name) = LOWER(CONCAT(linked_asa, "_LB")) OR LOWER(CONCAT(a.asa_name, "_LB")) = LOWER(linked_asa) THEN TRUE ELSE FALSE END AS is_asa_clustered,
    TRIM(a.asa_name, "_LB") AS asa_common_name
FROM parent_and_child_asa AS a
WHERE TRUE
    AND CASE WHEN LOWER(a.asa_name) = LOWER(CONCAT(linked_asa, "_LB")) OR LOWER(CONCAT(a.asa_name, "_LB")) = LOWER(linked_asa) THEN TRUE ELSE FALSE END = TRUE
    AND a.entity_id = a.entity_id_linked_asa
    AND a.country_code = a.country_code_linked_asa
ORDER BY 1, 2, 3
;

###---------------------------------------------------------------------------------------END OF STEP 2---------------------------------------------------------------------------------------###

-- Step 3.3: Add the master_asa_id to the table that contains the ASA setups in each country
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.asa_setups_loved_brands_scaled_code` AS
SELECT
    a.region,
    a.entity_id,
    a.country_code,
    a.asa_id,
    CASE WHEN b.is_asa_clustered = TRUE THEN MAX(asa_id) OVER(PARTITION BY a.entity_id, a.country_code) + 1 ELSE a.asa_id END AS master_asa_id,
    a.asa_name,
    COALESCE(b.asa_common_name, a.asa_name) AS asa_common_name,
    COALESCE(b.is_asa_clustered, FALSE) AS is_asa_clustered,
    a.asa_priority, -- The priority of the automatic assignment. This does not always match the priority ID in the DPS UI
    a.asa_setup_active_from,
    a.asa_setup_active_from_max_date,
    a.asa_setup_active_to,
    a.vendor_code,
    a.scheme_id,
    a.vendor_group_price_config_id,
    a.condition_priority,
    a.time_condition_id,
    a.customer_condition_id,
    a.vendor_count_caught_by_asa,
    n_schemes
FROM `dh-logistics-product-ops.pricing.asa_setups_loved_brands_scaled_code` AS a
LEFT JOIN `dh-logistics-product-ops.pricing.parent_child_asa_linking_loved_brands_scaled_code` AS b USING (entity_id, country_code, asa_id)
;

###---------------------------------------------------------------------------------------END OF STEP 3.3---------------------------------------------------------------------------------------###

-- Step 4: Get a list of vendor IDs per ASA ID. This list shows the vendor IDs that are caught by the filters of each ASA and takes into account the priority logic of the ASA
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.vendor_ids_per_asa_loved_brands_scaled_code` AS
SELECT DISTINCT
    region,
    entity_id,
    country_code,

    asa_id,
    master_asa_id, -- Artificial ASA ID
    asa_name,
    asa_common_name, -- Artificial ASA name
    is_asa_clustered, -- Flag to determine if the ASA was clustered before or not
    vendor_count_caught_by_asa,
    vendor_code
FROM `dh-logistics-product-ops.pricing.asa_setups_loved_brands_scaled_code`
WHERE TRUE
    AND time_condition_id IS NULL -- Filter for records without a time condition because the vendor codes would be duplicated if we have ASA configs with a time condition
    AND customer_condition_id IS NULL -- Filter for records without a customer condition becaue the vendor codes would be duplicated if we have ASA configs with a time condition
;

###---------------------------------------------------------------------------------------END OF STEP 4---------------------------------------------------------------------------------------###

-- Step 5: Get a list of scheme IDs per ASA ID
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.scheme_ids_per_asa_loved_brands_scaled_code` AS
SELECT DISTINCT
    region,
    entity_id,
    country_code,

    asa_id,
    master_asa_id,
    asa_name,
    asa_common_name,
    vendor_count_caught_by_asa,
    scheme_id,
    CASE WHEN time_condition_id IS NULL AND customer_condition_id IS NULL THEN "Main Scheme" ELSE "Condition Scheme" END AS scheme_type
FROM `dh-logistics-product-ops.pricing.asa_setups_loved_brands_scaled_code`
;

###---------------------------------------------------------------------------------------END OF STEP 5---------------------------------------------------------------------------------------###

-- Step 6: Get data about the DF tiers of each price scheme. We need this table for step 7
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.df_tiers_per_price_scheme_loved_brands_scaled_code` AS
WITH scheme_config AS (
    SELECT DISTINCT
        pcv.entity_id,
        pcv.country_code,
        pcv.scheme_id,
        pcv.scheme_name,
        TIMESTAMP_TRUNC(pcv.scheme_active_from, SECOND) AS scheme_active_from,
        TIMESTAMP_TRUNC(pcv.scheme_active_to, SECOND) AS scheme_active_to,
        pcv.scheme_component_ids.travel_time_config_id,
        COALESCE(ttc.travel_time_threshold, 9999999) AS tt_threshold,
        CASE
            WHEN ttc.travel_time_threshold IS NULL THEN 9999999
            ELSE ROUND(FLOOR(ttc.travel_time_threshold) + (ttc.travel_time_threshold - FLOOR(ttc.travel_time_threshold)) * 60 / 100, 2)
        END AS threshold_in_min_and_sec,
        ttc.travel_time_fee AS fee,
    FROM `fulfillment-dwh-production.cl.pricing_configuration_versions` pcv
    LEFT JOIN UNNEST(scheme_component_configs.travel_time_config) ttc
    WHERE scheme_active_to IS NULL
        AND CONCAT(pcv.entity_id, " | ", pcv.scheme_id) IN (
            SELECT DISTINCT CONCAT(entity_id, " | ", scheme_id) AS entity_country_scheme
            FROM `dh-logistics-product-ops.pricing.scheme_ids_per_asa_loved_brands_scaled_code`
        )
)

SELECT 
    b.region,
    a.entity_id,
    a.country_code,
    -- Keep in mind that one scheme could be included in more than one ASA, so joining "dps_config_versions" on the "scheme_ids_per_asa_loved_brands_scaled_code" will produce duplicates and this is expected
    b.asa_id,
    b.master_asa_id,
    b.asa_name,
    b.asa_common_name,
    a.* EXCEPT (entity_id, country_code),
    RANK() OVER (PARTITION BY a.entity_id, a.country_code, b.asa_id, a.scheme_id, a.travel_time_config_id ORDER BY a.tt_threshold) AS tier
FROM scheme_config AS a
LEFT JOIN `dh-logistics-product-ops.pricing.scheme_ids_per_asa_loved_brands_scaled_code` AS b
    ON TRUE
        AND a.entity_id = b.entity_id
        AND a.country_code = b.country_code
        AND a.scheme_id = b.scheme_id
ORDER BY region, entity_id, country_code, asa_id, scheme_id, tier
;

###---------------------------------------------------------------------------------------END OF STEP 6---------------------------------------------------------------------------------------###

-- Step 7: Get the DF tiers of each ASA. We will need this in step 14 when we implement the LBs logic
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.df_tiers_per_asa_loved_brands_scaled_code` AS
SELECT DISTINCT
    region,
    entity_id,
    country_code,
    asa_id,
    master_asa_id,
    asa_name,
    asa_common_name,
    fee
FROM `dh-logistics-product-ops.pricing.df_tiers_per_price_scheme_loved_brands_scaled_code`
ORDER BY 1, 2, 3, 4, 5
;

###---------------------------------------------------------------------------------------END OF STEP 7---------------------------------------------------------------------------------------###

-- Step 8: Get session data for all relevant vendors over the specified time frame
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.session_data_for_vendor_screening_loved_brands_scaled_code` AS
SELECT DISTINCT
    x.created_date, -- Date of the ga session
    x.entity_id, -- Entity ID
    x.country_code, -- Country code
    x.events_ga_session_id, -- GA session ID
    x.ga_dps_session_id, -- DPS session ID
    x.sessions.perseus_client_id, -- A unique customer identifier based on the device
    
    e.event_action, -- Can have five values --> home_screen.loaded, shop_list.loaded, shop_details.loaded, checkout.loaded, transaction
    e.vendor_code, -- Vendor ID
    e.event_time, -- The timestamp of the event's creation
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_ga_sessions` AS x
LEFT JOIN UNNEST(events) AS e
LEFT JOIN UNNEST(dps_zone) AS dps
WHERE TRUE
    -- Filter for the relevant combinations of entity, country_code, and vendor_code
    AND CONCAT(x.entity_id, " | ", x.country_code, " | ", e.vendor_code) IN (
        SELECT DISTINCT CONCAT(entity_id, " | ", country_code, " | ", vendor_code) AS entity_country_vendor
        FROM `dh-logistics-product-ops.pricing.vendor_ids_per_asa_loved_brands_scaled_code`
    )
    -- Extract session data over the specified time frame
    AND x.created_date BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AND LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) -- Sessions' start and end date
    -- Filter for 'shop_details.loaded', 'transaction' events as we only need those to calculate CVR3
    AND e.event_action IN ("shop_details.loaded", "transaction") -- transaction / shop_details.loaded = CVR3
;

###---------------------------------------------------------------------------------------END OF STEP 8---------------------------------------------------------------------------------------###

-- Step 9.1: Get order data for all relevant vendors over the specified time frame
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.order_data_for_vendor_screening_loved_brands_scaled_code` AS
SELECT
    entity_id,
    LOWER(country_code) AS country_code,
    vendor_id AS vendor_code,
    COUNT(DISTINCT platform_order_code) AS num_orders
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`
WHERE TRUE
    AND CONCAT(entity_id, " | ", country_code, " | ", vendor_id) IN (
        SELECT DISTINCT CONCAT(entity_id, " | ", country_code, " | ", vendor_code) AS entity_country_vendor
        FROM `dh-logistics-product-ops.pricing.vendor_ids_per_asa_loved_brands_scaled_code`
    )
    AND created_date BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AND LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
    AND is_sent -- Successful order
GROUP BY 1, 2, 3
;

###---------------------------------------------------------------------------------------END OF STEP 9.1---------------------------------------------------------------------------------------###

-- Step 9.2: Get order data for all relevant ASAs over the specified time frame. We will need this information to calculate the potential impact of "Loved Brands"
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.asa_order_data_for_impact_analysis_loved_brands_scaled_code` AS
WITH temp_tbl AS (
    SELECT
        o.entity_id,
        o.country_code,
        CAST(o.assignment_id AS INT64) AS asa_id, -- When vendor_price_scheme_type = 'Automatic scheme', assignment_id = asa_id
        o.platform_order_code,
        o.exchange_rate,
        CASE
            WHEN ent.region IN ("Europe", "Asia") THEN COALESCE( -- Get the delivery fee data of Pandora countries from Pandata tables
                pd.delivery_fee_local,
                IF(o.is_delivery_fee_covered_by_discount = TRUE OR o.is_delivery_fee_covered_by_voucher = TRUE, 0, o.delivery_fee_local)
            )
            WHEN ent.region NOT IN ("Europe", "Asia") THEN (CASE WHEN o.is_delivery_fee_covered_by_voucher = FALSE AND o.is_delivery_fee_covered_by_discount = FALSE THEN o.delivery_fee_local ELSE 0 END)
        END AS actual_df_paid_by_customer
    FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` AS o
    -- The "pd_orders" table contains info on the orders in Pandora countries
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` AS pd ON o.entity_id = pd.global_entity_id AND o.platform_order_code = pd.code AND o.created_date = pd.created_date_utc
    INNER JOIN `dh-logistics-product-ops.pricing.active_entities_loved_brands_scaled_code` AS ent ON o.entity_id = ent.entity_id AND o.country_code = ent.country_code -- Filter only for active DH entities
    WHERE TRUE
        AND CONCAT(o.entity_id, " | ", o.country_code, " | ", o.assignment_id) IN (
            SELECT DISTINCT CONCAT(entity_id, " | ", country_code, " | ", asa_id) AS entity_country_asa
            FROM `dh-logistics-product-ops.pricing.vendor_ids_per_asa_loved_brands_scaled_code`
        )
        AND o.created_date BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AND LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
        AND o.vendor_price_scheme_type = "Automatic scheme" -- Filter for orders coming from ASA schemes
        AND o.is_sent -- Successful order
)

SELECT
    entity_id,
    country_code,
    asa_id,
    COUNT(DISTINCT platform_order_code) AS num_orders_asa,
    ROUND(SUM(actual_df_paid_by_customer), 2) AS total_df_revenue_local,
    ROUND(SUM(actual_df_paid_by_customer / exchange_rate), 2) AS total_df_revenue_eur,
    ROUND(SUM(actual_df_paid_by_customer) / COUNT(DISTINCT platform_order_code), 2) AS avg_df_asa_local,
    ROUND(SUM(actual_df_paid_by_customer / exchange_rate) / COUNT(DISTINCT platform_order_code), 2) AS avg_df_asa_eur
FROM temp_tbl
GROUP BY 1, 2, 3
;

###---------------------------------------------------------------------------------------END OF STEP 9.2---------------------------------------------------------------------------------------###

-- Step 10: Compute the total orders, visits, and CVR3 for each vendor code in `dh-logistics-product-ops.pricing.vendor_ids_per_asa_loved_brands_scaled_code`
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.all_metrics_for_vendor_screening_loved_brands_scaled_code` AS
WITH all_metrics AS (
    SELECT
        v.*,
        COALESCE(o.num_orders, 0) AS num_orders,
        COALESCE(COUNT(DISTINCT CASE WHEN event_action = "shop_details.loaded" THEN events_ga_session_id END), 0) AS num_unique_vendor_visits, -- If a vendor was visited more than once in the same session, it's one visit
        COALESCE(COUNT(DISTINCT CASE WHEN event_action = "shop_details.loaded" THEN event_time END), 0) AS num_total_vendor_impressions, -- If a vendor was visited more than once in the same session, count all impressions
        COALESCE(COUNT(DISTINCT CASE WHEN event_action = "transaction" THEN events_ga_session_id END), 0) AS num_transactions,
        -- We choose to round to 5 decimal places because we compare the CVRs to one another at a later point in the code when we calculate the pct drop in CVR from the base
        -- More precision is needed to avoid calculation errors due to rounding
        COALESCE(
            ROUND(COUNT(DISTINCT CASE WHEN event_action = "transaction" THEN events_ga_session_id END) / NULLIF(COUNT(DISTINCT CASE WHEN event_action = "shop_details.loaded" THEN events_ga_session_id END), 0), 5),
            0
        ) AS cvr3
    FROM `dh-logistics-product-ops.pricing.vendor_ids_per_asa_loved_brands_scaled_code` AS v
    LEFT JOIN `dh-logistics-product-ops.pricing.session_data_for_vendor_screening_loved_brands_scaled_code` AS s
        ON TRUE
            AND v.entity_id = s.entity_id
            AND v.country_code = s.country_code
            AND v.vendor_code = s.vendor_code -- LEFT JOIN because we assume that the vendors table contains ALL vendors with and without sessions, so any vendors without sessions will get a "zero"
    LEFT JOIN `dh-logistics-product-ops.pricing.order_data_for_vendor_screening_loved_brands_scaled_code` AS o
        ON TRUE
            AND v.entity_id = o.entity_id
            AND v.country_code = o.country_code
            AND v.vendor_code = o.vendor_code -- LEFT JOIN for the same reason in the statement above
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
),

pct_ranks AS (
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY entity_id, country_code, master_asa_id ORDER BY num_orders), 4) AS orders_pct_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY entity_id, country_code, master_asa_id ORDER BY num_unique_vendor_visits), 4) AS unique_visits_pct_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY entity_id, country_code, master_asa_id ORDER BY cvr3), 4) AS cvr3_pct_rank
    FROM all_metrics
),

additional_fields AS (
    SELECT
        *,
        -- Entity orders and ASA order share
        SUM(num_orders) OVER (PARTITION BY entity_id, country_code) AS entity_orders,
        SUM(num_orders) OVER (PARTITION BY entity_id, country_code, asa_id) AS asa_orders, -- We use asa_id not master_asa_id because this field and the others below it are used in the dashboard, which is on ASA level not master ASA
        ROUND(SUM(num_orders) OVER (PARTITION BY entity_id, country_code, asa_id) / NULLIF(SUM(num_orders) OVER (PARTITION BY entity_id, country_code), 0), 4) AS asa_order_share_of_entity,

        -- ASA order count after every filtering step
        SUM(CASE WHEN unique_visits_pct_rank >= sessions_ntile_thr THEN num_orders END) OVER (PARTITION BY entity_id, country_code, asa_id) AS asa_orders_after_visits_filter,
        SUM(
            CASE WHEN
                unique_visits_pct_rank >= sessions_ntile_thr
                AND orders_pct_rank >= orders_ntile_thr
                THEN num_orders END
        ) OVER (PARTITION BY entity_id, country_code, asa_id) AS asa_orders_after_visits_and_orders_filters,

        SUM(
            CASE WHEN
                unique_visits_pct_rank >= sessions_ntile_thr
                AND orders_pct_rank >= orders_ntile_thr
                AND cvr3_pct_rank >= cvr3_ntile_thr
                THEN num_orders END
        ) OVER (PARTITION BY entity_id, country_code, asa_id) AS asa_orders_after_all_initial_filters,

        -- ASA vendor count after every filtering step
        COUNT(DISTINCT CASE WHEN unique_visits_pct_rank >= sessions_ntile_thr THEN vendor_code END) OVER (PARTITION BY entity_id, country_code, asa_id) AS vendor_count_remaining_after_visits_filter,

        COUNT(
            DISTINCT CASE WHEN
                unique_visits_pct_rank >= sessions_ntile_thr
                AND orders_pct_rank >= orders_ntile_thr
                THEN vendor_code END
        ) OVER (PARTITION BY entity_id, country_code, asa_id) AS vendor_count_remaining_after_visits_and_orders_filters,

        COUNT(
            DISTINCT CASE WHEN
                unique_visits_pct_rank >= sessions_ntile_thr
                AND orders_pct_rank >= orders_ntile_thr
                AND cvr3_pct_rank >= cvr3_ntile_thr
                THEN vendor_code END
        ) OVER (PARTITION BY entity_id, country_code, asa_id) AS vendor_count_remaining_after_all_initial_filters
    FROM pct_ranks
)

SELECT
    *,

    -- ASA order share after every filtering step
    ROUND(asa_orders_after_visits_filter / NULLIF(entity_orders, 0), 4) AS asa_order_share_after_visits_filter,
    ROUND(asa_orders_after_visits_and_orders_filters / NULLIF(entity_orders, 0), 4) AS asa_order_share_after_visits_and_orders_filters,
    ROUND(asa_orders_after_all_initial_filters / NULLIF(entity_orders, 0), 4) AS asa_order_share_after_all_initial_filters,

    -- Percentage of vendors remaining in the ASA after every filtering step
    ROUND(vendor_count_remaining_after_visits_filter / NULLIF(vendor_count_caught_by_asa, 0), 4) AS perc_vendors_remaining_after_visits_filter,
    ROUND(vendor_count_remaining_after_visits_and_orders_filters / NULLIF(vendor_count_caught_by_asa, 0), 4) AS perc_vendors_remaining_after_visits_and_orders_filters,
    ROUND(vendor_count_remaining_after_all_initial_filters / NULLIF(vendor_count_caught_by_asa, 0), 4) AS perc_vendors_remaining_after_all_initial_filters
FROM additional_fields
;

###---------------------------------------------------------------------------------------END OF STEP 10---------------------------------------------------------------------------------------###

-- Step 11: Filtering for vendors based on percentile ranks
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.all_metrics_after_session_order_cvr_filters_loved_brands_scaled_code` AS
SELECT
    -- Identifiers
    region,
    entity_id,
    country_code,
    asa_id,
    master_asa_id,
    asa_name,
    asa_common_name,
    vendor_code,

    -- Orders, visits, impressions, transactions, and CVR3 per vendor
    num_orders,
    num_unique_vendor_visits,
    num_total_vendor_impressions,
    num_transactions,
    cvr3,

    -- Percentile ranks. The comparison set is all vendors within an ASA
    orders_pct_rank,
    unique_visits_pct_rank,
    cvr3_pct_rank,

    -- Order counts after each filtering step
    entity_orders,
    asa_order_share_of_entity,
    asa_orders_after_visits_filter,
    asa_orders_after_visits_and_orders_filters,
    asa_orders_after_all_initial_filters,

    -- Order shares after each filtering step
    asa_order_share_after_visits_filter,
    asa_order_share_after_visits_and_orders_filters,
    asa_order_share_after_all_initial_filters,

    -- Vendor counts remaining after each filtering step
    vendor_count_caught_by_asa,
    vendor_count_remaining_after_visits_filter,
    vendor_count_remaining_after_visits_and_orders_filters,
    vendor_count_remaining_after_all_initial_filters,

    -- Percentage of ASA vendors remaining after each filtering step
    perc_vendors_remaining_after_visits_filter,
    perc_vendors_remaining_after_visits_and_orders_filters,
    perc_vendors_remaining_after_all_initial_filters

FROM `dh-logistics-product-ops.pricing.all_metrics_for_vendor_screening_loved_brands_scaled_code`
WHERE unique_visits_pct_rank >= sessions_ntile_thr AND orders_pct_rank >= orders_ntile_thr AND cvr3_pct_rank >= cvr3_ntile_thr
ORDER BY region ASC, entity_id ASC, country_code ASC, master_asa_id ASC, cvr3_pct_rank DESC
;

#############################################################################################################################################################################################################
#####------------------------------------------------------------------------END OF THE ORDERS, SESSIONS, CVR3 FILTERING PROCESS------------------------------------------------------------------------#####
#############################################################################################################################################################################################################

-- Step 12: Get data about the DF seen in the session from the DPS logs
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.dps_logs_loved_brands_scaled_code` AS
WITH dps_logs_stg_1 AS (
    SELECT
        logs.entity_id,
        LOWER(logs.country_code) AS country_code,
        logs.created_date,
        customer.user_id AS perseus_id,
        customer.session.id AS dps_session_id,
        vendors,
        customer.session.timestamp AS session_timestamp,
        logs.created_at
    FROM `fulfillment-dwh-production.cl.dynamic_pricing_user_sessions` AS logs
    INNER JOIN `dh-logistics-product-ops.pricing.city_data_loved_brands_scaled_code` AS cd ON logs.entity_id = cd.entity_id AND ST_CONTAINS(cd.zone_shape, logs.customer.location) -- Filter for sessions in the zones specified above
    WHERE TRUE
        -- Filter for the relevant combinations of entity and country_code
        AND CONCAT(logs.entity_id, " | ", LOWER(logs.country_code)) IN (
            SELECT DISTINCT CONCAT(entity_id, " | ", country_code) AS entity_country
            FROM `dh-logistics-product-ops.pricing.city_data_loved_brands_scaled_code`
        )
        -- Do NOT filter for multiplFee (MF) endpoints because the query times out if you do so. singleFee endpoint requests are sufficient for our purposes even though we lose a bit of data richness when we don't consider MF requests
        -- We lose data on the delivery fee seen in sessions for about 7.5% of all GA sessions in "session_data_for_vendor_screening_loved_brands_scaled_code" by adding this filter. However, we gain much more in code efficiency   
        AND endpoint = "singleFee"
        -- Do NOT use the "start_date" and "end_date" params here. This will inc. the query size
        AND logs.created_date BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AND LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
        AND logs.customer.session.id IS NOT NULL -- We must have the dps session ID to be able to obtain the session's DF in the next query
),

dps_logs_stg_2 AS (
    SELECT DISTINCT
        dps.* EXCEPT(session_timestamp, created_at, vendors),
        v.id AS vendor_code,
        v.meta_data.scheme_id,
        v.meta_data.vendor_price_scheme_type,
        v.delivery_fee.travel_time AS df_total,
        dps.session_timestamp,
        dps.created_at
    FROM dps_logs_stg_1 AS dps
    LEFT JOIN UNNEST(vendors) AS v
    WHERE TRUE
        -- Filter for the relevant combinations of entity, country_code, and vendor_code
        AND CONCAT(dps.entity_id, " | ", dps.country_code, " | ", v.id) IN (
            SELECT DISTINCT CONCAT(entity_id, " | ", country_code, " | ", vendor_code) AS entity_country_vendor
            FROM `dh-logistics-product-ops.pricing.vendor_ids_per_asa_loved_brands_scaled_code`
        )

-- Don't filter for the relevant combinations of entity, country_code, and scheme_id even though the code takes longer to run
-- We care about sessions that were associated with the DF amounts of the respective ASA, not which scheme these DF amounts came from. To a customer, the DF amount is what matters. They don't know which scheme it came from
)

SELECT *
FROM dps_logs_stg_2
-- Create a row counter to take the last delivery fee seen in the session for each vendor_id. We assume that this is the one that the customer took their decision to purchase/not purchase on
QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_id, country_code, dps_session_id, vendor_code ORDER BY created_at DESC) = 1
ORDER BY dps_session_id, vendor_code, created_at
;

###---------------------------------------------------------------------------------------END OF STEP 12---------------------------------------------------------------------------------------###

-- Step 13: Join the DPS logs to the GA sessions data
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ga_dps_sessions_loved_brands_scaled_code` AS
SELECT
    x.*,
    logs.df_total,
    logs.scheme_id,
    logs.vendor_price_scheme_type,
    logs.created_at AS dps_logs_created_at
FROM `dh-logistics-product-ops.pricing.session_data_for_vendor_screening_loved_brands_scaled_code` AS x
-- With this join, ~ 12.9% of all rows get a NULL DF_total as a result of filtering out multipleFee endpoint requests
LEFT JOIN `dh-logistics-product-ops.pricing.dps_logs_loved_brands_scaled_code` AS logs -- You can use an INNER JOIN here if it's important to have a DF value associated with every session
    ON TRUE
        AND x.entity_id = logs.entity_id
        AND x.country_code = logs.country_code
        AND x.ga_dps_session_id = logs.dps_session_id
        -- **IMPORTANT**: Sometimes, the dps logs give us multiple delivery fees per session. One reason for this could be a change in location. We eliminated sessions with multiple DFs in the previous step to keep the dataset clean
        AND x.vendor_code = logs.vendor_code
ORDER BY events_ga_session_id, event_time
;

###---------------------------------------------------------------------------------------END OF STEP 13---------------------------------------------------------------------------------------###

-- Step 14: Now, we are ready to apply the Loved Brands logic
-- Instead of specifying CVR3 drop thresholds that cannot be exceeded in a manual manner, we will calculate the overall CVR3 per DF bucket for each ASA \
-- and use the percentage changes from the lowest DF tier to each subsequent one as our thresholds

-- Step 14.1: Calculate the conversion rate per DF bucket for each vendor
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.cvr_per_df_bucket_vendor_level_loved_brands_scaled_code` AS
SELECT
    ven.region,
    ven.entity_id,
    ven.country_code,
    ven.master_asa_id,
    ven.asa_common_name,
    ven.vendor_count_caught_by_asa,
    ven.vendor_code,
    ses.df_total,
    -- If a vendor was visited more than once in the same session, this is considered one visit
    COALESCE(COUNT(DISTINCT CASE WHEN ses.event_action = "shop_details.loaded" THEN ses.events_ga_session_id END), 0) AS num_unique_vendor_visits,
    -- If a vendor was visited more than once in the same session, all impressions are counted
    COALESCE(COUNT(DISTINCT CASE WHEN ses.event_action = "shop_details.loaded" THEN ses.event_time END), 0) AS num_total_vendor_impressions,
    COALESCE(COUNT(DISTINCT CASE WHEN ses.event_action = "transaction" THEN ses.events_ga_session_id END), 0) AS num_transactions,
    COALESCE(
        ROUND(
            COUNT(DISTINCT CASE WHEN ses.event_action = "transaction" THEN ses.events_ga_session_id END)
            / NULLIF(COUNT(DISTINCT CASE WHEN ses.event_action = "shop_details.loaded" THEN ses.events_ga_session_id END), 0),
            5
        ),
        0
    ) AS cvr3
FROM `dh-logistics-product-ops.pricing.all_metrics_after_session_order_cvr_filters_loved_brands_scaled_code` AS ven
LEFT JOIN `dh-logistics-product-ops.pricing.ga_dps_sessions_loved_brands_scaled_code` AS ses
    ON TRUE
        AND ven.entity_id = ses.entity_id
        AND ven.country_code = ses.country_code
        AND ven.vendor_code = ses.vendor_code
WHERE TRUE
    AND ses.df_total IS NOT NULL -- Remove DPS sessions that do not return a DF value because any such record would be meaningless
    AND CONCAT(ven.entity_id, " | ", ven.country_code, " | ", ven.master_asa_id, " | ", ses.df_total) IN (
        SELECT DISTINCT CONCAT(entity_id, " | ", country_code, " | ", master_asa_id, " | ", fee) AS entity_country_asa_fee
        FROM `dh-logistics-product-ops.pricing.df_tiers_per_asa_loved_brands_scaled_code`
    )
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8 -- Filter for the main DF thresholds under each ASA (RIGHT NOW as reported by dps_config_versions) because the DF tiers that were obtained from the logs could contain ones that are not related to the ASA
;

###---------------------------------------------------------------------------------------END OF STEP 14.1---------------------------------------------------------------------------------------###

-- Step 14.2: Calculate the CVR3 at the smallest DF per vendor so that we can calculate the percentage drop in CVR3 from that base. Also, include the min DF per vendor in this table
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.df_and_cvr3_at_min_tier_vendor_level_loved_brands_scaled_code` AS
WITH b AS (
    SELECT
        region,
        entity_id,
        country_code,
        master_asa_id,
        asa_common_name,
        vendor_count_caught_by_asa,
        vendor_code,
        MIN(df_total) AS min_df_total_of_vendor
    FROM `dh-logistics-product-ops.pricing.cvr_per_df_bucket_vendor_level_loved_brands_scaled_code`
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT
    a.region,
    a.entity_id,
    a.country_code,
    a.master_asa_id,
    a.asa_common_name,
    a.vendor_count_caught_by_asa,
    a.vendor_code,
    b.min_df_total_of_vendor,
    a.cvr3 AS vendor_cvr3_at_min_df
FROM `dh-logistics-product-ops.pricing.cvr_per_df_bucket_vendor_level_loved_brands_scaled_code` AS a
INNER JOIN b ON a.entity_id = b.entity_id AND a.country_code = b.country_code AND a.master_asa_id = b.master_asa_id AND a.vendor_code = b.vendor_code AND a.df_total = b.min_df_total_of_vendor
;

###---------------------------------------------------------------------------------------END OF STEP 14.2---------------------------------------------------------------------------------------###

-- Step 14.3: Calculate the percentage drop in CVR3 from the base/min DF that was calculated in the previous step and append the result to the first table "cvr_per_df_bucket_vendor_level_loved_brands_scaled_code"
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.cvr_per_df_bucket_vendor_level_loved_brands_scaled_code` AS
WITH add_tier_rank AS (
    SELECT
        a.*,
        b.min_df_total_of_vendor,
        b.vendor_cvr3_at_min_df,
        CASE WHEN a.cvr3 = b.vendor_cvr3_at_min_df THEN NULL ELSE ROUND(a.cvr3 / NULLIF(b.vendor_cvr3_at_min_df, 0) - 1, 4) END AS pct_chng_of_actual_cvr3_from_base,
        ROW_NUMBER() OVER (PARTITION BY a.entity_id, a.country_code, a.master_asa_id, a.vendor_code ORDER BY a.df_total) AS tier_rank_vendor
    FROM `dh-logistics-product-ops.pricing.cvr_per_df_bucket_vendor_level_loved_brands_scaled_code` AS a
    LEFT JOIN `dh-logistics-product-ops.pricing.df_and_cvr3_at_min_tier_vendor_level_loved_brands_scaled_code` AS b USING (entity_id, country_code, master_asa_id, vendor_code)
)

SELECT 
    *,
    MAX(tier_rank_vendor) OVER (PARTITION BY entity_id, country_code, master_asa_id, vendor_code) AS num_tiers_vendor
FROM add_tier_rank
;

###---------------------------------------------------------------------------------------END OF STEP 14.3---------------------------------------------------------------------------------------###

-- Step 15.1: Calculate the **overall** CVR3 per DF tier/bucket (i.e., on the ASA level including all the schemes and DF tiers under it). This will be the average performance that we will compare each vendor against
-- The average performance is defined by pooling together the top 25% of vendors in terms of visits, orders, and CVR3. The bottom 75% of vendors are NOT taken into account.
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.cvr_per_df_bucket_asa_level_loved_brands_scaled_code` AS
SELECT
    ven.region,
    ven.entity_id,
    ven.country_code,
    ven.master_asa_id,
    ven.asa_common_name,
    ven.vendor_count_caught_by_asa,
    ses.df_total,
    -- If a vendor was visited more than once in the same session, this is considered one visit
    COALESCE(COUNT(DISTINCT CASE WHEN ses.event_action = "shop_details.loaded" THEN ses.events_ga_session_id END), 0) AS num_unique_vendor_visits,
    -- If a vendor was visited more than once in the same session, all impressions are counted
    COALESCE(COUNT(DISTINCT CASE WHEN ses.event_action = "transaction" THEN ses.events_ga_session_id END), 0) AS num_transactions,
    COALESCE(
        ROUND(
            COUNT(DISTINCT CASE WHEN ses.event_action = "transaction" THEN ses.events_ga_session_id END)
            / NULLIF(COUNT(DISTINCT CASE WHEN ses.event_action = "shop_details.loaded" THEN ses.events_ga_session_id END), 0),
            5
        ),
        0
    ) AS asa_cvr3_per_df
FROM `dh-logistics-product-ops.pricing.all_metrics_after_session_order_cvr_filters_loved_brands_scaled_code` AS ven
LEFT JOIN `dh-logistics-product-ops.pricing.ga_dps_sessions_loved_brands_scaled_code` AS ses
    ON TRUE
        AND ven.entity_id = ses.entity_id
        AND ven.country_code = ses.country_code
        AND ven.vendor_code = ses.vendor_code
WHERE TRUE
    AND ses.df_total IS NOT NULL -- Remove DPS sessions that do not return a DF value because any such record would be meaningless
    AND CONCAT(ven.entity_id, " | ", ven.country_code, " | ", ven.master_asa_id, " | ", ses.df_total) IN (
        SELECT DISTINCT CONCAT(entity_id, " | ", country_code, " | ", master_asa_id, " | ", fee) AS entity_country_asa_fee
        FROM `dh-logistics-product-ops.pricing.df_tiers_per_asa_loved_brands_scaled_code`
    ) -- Filter for the main DF thresholds under each ASA (RIGHT NOW as reported by dps_config_versions) because the DF tiers that were obtained from the logs could contain ones that are not related to the ASA
GROUP BY 1, 2, 3, 4, 5, 6, 7
;

###---------------------------------------------------------------------------------------END OF STEP 15.1---------------------------------------------------------------------------------------###

-- Step 15.2: Calculate the overall CVR3 at the smallest DF per ASA so that we can calculate the percentage drop in CVR3 from that base. Also, include the min DF per ASA in this table
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.df_and_cvr3_at_min_tier_asa_level_loved_brands_scaled_code` AS
WITH b AS (
    SELECT
        region,
        entity_id,
        country_code,
        master_asa_id,
        asa_common_name,
        vendor_count_caught_by_asa,
        MIN(df_total) AS min_df_total_of_asa
    FROM `dh-logistics-product-ops.pricing.cvr_per_df_bucket_asa_level_loved_brands_scaled_code`
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT
    a.region,
    a.entity_id,
    a.country_code,
    a.master_asa_id,
    a.asa_common_name,
    a.vendor_count_caught_by_asa,
    b.min_df_total_of_asa,
    a.asa_cvr3_per_df AS asa_cvr3_at_min_df
FROM `dh-logistics-product-ops.pricing.cvr_per_df_bucket_asa_level_loved_brands_scaled_code` AS a
INNER JOIN b ON a.entity_id = b.entity_id AND a.country_code = b.country_code AND a.master_asa_id = b.master_asa_id AND a.df_total = b.min_df_total_of_asa
;

###---------------------------------------------------------------------------------------END OF STEP 15.2---------------------------------------------------------------------------------------###

-- Step 15.3: Calculate the percentage drop in CVR3 from the base/min DF that was calculated in the previous step and append the result to the first table "cvr_per_df_bucket_asa_level_loved_brands_scaled_code"
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.cvr_per_df_bucket_asa_level_loved_brands_scaled_code` AS
WITH add_tier_rank AS (
    SELECT
        a.*,
        b.min_df_total_of_asa,
        b.asa_cvr3_at_min_df,
        CASE WHEN a.asa_cvr3_per_df = b.asa_cvr3_at_min_df THEN NULL ELSE ROUND(a.asa_cvr3_per_df / NULLIF(b.asa_cvr3_at_min_df, 0) - 1, 4) END AS pct_chng_of_asa_cvr3_from_base,
        ROW_NUMBER() OVER (PARTITION BY a.entity_id, a.country_code, a.master_asa_id ORDER BY a.df_total) AS tier_rank_master_asa
    FROM `dh-logistics-product-ops.pricing.cvr_per_df_bucket_asa_level_loved_brands_scaled_code` AS a
    LEFT JOIN `dh-logistics-product-ops.pricing.df_and_cvr3_at_min_tier_asa_level_loved_brands_scaled_code` AS b USING (entity_id, country_code, master_asa_id)
)

SELECT 
    *,
    MAX(tier_rank_master_asa) OVER (PARTITION BY entity_id, country_code, master_asa_id) AS num_tiers_master_asa
FROM add_tier_rank
;

###---------------------------------------------------------------------------------------END OF STEP 15.3---------------------------------------------------------------------------------------###

-- Step 16: Append the ASA level table "cvr_per_df_bucket_asa_level_loved_brands_scaled_code" to the vendor level table "cvr_per_df_bucket_vendor_level_loved_brands_scaled_code"
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.cvr_per_df_bucket_vendor_level_plus_cvr_thresholds_loved_brands_scaled_code` AS
SELECT
    -- Vendor level data
    a.* EXCEPT (cvr3, min_df_total_of_vendor, vendor_cvr3_at_min_df, pct_chng_of_actual_cvr3_from_base, tier_rank_vendor, num_tiers_vendor, vendor_cvr3_slope),
    a.min_df_total_of_vendor,
    a.vendor_cvr3_at_min_df,

    -- ASA level data at the min DF tier of the ASA
    b.min_df_total_of_asa, -- The min DF tier of the ASA
    b.asa_cvr3_at_min_df, -- CVR3 at the lowest DF tier of the ASA, not the lowest DF observed in the vendor's sessions

    -- ASA level data at each DF tier of the ASA. We only match to the DF tiers observed in the vendor's sessions
    c.asa_cvr3_per_df,
    c.pct_chng_of_asa_cvr3_from_base,
    c.asa_cvr3_slope,
    c.tier_rank_master_asa,
    c.num_tiers_master_asa,

    -- Vendor level data
    a.cvr3,
    a.pct_chng_of_actual_cvr3_from_base, -- The base here being the lowest DF tier observed in the vendor's sessions NOT the lowest DF of the ASA
    a.vendor_cvr3_slope,
    a.tier_rank_vendor,
    a.num_tiers_vendor,
    -- If the change in the vendor's CVR is > the change in the ASA's CVR, that's a YES. Otherwise, NO
    CASE WHEN a.pct_chng_of_actual_cvr3_from_base > c.pct_chng_of_asa_cvr3_from_base THEN "Y" ELSE "N" END AS is_lb_test_passed,
    -- If the slope from the linear regression on the vendor level > the slope on the ASA level, that's a YES. Otherwise, NO
    CASE
        -- If the slope on the vendor level is zero, this means that the vendor had a 0 CVR3, so we automatically label because the elasticity calculation in such case is void
        WHEN a.vendor_cvr3_slope = 0 THEN "N"
        WHEN a.vendor_cvr3_slope <= c.asa_cvr3_slope THEN "N"
        WHEN a.vendor_cvr3_slope IS NULL THEN "N" -- If the vendor_cvr3_slope IS NULL, this means that the vendor has a flat DF, so no elasticity can be calculated
        WHEN a.vendor_cvr3_slope > c.asa_cvr3_slope THEN "Y"
        ELSE "Unknown"
    END AS is_lb_test_passed_lm,
    -- The # of YES's per vendor
    SUM(CASE WHEN a.pct_chng_of_actual_cvr3_from_base > c.pct_chng_of_asa_cvr3_from_base THEN 1 ELSE 0 END) OVER (PARTITION BY a.entity_id, a.country_code, a.master_asa_id, a.vendor_code) AS num_yes
FROM `dh-logistics-product-ops.pricing.cvr_per_df_bucket_vendor_level_loved_brands_scaled_code` AS a
LEFT JOIN `dh-logistics-product-ops.pricing.df_and_cvr3_at_min_tier_asa_level_loved_brands_scaled_code` AS b
    ON TRUE
        AND a.entity_id = b.entity_id
        AND a.country_code = b.country_code
        AND a.master_asa_id = b.master_asa_id
LEFT JOIN `dh-logistics-product-ops.pricing.cvr_per_df_bucket_asa_level_loved_brands_scaled_code` AS c
    ON TRUE
        AND a.entity_id = c.entity_id
        AND a.country_code = c.country_code
        AND a.master_asa_id = c.master_asa_id
        AND a.df_total = c.df_total
;

###---------------------------------------------------------------------------------------END OF STEP 15.4---------------------------------------------------------------------------------------###

-- Step 17: Pull all the data associated with the "Loved Brands" that were obtained in the previous step
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.final_vendor_list_all_data_temp_loved_brands_scaled_code` AS
WITH temp_tbl AS (
    SELECT
        -- Grouping variables
        region,
        entity_id,
        country_code,
        master_asa_id,
        asa_common_name,
        vendor_count_caught_by_asa,
        vendor_code,
        -- Vendor data (DFs, CVR per DF tier, and percentage changes from the base tier to each subsequent one)
        CASE WHEN num_yes > 0 THEN "Y" ELSE "N" END AS is_lb,
        is_lb_test_passed_lm AS is_lb_lm,
        ARRAY_TO_STRING(ARRAY_AGG(CAST(df_total AS STRING) ORDER BY df_total), ", ") AS dfs_seen_in_sessions,
        ARRAY_TO_STRING(ARRAY_AGG(CAST(is_lb_test_passed AS STRING) ORDER BY df_total), ", ") AS is_lb_test_passed,
        ARRAY_TO_STRING(ARRAY_AGG(CAST(is_lb_test_passed_lm AS STRING) ORDER BY df_total), ", ") AS is_lb_test_passed_lm,
        
        ARRAY_TO_STRING(ARRAY_AGG(CAST(cvr3 AS STRING) ORDER BY df_total), ", ") AS vendor_cvr3_by_df,
        ARRAY_TO_STRING(ARRAY_AGG(CAST(pct_chng_of_actual_cvr3_from_base AS STRING) ORDER BY df_total), ", ") AS pct_chng_of_actual_cvr3_from_base,
        AVG(vendor_cvr3_slope) AS vendor_cvr3_slope,
        AVG(num_tiers_vendor) AS num_tiers_vendor,

        ARRAY_TO_STRING(ARRAY_AGG(CAST(asa_cvr3_per_df AS STRING) ORDER BY df_total), ", ") AS asa_cvr3_per_df,
        ARRAY_TO_STRING(ARRAY_AGG(CAST(pct_chng_of_asa_cvr3_from_base AS STRING) ORDER BY df_total), ", ") AS pct_chng_of_asa_cvr3_from_base,
        AVG(asa_cvr3_slope) AS asa_cvr3_slope,
        AVG(num_tiers_master_asa) AS num_tiers_master_asa
    FROM `dh-logistics-product-ops.pricing.cvr_per_df_bucket_vendor_level_plus_cvr_thresholds_loved_brands_scaled_code`
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
)

SELECT
    -- Identifiers
    a.region,
    a.entity_id,
    a.country_code,
    a.asa_id,
    a.master_asa_id,
    a.asa_name,
    a.asa_common_name,
    a.vendor_code,
    c.vertical_type,
    CASE WHEN b.is_lb IS NOT NULL THEN "Top 25%" ELSE "Bottom 75%" END AS vendor_rank,
    CASE WHEN b.is_lb_lm IS NOT NULL THEN "Top 25%" ELSE "Bottom 75%" END AS vendor_rank_lm,

    -- Vendor data (DFs, CVR per DF tier, and percentage changes from the base tier to each subsequent one) 
    COALESCE(b.is_lb, "N") AS is_lb, -- Impute the label of the vendor (pct chng method). If the vendor is in the bottom 75%, it's a non-LB by definition
    COALESCE(b.is_lb_lm, "N") AS is_lb_lm, -- Impute the label of the vendor (linear reg method). If the vendor is in the bottom 75%, it's a non-LB by definition
    b.dfs_seen_in_sessions,
    COALESCE(b.is_lb_test_passed, "N") AS is_lb_test_passed, -- If the vendor is in the bottom 75% of vendors, the LB test is not passed by definition
    COALESCE(b.is_lb_test_passed_lm, "N") AS is_lb_test_passed_lm, -- If the vendor is in the bottom 75% of vendors, the LB test is not passed by definition
    b.vendor_cvr3_by_df,
    b.pct_chng_of_actual_cvr3_from_base,
    b.vendor_cvr3_slope,
    CAST(b.num_tiers_vendor AS INT64) AS num_tiers_vendor,
    
    b.asa_cvr3_per_df,
    b.pct_chng_of_asa_cvr3_from_base,
    b.asa_cvr3_slope,
    CAST(b.num_tiers_master_asa AS INT64) AS num_tiers_master_asa,

    -- Vendor data (Busines metrics and other KPIs)
    a.num_orders,
    a.num_unique_vendor_visits,
    a.cvr3,
    a.orders_pct_rank,
    a.unique_visits_pct_rank,
    a.cvr3_pct_rank,

    -- ASA and entity data
    a.entity_orders,
    a.asa_orders,
    a.asa_order_share_of_entity,
    a.asa_orders_after_visits_filter,
    a.asa_orders_after_visits_and_orders_filters,
    a.asa_orders_after_all_initial_filters,
    SUM(CASE WHEN b.is_lb = "Y" THEN a.num_orders ELSE 0 END) OVER (PARTITION BY b.entity_id, b.country_code, a.asa_id) AS asa_orders_after_lb_logic, -- We use asa_id not master_asa_id to reflect the right granularity in the dashboard
    SUM(CASE WHEN b.is_lb_lm = "Y" THEN a.num_orders ELSE 0 END) OVER (PARTITION BY b.entity_id, b.country_code, a.asa_id) AS asa_orders_after_lb_logic_lm,
    a.asa_order_share_after_visits_filter,
    a.asa_order_share_after_visits_and_orders_filters,
    a.asa_order_share_after_all_initial_filters,
    ROUND(SUM(CASE WHEN b.is_lb = "Y" THEN a.num_orders ELSE 0 END) OVER (PARTITION BY b.entity_id, b.country_code, a.asa_id) / NULLIF(a.entity_orders, 0), 4) AS asa_order_share_after_lb_logic,
    ROUND(SUM(CASE WHEN b.is_lb_lm = "Y" THEN a.num_orders ELSE 0 END) OVER (PARTITION BY b.entity_id, b.country_code, a.asa_id) / NULLIF(a.entity_orders, 0), 4) AS asa_order_share_after_lb_logic_lm,

    -- Vendor count and share after each filtering stage
    a.vendor_count_caught_by_asa,
    a.vendor_count_remaining_after_visits_filter,
    a.vendor_count_remaining_after_visits_and_orders_filters,
    a.vendor_count_remaining_after_all_initial_filters,
    COUNT(DISTINCT CASE WHEN b.is_lb = "Y" THEN b.vendor_code END) OVER (PARTITION BY b.entity_id, b.country_code, a.asa_id) AS vendor_count_remaining_after_lb_logic,
    COUNT(DISTINCT CASE WHEN b.is_lb_lm = "Y" THEN b.vendor_code END) OVER (PARTITION BY b.entity_id, b.country_code, a.asa_id) AS vendor_count_remaining_after_lb_logic_lm,
    a.perc_vendors_remaining_after_visits_filter,
    a.perc_vendors_remaining_after_visits_and_orders_filters,
    a.perc_vendors_remaining_after_all_initial_filters,
    ROUND(COUNT(DISTINCT CASE WHEN b.is_lb = "Y" THEN b.vendor_code END) OVER (PARTITION BY b.entity_id, b.country_code, a.asa_id) / NULLIF(a.vendor_count_caught_by_asa, 0), 4) AS perc_vendors_remaining_after_lb_logic,
    ROUND(COUNT(DISTINCT CASE WHEN b.is_lb_lm = "Y" THEN b.vendor_code END) OVER (PARTITION BY b.entity_id, b.country_code, a.asa_id) / NULLIF(a.vendor_count_caught_by_asa, 0), 4) AS perc_vendors_remaining_after_lb_logic_lm,
    CURRENT_TIMESTAMP() AS update_timestamp
FROM `dh-logistics-product-ops.pricing.all_metrics_for_vendor_screening_loved_brands_scaled_code` AS a -- Contains ALL vendors (bottom 75% and top 25%)
LEFT JOIN temp_tbl AS b
    ON TRUE
        AND a.entity_id = b.entity_id
        AND a.country_code = b.country_code
        AND a.master_asa_id = b.master_asa_id
        AND a.vendor_code = b.vendor_code
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.vendors` AS c ON a.entity_id = c.global_entity_id AND a.vendor_code = c.vendor_id
;

###---------------------------------------------------------------------------------------END OF STEP 16---------------------------------------------------------------------------------------###

-- Step 18: Append the output of `final_vendor_list_all_data_temp_loved_brands_scaled_code` to the final table `final_vendor_list_all_data_loved_brands_scaled_code`
INSERT `dh-logistics-product-ops.pricing.final_vendor_list_all_data_loved_brands_scaled_code`
SELECT *
FROM `dh-logistics-product-ops.pricing.final_vendor_list_all_data_temp_loved_brands_scaled_code`
;
