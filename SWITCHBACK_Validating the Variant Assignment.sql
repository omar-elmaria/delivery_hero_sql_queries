CREATE OR REPLACE TABLE `logistics-data-storage-staging.temp_pricing.sb_testing_data_audit` AS
WITH test_names AS (
  SELECT DISTINCT
    entity_id,
    test_name,
    test_id,
    test_start_date,
    test_end_date,
    experiment_type,
    switchback_window_in_mins
  FROM `fulfillment-dwh-production.cl.dps_experiment_setups`
  WHERE LOWER(experiment_type) = "switchback" AND test_end_date IS NULL
),

add_timestamp_array_start AS (
  SELECT
    *,
    GENERATE_TIMESTAMP_ARRAY(test_start_date, TIMESTAMP_ADD(test_start_date, INTERVAL 10 DAY), INTERVAL 1 HOUR) AS time_slot_start
  FROM test_names
),

time_slot_ranges AS (
  SELECT 
    * EXCEPT(time_slot_start),
    tss AS time_slot_start,
    LEAD(tss) OVER (PARTITION BY entity_id, test_name, test_id, test_start_date, test_end_date, experiment_type ORDER BY tss) AS time_slot_end,
    CAST(FLOOR(10000*RAND()) AS INT64) AS test_slot_random_number
  FROM add_timestamp_array_start
  LEFT JOIN UNNEST(time_slot_start) AS tss
),

order_data AS (
  SELECT
    -- Identifiers and supplementary fields     
    -- Date and time
    a.created_date AS created_date_utc,
    a.order_placed_at AS order_placed_at_utc,
    a.created_date_local,
    a.order_placed_at_local,
    DATETIME(dps_session_id_created_at, 'Asia/Karachi') AS dps_session_id_created_at_local,

    -- Location of order
    ent.segment AS region,
    a.entity_id,
    a.country_code,
    a.city_name,
    a.city_id,
    a.zone_name,
    a.zone_id,

    -- Order/customer identifiers and session data
    a.perseus_client_id,
    a.test_variant,
    a.test_slot_id,
    c.test_slot_random_number,
    a.test_id,
    b.test_name,
    CONCAT(b.test_name, " | ", a.created_date) AS test_name_date,
    a.is_in_treatment,
    a.platform_order_code,
    a.scheme_id,
    a.vendor_price_scheme_type,	-- The assignment type of the scheme to the vendor during the time of the order, such as "Automatic", "Manual", "Campaign", and "Country Fallback"

    -- Vendor data and information on the delivery
    a.vendor_id,
    b.target_group,
    a.vertical_type,
    a.vendor_vertical_parent,
    a.delivery_status,
    a.is_own_delivery,
    a.exchange_rate,

    -- Business KPIs (These are the components of profit)
    a.dps_delivery_fee_local,
    a.dps_travel_time_fee_local,
    a.dps_surge_fee_local,
    CASE WHEN a.dps_surge_fee_local != 0 AND a.dps_surge_fee_local IS NOT NULL THEN TRUE ELSE FALSE END AS is_surged,
    a.delivery_fee_local
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders` a
LEFT JOIN `fulfillment-dwh-production.cl.dps_test_orders` b ON a.entity_id = b.entity_id AND a.order_id = b.order_id
LEFT JOIN time_slot_ranges c ON a.entity_id = c.entity_id AND a.test_id = c.test_id AND DATETIME(dps_session_id_created_at, 'Asia/Karachi') >= CAST(c.time_slot_start AS DATETIME) AND DATETIME(dps_session_id_created_at, 'Asia/Karachi') <= CAST(c.time_slot_end AS DATETIME) 
INNER JOIN `fulfillment-dwh-production.curated_data_shared_coredata.global_entities` ent ON a.entity_id = ent.global_entity_id -- Get the region associated with every entity_id
WHERE TRUE
    AND a.created_date >= (SELECT MIN(DATE(test_start_date)) FROM test_names)
    AND b.test_name IN (SELECT test_name FROM test_names)
    AND a.is_own_delivery -- OD orders only
    AND a.is_match_test_vertical
    AND a.is_in_treatment
)

SELECT
  a.entity_id,
  a.test_name,
  a.test_id,
  a.test_slot_id,
  a.target_group,
  a.is_in_treatment,
  a.test_variant,
  a.scheme_id,
  a.vendor_price_scheme_type,
  b.test_start_date,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CAST(test_slot_random_number AS STRING) ORDER BY CAST(test_slot_random_number AS STRING)), ", ") AS test_slot_random_number,
  MIN(a.dps_session_id_created_at_local) AS min_timestamp_test_slot_id,
  MAX(a.dps_session_id_created_at_local) AS max_timestamp_test_slot_id,
  COUNT(DISTINCT a.platform_order_code) AS order_count
FROM order_data a
LEFT JOIN test_names b USING (entity_id, test_name)
GROUP BY 1,2,3,4,5,6,7,8,9,10
ORDER BY 1,2,3, MIN(a.dps_session_id_created_at_local), test_slot_id