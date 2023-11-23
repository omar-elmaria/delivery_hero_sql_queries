DECLARE entity_id_var STRING;
DECLARE start_date DATE;
DECLARE exp_id INT64;
DECLARE sb_interval_var INT64;
DECLARE num_days_time_array_gen INT64;
SET entity_id_var = "PY_PE";
SET start_date = DATE("2023-11-01");
SET exp_id = 108;
SET sb_interval_var = 2;
SET num_days_time_array_gen = 6;

CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.pe_sb_aa_test_analysis` AS
-- Pull the test data from dps_experiment_setups
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
  WHERE LOWER(experiment_type) = "switchback" AND entity_id = entity_id_var AND test_id = exp_id
),

-- Generate another timestamp array starting from the test_start_date and ending "num_days_time_array_gen" in the future. The interval of the timestamp should be the SB interval of the test (in this case 2 hours)
-- This will be used to construct the SB time intervals
add_timestamp_array_start AS (
  SELECT
    *,
    GENERATE_TIMESTAMP_ARRAY(test_start_date, TIMESTAMP_ADD(test_start_date, INTERVAL num_days_time_array_gen DAY), INTERVAL sb_interval_var HOUR) AS time_slot_start
  FROM test_names
),

-- Use the "LEAD" function to create a new column called "time_slot_end" so we can create an interval (time_slot_start | time_slot_end)
-- Use the RAND() function to generate a "random_test_slot_number" and "slot_variant". This is essentially replicating the DPS randomization algorithm synthetically
assignments AS (
  SELECT 
    tas.* EXCEPT(time_slot_start),
    tss AS time_slot_start,
    LEAD(tss) OVER (PARTITION BY entity_id, test_name, test_id, test_start_date, test_end_date, experiment_type ORDER BY tss) AS time_slot_end,
    CAST(FLOOR(100000*RAND()) AS INT64) AS test_slot_random_number,
    CASE
      WHEN RAND() <= 0.5 THEN "Control"
      ELSE "Variation1"
    END AS random_test_variant
  FROM add_timestamp_array_start tas
  LEFT JOIN UNNEST(time_slot_start) AS tss
),

-- Generate a timestamp array starting from the test_start_date and ending "num_days_time_array_gen" days in the future. The interval of the timestamp should be 1 minute
-- This will be used when joining the SB time intervals with order data
minute_slots_tbl AS (
  SELECT minute_slots
  FROM test_names
  LEFT JOIN UNNEST(GENERATE_TIMESTAMP_ARRAY(test_start_date, TIMESTAMP_ADD(test_start_date, INTERVAL num_days_time_array_gen DAY), INTERVAL 1 MINUTE)) AS minute_slots
),

-- Use the "LEAD" function to create a new column called "minute_slots_end" so we can create an interval (minute_slots_start | minute_slots_end)
-- The result of all the sub-queries until this point is a table with a 2-hour SB interval and 1-minute time periods within each interval 
assignments_enriched_with_minute_slots AS (
  SELECT
    a.*,
    b.minute_slots AS minute_slots_start,
    LEAD(minute_slots) OVER (PARTITION BY entity_id, test_name, test_id, test_start_date, test_end_date, experiment_type ORDER BY minute_slots) AS minute_slots_end,
  FROM assignments a
  LEFT JOIN minute_slots_tbl b ON b.minute_slots >= a.time_slot_start AND b.minute_slots < a.time_slot_end
  WHERE time_slot_end IS NOT NULL
),

###---------------------------------###---------------------------------###

### Pull the DPS logs data to get the session starting timestamp (customer.session.timestamp). This field is not yet curated to sessions_mapped_to_orders, which is why we have to use the logs
dps_logs AS (
  SELECT DISTINCT
    created_date
    , created_at
    , entity_id
    , customer.user_id AS perseus_client_id
    , customer.session.id AS dps_session_id
    , customer.session.timestamp AS dps_session_starting_timestamp
    , IF(experiments.id IS NULL, 0, experiments.id) AS experiment_id
    , experiments.variant AS experiment_variant
    , experiments.slot_id AS experiment_slot_id
  FROM `fulfillment-dwh-production.cl.dynamic_pricing_user_sessions`
  LEFT JOIN UNNEST(customer.experiments) experiments
  LEFT JOIN UNNEST(vendors) vendors
  WHERE TRUE
    AND entity_id IS NOT NULL AND entity_id = entity_id_var
    AND created_date > start_date
    AND experiments.id = exp_id
    AND endpoint = 'singleFee'
    -- filter out  invalid sessions ids, e.g. 'null' etc
    AND customer.session.id IS NOT NULL
    AND customer.session.id NOT IN UNNEST(ARRAY(SELECT id FROM `fulfillment-dwh-production.cl._bad_dps_logs_ids`))
    AND customer.user_id IS NOT NULL
    AND customer.user_id NOT IN UNNEST(ARRAY(SELECT id FROM `fulfillment-dwh-production.cl._bad_dps_logs_ids`))
    AND customer.location IS NOT NULL
    AND ST_ASTEXT(customer.location) != 'POINT(0 0)'
),

-- Pull the experiment's data from dps_sessions_mapped_to_orders
order_data AS (
  SELECT
    -- Order data
    a.region,
    a.country_code,
    a.country_name,
    a.city_id,
    a.city_name,
    a.zone_id,
    a.zone_name,
    a.timezone,
    a.entity_id,
    a.created_date,
    a.created_date_local,
    a.order_placed_at,
    a.order_placed_at_local,
    a.order_report_date,
    a.order_report_time,
    a.platform_order_code,
    d.target_group,
    a.is_in_treatment,
    a.scheme_id,
    a.vendor_price_scheme_type,
    a.dps_session_id,
    a.dps_session_id_created_at,
    -- DPS logs data
    b.dps_session_starting_timestamp,
    a.dps_session_timestamp AS dps_session_timestamp_smo,
    -- Order data
    a.test_id,
    a.test_variant,
    a.test_slot_id,
    c.test_start_date,
    c.test_end_date,
    c.switchback_window_in_mins,
    a.dps_mean_delay
  FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders` a
  LEFT JOIN dps_logs b ON a.entity_id = b.entity_id AND a.dps_session_id = b.dps_session_id AND a.test_id = b.experiment_id AND a.dps_session_id_created_at = b.created_at
  LEFT JOIN test_names c ON a.entity_id = c.entity_id AND a.test_id = c.test_id
  LEFT JOIN `fulfillment-dwh-production.cl.dps_test_orders` d ON a.entity_id = d.entity_id AND a.platform_order_code = d.platform_order_code
  WHERE TRUE
    AND a.entity_id = entity_id_var
    AND a.created_date > start_date
    AND a.test_id = exp_id
    AND a.is_in_treatment
)

-- Join the experiment's data with the synthetically created time intervals
-- The result is a table where each row is a 1-minute interval. If an order was made between minute_slots_start and minute_slots_end, its data will be shown. Otherwise, the order data will show NULLs
SELECT
  a.*,
  DATETIME(time_slot_start, 'America/Lima') AS time_slot_start_local,
  DATETIME(time_slot_end, 'America/Lima') AS time_slot_end_local,
  DATETIME(minute_slots_start, 'America/Lima') AS minute_slots_start_local,
  DATETIME(minute_slots_end, 'America/Lima') AS minute_slots_end_local,
  b.created_date,
  b.created_date_local,
  b.order_placed_at,
  b.order_placed_at_local,
  b.order_report_date,
  b.order_report_time,
  b.platform_order_code,
  b.target_group,
  b.is_in_treatment,
  b.scheme_id,
  b.vendor_price_scheme_type,
  b.dps_session_starting_timestamp,
  b.dps_session_timestamp_smo,
  b.test_variant,
  b.test_slot_id,
  b.dps_mean_delay
FROM assignments_enriched_with_minute_slots a
LEFT JOIN order_data b ON TRUE
  AND a.entity_id = b.entity_id
  AND a.test_id = b.test_id
  AND DATETIME(b.dps_session_starting_timestamp) >= CAST(a.minute_slots_start AS DATETIME)
  AND DATETIME(b.dps_session_starting_timestamp) < CAST(a.minute_slots_end AS DATETIME)
ORDER BY a.minute_slots_start