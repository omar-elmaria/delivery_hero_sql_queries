DECLARE entity_id_var STRING;
DECLARE start_date DATE;
DECLARE exp_id INT64;
DECLARE sb_interval_var INT64;
DECLARE num_days_time_array_gen INT64;
SET entity_id_var = "FP_BD";
SET start_date = DATE("2023-10-16");
SET exp_id = 65;
SET sb_interval_var = 2;
SET num_days_time_array_gen = 5;

CREATE OR REPLACE TABLE `logistics-data-storage-staging.temp_pricing.bd_sb_aa_test_time_intervals` AS
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

min_slots_tbl AS (
  SELECT min_slots
  FROM test_names
  LEFT JOIN UNNEST(GENERATE_TIMESTAMP_ARRAY(test_start_date, TIMESTAMP_ADD(test_start_date, INTERVAL num_days_time_array_gen DAY), INTERVAL 1 MINUTE)) AS min_slots
),

add_timestamp_array_start AS (
  SELECT
    *,
    GENERATE_TIMESTAMP_ARRAY(test_start_date, TIMESTAMP_ADD(test_start_date, INTERVAL num_days_time_array_gen DAY), INTERVAL sb_interval_var HOUR) AS time_slot_start
  FROM test_names
),

assignments AS (
  SELECT 
    tas.* EXCEPT(time_slot_start),
    tss AS time_slot_start,
    LEAD(tss) OVER (PARTITION BY entity_id, test_name, test_id, test_start_date, test_end_date, experiment_type ORDER BY tss) AS time_slot_end,
    CAST(FLOOR(10000*RAND()) AS INT64) AS test_slot_random_number,
    CASE
      WHEN RAND() <= 0.5 THEN "Control"
      ELSE "Variation1"
    END AS random_test_variant
  FROM add_timestamp_array_start tas
  LEFT JOIN UNNEST(time_slot_start) AS tss
)

SELECT
  a.*,
  b.min_slots AS min_slots_start,
  LEAD(min_slots) OVER (PARTITION BY entity_id, test_name, test_id, test_start_date, test_end_date, experiment_type ORDER BY min_slots) AS min_slots_end,
FROM assignments a
LEFT JOIN min_slots_tbl b ON b.min_slots >= a.time_slot_start AND b.min_slots < a.time_slot_end
WHERE time_slot_end IS NOT NULL;

SELECT *
FROM `logistics-data-storage-staging.temp_pricing.bd_sb_aa_test_time_intervals`
ORDER BY time_slot_start, min_slots;
