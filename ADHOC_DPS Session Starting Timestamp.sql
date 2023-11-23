DECLARE entity_id_var STRING;
DECLARE start_date DATE;
DECLARE exp_id INT64;
SET entity_id_var = "FP_BD";
SET start_date = DATE("2023-10-16");
SET exp_id = 65;

WITH dps_logs AS (
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

dps_logs_enriched AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY dps_session_id ORDER BY dps_session_starting_timestamp) AS row_num
  FROM dps_logs
)

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
  a.dps_session_id,
  a.dps_session_id_created_at,
  -- DPS logs data
  b.dps_session_starting_timestamp,
  -- Order data
  a.test_id,
  a.test_variant,
  a.test_slot_id,
  a.dps_mean_delay
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders` a
LEFT JOIN dps_logs_enriched b ON a.entity_id = b.entity_id AND a.dps_session_id = b.dps_session_id AND a.test_id = b.experiment_id AND a.dps_session_id_created_at = b.created_at
WHERE TRUE
  AND a.entity_id = entity_id_var
  AND a.created_date > start_date
  AND a.test_id = exp_id
ORDER BY a.platform_order_code, b.dps_session_starting_timestamp