-- Declare input variables
DECLARE entity_id_var STRING;
SET entity_id_var = "FP_KH";

-- ASA IDs with the same surge component
WITH asa_data AS (
  SELECT
    entity_id,
    asa_name,
    asa_id,
    active_from,
    active_to,
    asa.priority AS asa_priority,
    assigned_vendors_count,
    n_schemes,
    pc.scheme_id,
    pc.priority AS scheme_priority,
    is_default_scheme,
    customer_condition_id,
    schedule_id AS time_condition_id,
    scheme_component_ids.delay_config_id AS surge_component_id,
    COALESCE(fdc.travel_time_threshold, 999999) AS travel_time_threshold,
    COALESCE(dc.delay_threshold, 999999) AS delay_threshold,
    dc.delay_fee
  FROM `fulfillment-dwh-production.cl.pricing_asa_full_configuration_versions` asa
  LEFT JOIN UNNEST(asa_price_config) pc
  LEFT JOIN UNNEST(scheme_component_configs.fleet_delay_config) AS fdc
  LEFT JOIN UNNEST(fdc.delay_config) AS dc
  WHERE TRUE
    AND entity_id = entity_id_var
    AND active_to IS NULL -- Pull the latest ASA version
  -- It's not a best practice to perform an ORDER BY command in CTEs but I included the order of the columns here so you know how to sort the columns correctly to display the surge setup 
  ORDER BY entity_id, asa_id, pc.scheme_id, scheme_priority, COALESCE(fdc.travel_time_threshold, 999999), COALESCE(dc.delay_threshold, 999999), dc.delay_fee
)

SELECT
  entity_id,
  surge_component_id,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CAST(asa_id AS STRING) ORDER BY CAST(asa_id AS STRING)), ", ") AS asa_ids_with_same_surge_component
FROM asa_data
GROUP BY 1,2;

###--------------------------------###--------------------------------###

-- ASA IDs with the same scheme ID
WITH asa_data AS (
  SELECT
    entity_id,
    asa_name,
    asa_id,
    active_from,
    active_to,
    asa.priority AS asa_priority,
    assigned_vendors_count,
    n_schemes,
    pc.scheme_id,
    pc.priority AS scheme_priority,
    is_default_scheme,
    customer_condition_id,
    schedule_id AS time_condition_id,
    scheme_component_ids.delay_config_id AS surge_component_id,
    COALESCE(fdc.travel_time_threshold, 999999) AS travel_time_threshold,
    COALESCE(dc.delay_threshold, 999999) AS delay_threshold,
    dc.delay_fee
  FROM `fulfillment-dwh-production.cl.pricing_asa_full_configuration_versions` asa
  LEFT JOIN UNNEST(asa_price_config) pc
  LEFT JOIN UNNEST(scheme_component_configs.fleet_delay_config) AS fdc
  LEFT JOIN UNNEST(fdc.delay_config) AS dc
  WHERE TRUE
    AND entity_id = entity_id_var
    AND active_to IS NULL -- Pull the latest ASA version
  -- It's not a best practice to perform an ORDER BY command in CTEs but I included the order of the columns here so you know how to sort the columns correctly to display the surge setup 
  ORDER BY entity_id, asa_id, pc.scheme_id, scheme_priority, COALESCE(fdc.travel_time_threshold, 999999), COALESCE(dc.delay_threshold, 999999), dc.delay_fee
)

SELECT
  entity_id,
  scheme_id,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CAST(asa_id AS STRING) ORDER BY CAST(asa_id AS STRING)), ", ") AS asa_ids_with_same_surge_component
FROM asa_data
GROUP BY 1,2;