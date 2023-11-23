WITH dps AS (
    SELECT DISTINCT entity_id
    FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`
),

active_entities AS (
  SELECT
    ent.region,
    p.entity_id,
    LOWER(ent.country_iso) AS country_code,
    ent.country_name
  FROM `fulfillment-dwh-production.cl.entities` AS ent
  LEFT JOIN UNNEST(platforms) AS p
  INNER JOIN dps ON p.entity_id = dps.entity_id
  WHERE TRUE
    -- Eliminate entities starting with ODR (on-demand riders)
    AND p.entity_id NOT LIKE "ODR%"
    -- Eliminate entities starting with DN_ as they are not part of DPS
    AND p.entity_id NOT LIKE "DN_%"
    -- Eliminate irrelevant entity_ids in APAC
    AND p.entity_id NOT IN ("FP_DE", "FP_JP", "BM_VN", "BM_KR")
    -- Eliminate irrelevant entity_ids in MENA
    AND p.entity_id NOT IN (
        "TB_SA", "HS_BH", "CG_QA", "IN_AE", "ZO_AE", "IN_BH"
    )
    -- Eliminate irrelevant entity_ids in Europe
    AND p.entity_id NOT IN (
        "TB_SA", "HS_BH", "CG_QA", "IN_AE", "ZO_AE", "IN_BH"
    )
    -- Eliminate irrelevant entity_ids in LATAM
    AND p.entity_id NOT IN ("CD_CO")
),

asa_setups AS (
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
  FROM `fulfillment-dwh-production.cl.pricing_asa_full_configuration_versions` AS asa
  INNER JOIN active_entities AS ent ON asa.entity_id = ent.entity_id -- Filter only for active DH entities
  LEFT JOIN UNNEST(sorted_assigned_vendor_ids) AS vendor_code
  LEFT JOIN UNNEST(asa.asa_price_config) AS pc
  WHERE asa.active_to IS NULL AND asa.priority > 0 -- Get the most up-to-date ASA assignment setup
  ORDER BY ent.region, asa.entity_id, asa.country_code, asa.asa_id, asa.priority, vendor_code, pc.priority, pc.scheme_id
),

vendor_ids_per_asa AS (
  SELECT DISTINCT
    region,
    entity_id,
    country_code,

    asa_id,
    asa_name,
    is_asa_clustered, -- Flag to determine if the ASA was clustered before or not
    vendor_count_caught_by_asa,
    vendor_code
FROM `dh-logistics-product-ops.pricing.asa_setups_loved_brands_scaled_code`
WHERE TRUE
    AND time_condition_id IS NULL -- Filter for records without a time condition because the vendor codes would be duplicated if we have ASA configs with a time condition
    AND customer_condition_id IS NULL -- Filter for records without a customer condition becaue the vendor codes would be duplicated if we have ASA configs with a time condition
),

session_data AS (
  SELECT DISTINCT
    x.created_date, -- Date of the ga session
    x.entity_id, -- Entity ID
    x.country_code, -- Country code
    x.events_ga_session_id, -- GA session ID
    x.ga_dps_session_id, -- DPS session ID
    x.sessions.perseus_client_id, -- A unique customer identifier based on the device
    
    e.event_action, -- Can have five values --> home_screen.loaded, shop_list.loaded, shop_details.loaded, checkout.loaded, transaction
    e.vendor_code, -- Vendor ID
    vasa.asa_id, -- ASA ID
    vasa.asa_name, -- ASA Name
    e.event_time, -- The timestamp of the event's creation
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_ga_sessions` AS x
LEFT JOIN UNNEST(events) AS e
LEFT JOIN UNNEST(dps_zone) AS dps
LEFT JOIN vendor_ids_per_asa AS vasa ON x.entity_id = vasa.entity_id AND e.vendor_code = vasa.vendor_code
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
)

SELECT
  v.entity_id,
  v.country_code,
  v.asa_id,
  v.asa_name,
  COALESCE(COUNT(DISTINCT CASE WHEN event_action = "shop_details.loaded" THEN events_ga_session_id END), 0) AS num_unique_vendor_visits, -- If a vendor was visited more than once in the same session, it's one visit
  COALESCE(COUNT(DISTINCT CASE WHEN event_action = "transaction" THEN events_ga_session_id END), 0) AS num_transactions,
  COALESCE(
      ROUND(COUNT(DISTINCT CASE WHEN event_action = "transaction" THEN events_ga_session_id END) / NULLIF(COUNT(DISTINCT CASE WHEN event_action = "shop_details.loaded" THEN events_ga_session_id END), 0), 5),
      0
  ) AS asa_cvr3
FROM session_data AS v
GROUP BY 1, 2, 3, 4
ORDER BY 1,2,3,4