-- Step 1: Declare your inputs
DECLARE entity_id_var STRING;
DECLARE start_date_var DATE;
DECLARE asa_id_var INT64;
DECLARE active_from_array ARRAY <TIMESTAMP>;
SET entity_id_var = "FP_PH";
SET start_date_var = DATE("2022-11-11");
SET asa_id_var = 573;
SET active_from_array = [CAST("2022-10-18 16:58:00 UTC" AS TIMESTAMP), CAST("2022-10-19 03:33:00 UTC" AS TIMESTAMP)];

-- Step 2: Get GA session data for all relevant vendors over the specified time frame
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ga_session_data_surge_data_exercise` AS
SELECT DISTINCT
    x.created_date, -- Date of the ga session
    x.entity_id, -- Entity ID
    x.country_code, -- Country code
    x.platform, -- Operating system (iOS, Android, Web, etc.)
    x.brand, -- Talabat, foodpanda, Foodora, etc.
    x.events_ga_session_id, -- GA session ID
    x.fullvisitor_id, -- The visit_id defined by Google Analytics
    x.visit_id, -- 	The visit_id defined by Google Analytics
    x.has_transaction, -- A field that indicates whether or not a session ended in a transaction
    x.total_transactions, -- The total number of transactions in the GA session
    x.ga_dps_session_id, -- DPS session ID

    x.sessions.dps_session_timestamp, -- The timestamp of the DPS logs
    x.sessions.endpoint, -- The endpoint from where the DPS request is coming
    x.sessions.perseus_client_id, -- A unique customer identifier based on the device
    x.sessions.variant, -- AB variant (e.g. Control, Variation1, Variation2, etc.)
    x.sessions.experiment_id AS test_id, -- Experiment ID
    CASE
        WHEN x.sessions.vertical_parent IS NULL THEN NULL
        WHEN LOWER(x.sessions.vertical_parent) IN ("restaurant", "restaurants") THEN "restaurant"
        WHEN LOWER(x.sessions.vertical_parent) = "shop" THEN "shop"
        WHEN LOWER(x.sessions.vertical_parent) = "darkstores" THEN "darkstores"
    END AS vertical_parent, -- Parent vertical
    x.sessions.customer_status, -- The customer.tag, indicating whether the customer is new or not
    x.sessions.location, -- The customer.location
    x.sessions.variant_concat, -- The concatenation of all the existing variants for the dps session id. There might be multiple variants due to location changes or session timeout
    x.sessions.location_concat, -- The concatenation of all the existing locations for the dps session id
    x.sessions.customer_status_concat, -- The concatenation of all the existing customer.tag for the dps session id

    e.event_action, -- Can have five values --> home_screen.loaded, shop_list.loaded, shop_details.loaded, checkout.loaded, transaction
    e.vendor_code, -- Vendor ID
    e.vertical_type, -- This field is NULL for event types home_screen.loaded and shop_list.loaded 
    e.event_time, -- The timestamp of the event's creation
    e.transaction_id, -- The transaction id for the GA session if the session has a transaction (i.e. order code)
    e.expedition_type, -- The delivery type of the session, pickup or delivery

    dps.city_id, -- City ID based on the DPS session
    dps.city_name, -- City name based on the DPS session
    dps.id AS zone_id, -- Zone ID based on the DPS session
    dps.name AS zone_name, -- Zone name based on the DPS session
    dps.timezone, -- Time zone of the city based on the DPS session

    ST_ASTEXT(x.ga_location) AS ga_location -- GA location expressed as a STRING
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_ga_sessions` AS x
LEFT JOIN UNNEST(events) AS e
LEFT JOIN UNNEST(dps_zone) AS dps
WHERE TRUE
    -- Choose the entity ID
    AND x.entity_id = entity_id_var
    -- Extract session data over the specified time frame
    AND x.created_date = start_date_var
    -- Filter for 'shop_details.loaded', 'transaction' events as we only need those to calculate CVR3
    AND e.event_action IN ("shop_details.loaded", "checkout.loaded", "transaction")
ORDER BY x.entity_id, x.events_ga_session_id, e.event_action 
;

/*
**Note 1:**
You can INNER JOIN on a dataset containing the latitude/longitude polygons of the zones to filter for sessions in relevant parts of the country
WITH geo_data AS (
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
)

...
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_ga_sessions` AS x
INNER JOIN geo_data AS cd
  ON TRUE
    AND x.entity_id = cd.entity_id
    AND x.country_code = cd.country_code
    AND dps.city_name = cd.city_name
    AND ST_CONTAINS(cd.zone_shape, x.ga_location)

**Note 2:**
You can filter for the relevant combinations of entity, country_code, and vendor_code by adding this condition to the WHERE clause

...
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_ga_sessions` AS x
LEFT JOIN UNNEST(events) AS e
WHERE TRUE
  AND CONCAT(x.entity_id, " | ", x.country_code, " | ", e.vendor_code) IN (
    SELECT DISTINCT CONCAT(entity_id, " | ", country_code, " | ", vendor_code) AS entity_country_vendor
    FROM `dh-logistics-product-ops.pricing.{vendor_code_list}`
  )
*/

###---------------------------------------------------------------------------------------END OF STEP 2---------------------------------------------------------------------------------------###

-- Step 3: Get data about the DF seen in the session from the DPS logs
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.dps_logs_surge_data_exercise` AS
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
    WHERE TRUE
        -- Filter for the relevant combinations of entity and country_code
        AND entity_id = entity_id_var
        -- Do NOT filter for multiplFee (MF) endpoints because the query times out if you do so. singleFee endpoint requests are sufficient for our purposes even though we lose a bit of data richness when we don't consider MF requests
        -- We lose data on the delivery fee seen in sessions for about 7.5% of all GA sessions in "session_data_for_vendor_screening_surge_data_exercise" by adding this filter. However, we gain much more in code efficiency   
        AND endpoint = "singleFee"
        -- Do NOT use the "start_date" and "end_date" params here. This will inc. the query size
        AND logs.created_date = start_date_var
        AND logs.customer.session.id IS NOT NULL -- We must have the dps session ID to be able to obtain the session's DF in the next query
),

dps_logs_stg_2 AS (
    SELECT DISTINCT
        dps.* EXCEPT(session_timestamp, created_at, vendors),
        v.id AS vendor_code,
        v.meta_data.scheme_id, -- You can also obtain the components by calling this field --> comp.id, comp.type, but you will have to unnest the array like so... --> LEFT JOIN UNNEST(v.components) comp. This will produce duplicates
        v.meta_data.vendor_price_scheme_type,
        v.delivery_fee.fleet_utilisation AS surge_fee,
        v.fleet_delay_zone.mean_delay,
        v.travel_time,
        dps.session_timestamp,
        dps.created_at,
        -- Create a row counter to take the last pricing config seen in the session for each vendor_id. We assume that this is the one that the customer took their decision to purchase/not purchase on
        ROW_NUMBER() OVER (PARTITION BY entity_id, country_code, dps_session_id, v.id ORDER BY created_at DESC) AS api_call_rank -- The DESC sorting here is important as we want the latest API call
    FROM dps_logs_stg_1 AS dps
    LEFT JOIN UNNEST(vendors) AS v
)
/*
**Note 1:**
You can filter for the specific zones to speed up querying

WITH geo_data AS (
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
)

...
FROM `fulfillment-dwh-production.cl.dynamic_pricing_user_sessions` AS logs
INNER JOIN `dh-logistics-product-ops.pricing.geo_data` AS cd ON logs.entity_id = cd.entity_id AND ST_CONTAINS(cd.zone_shape, logs.customer.location) -- Filter for sessions in the zones specified above

**Note 2:**
You can filter for the relevant combinations of entity, country_code, and vendor_code in this step by adding a WHERE clause

...
WHERE TRUE
    -- Filter for the relevant combinations of entity, country_code, and vendor_code
    AND CONCAT(dps.entity_id, " | ", dps.country_code, " | ", v.id) IN (
        SELECT DISTINCT CONCAT(entity_id, " | ", country_code, " | ", vendor_code) AS entity_country_vendor
        FROM `dh-logistics-product-ops.pricing.{vendor_id_list}`
    )

**Note 3:**
You can also filter for the relevant combinations of entity, country_code, and scheme_id by adding another condition to the WHERE clause

...
-- Filter for the relevant combinations of entity, country_code, and vendor_code
AND CONCAT(dps.entity_id, " | ", dps.country_code, " | ", v.meta_data.scheme_id) IN (
    SELECT DISTINCT CONCAT(entity_id, " | ", country_code, " | ", scheme_id) AS entity_country_scheme
    FROM `dh-logistics-product-ops.pricing.{scheme_id_id_list}`
)
*/

SELECT 
    *
FROM dps_logs_stg_2
QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_id, country_code, dps_session_id, vendor_code ORDER BY created_at DESC) = 1
ORDER BY dps_session_id, vendor_code, created_at
;

###---------------------------------------------------------------------------------------END OF STEP 3---------------------------------------------------------------------------------------###

-- Step 4: Join the DPS logs to the GA sessions data
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.ga_dps_sessions_surge_data_exercise` AS
SELECT
    x.*,
    logs.surge_fee,
    logs.scheme_id,
    logs.vendor_price_scheme_type,
    logs.mean_delay,
    logs.travel_time,
    logs.created_at AS dps_logs_created_at
FROM `dh-logistics-product-ops.pricing.ga_session_data_surge_data_exercise` AS x
-- With this join, ~ 12.9% of all rows get a NULL surge_fee as a result of filtering out multipleFee endpoint requests
LEFT JOIN `dh-logistics-product-ops.pricing.dps_logs_surge_data_exercise` AS logs -- You can use an INNER JOIN here if it's important to have a DF value associated with every session
    ON TRUE
        AND x.entity_id = logs.entity_id
        AND x.country_code = logs.country_code
        AND x.ga_dps_session_id = logs.dps_session_id
        -- **IMPORTANT**: Sometimes, the dps logs give us multiple delivery fees per session. One reason for this could be a change in location. We eliminated sessions with multiple DFs in the previous step to keep the dataset clean
        AND x.vendor_code = logs.vendor_code
ORDER BY events_ga_session_id, event_time
;

#############################################################################################################################################################################################################
#####------------------------------------------------------------------------END OF DPS LOGS AND GA SESSIONS QUERIES------------------------------------------------------------------------------------#####
#############################################################################################################################################################################################################

-- Step 5: TT fee config changes
WITH pull_tt_data AS (
  SELECT DISTINCT
    fcv.entity_id,
    fcv.asa_id,
    fcv.active_from,
    fcv.active_to, -- The last record has active_to
    fcv.asa_name,
    apc.scheme_id,
    apc.scheme_component_ids.travel_time_config_id AS travel_time_config_id,
    ROW_NUMBER() OVER (PARTITION BY fcv.entity_id, fcv.asa_id, fcv.active_from, apc.scheme_id, apc.scheme_component_ids.travel_time_config_id ORDER BY COALESCE(travel_time_threshold, 999999)) AS tt_tier_num,
    ttc.travel_time_fee,
    COALESCE(ttc.travel_time_threshold, 999999) AS travel_time_threshold,
  FROM `dh-logistics-product-ops.pricing.dps_asa_full_config_versions` fcv
  LEFT JOIN UNNEST(asa_price_config) apc
  LEFT JOIN UNNEST(apc.scheme_component_configs.travel_time_config) ttc
  WHERE active_from IN UNNEST(active_from_array) AND entity_id = entity_id_var AND asa_id = asa_id_var
  ORDER BY entity_id, asa_id, active_from, apc.scheme_id, apc.scheme_component_ids.travel_time_config_id, travel_time_threshold
),
agg_tt_data AS (
  SELECT
    * EXCEPT(travel_time_fee, travel_time_threshold, tt_tier_num),
    ARRAY_TO_STRING(ARRAY_AGG(CONCAT(travel_time_fee , " | ", travel_time_threshold) ORDER BY tt_tier_num), ", ") AS tt_fee_and_thr_concat,
  FROM pull_tt_data
  GROUP BY 1,2,3,4,5,6,7
),
apply_lag_func_tt AS (
  SELECT
    *,
    LAG(tt_fee_and_thr_concat) OVER (PARTITION BY entity_id, asa_id, scheme_id, travel_time_config_id ORDER BY active_from) AS tt_fee_and_thr_concat_prev_record 
  FROM agg_tt_data
)
SELECT 
  *,
  CASE 
    WHEN tt_fee_and_thr_concat = tt_fee_and_thr_concat_prev_record THEN FALSE
    WHEN tt_fee_and_thr_concat_prev_record IS NULL THEN FALSE
    ELSE TRUE
  END AS is_tt_fee_config_changed
FROM apply_lag_func_tt
ORDER BY entity_id, asa_id, scheme_id, travel_time_config_id, active_to DESC;

###---------------------------------------------------------------------------------------END OF STEP 5---------------------------------------------------------------------------------------###

-- Step 6: Service fee config changes
WITH pull_sf_data AS (
  SELECT DISTINCT
    fcv.entity_id,
    fcv.asa_id,
    fcv.active_from,
    fcv.active_to, -- The last record has active_to
    fcv.asa_name,
    apc.scheme_id,
    apc.scheme_component_ids.service_fee_config_id AS service_fee_config_id,
    sf.service_fee,
    COALESCE(sf.min_service_fee, 0) AS min_service_fee,
    COALESCE(sf.max_service_fee, 999999) AS max_service_fee
  FROM `dh-logistics-product-ops.pricing.dps_asa_full_config_versions` fcv
  LEFT JOIN UNNEST(asa_price_config) apc
  LEFT JOIN UNNEST(apc.scheme_component_configs.service_fee_config) sf
  WHERE active_from IN UNNEST(active_from_array) AND entity_id = entity_id_var AND asaId_var = asa_id_var
  ORDER BY entity_id, asa_id, active_from, apc.scheme_id, apc.scheme_component_ids.service_fee_config_id
),
agg_sf_data AS (
  SELECT
    * EXCEPT(service_fee, min_service_fee, max_service_fee),
    ARRAY_TO_STRING(ARRAY_AGG(CONCAT(service_fee, " | ", min_service_fee, " | ", max_service_fee)), ", ") AS service_fee_concat,
  FROM pull_sf_data
  GROUP BY 1,2,3,4,5,6,7
),
apply_lag_func_sf AS (
  SELECT
    *,
    LAG(service_fee_concat) OVER (PARTITION BY entity_id, asa_id, scheme_id, service_fee_config_id ORDER BY active_from) AS service_fee_concat_prev_record 
  FROM agg_sf_data
)
SELECT 
  *,
  CASE 
    WHEN service_fee_concat = service_fee_concat_prev_record THEN FALSE
    WHEN service_fee_concat_prev_record IS NULL THEN FALSE
    ELSE TRUE
  END AS is_sf_config_changed
FROM apply_lag_func_sf
ORDER BY entity_id, asa_id, scheme_id, service_fee_config_id, active_to DESC;

###---------------------------------------------------------------------------------------END OF STEP 6---------------------------------------------------------------------------------------###

-- Step 7: BVD config changes
WITH pull_bvd_data AS (
  SELECT DISTINCT
    fcv.entity_id,
    fcv.asa_id,
    fcv.active_from,
    fcv.active_to, -- The last record has active_to
    fcv.asa_name,
    apc.scheme_id,
    apc.scheme_component_ids.basket_value_config_id AS basket_value_config_id,
    bvd.basket_value_fee,
    COALESCE(bvd.basket_value_threshold, 999999) AS basket_value_threshold,
  FROM `dh-logistics-product-ops.pricing.dps_asa_full_config_versions` fcv
  LEFT JOIN UNNEST(asa_price_config) apc
  LEFT JOIN UNNEST(apc.scheme_component_configs.basket_value_config) bvd
  WHERE active_from IN UNNEST(active_from_array) AND entity_id = entity_id_var AND asaId_var = asa_id_var
  ORDER BY entity_id, asa_id, active_from, apc.scheme_id, apc.scheme_component_ids.basket_value_config_id
),
agg_bvd_data AS (
  SELECT
    * EXCEPT(basket_value_fee, basket_value_threshold),
    ARRAY_TO_STRING(ARRAY_AGG(CONCAT(basket_value_fee, " | ", basket_value_threshold) ORDER BY basket_value_threshold), ", ") AS basket_value_concat,
  FROM pull_bvd_data
  GROUP BY 1,2,3,4,5,6,7
),
apply_lag_func_bvd AS (
  SELECT
    *,
    LAG(basket_value_concat) OVER (PARTITION BY entity_id, asa_id, scheme_id, basket_value_config_id ORDER BY active_from) AS basket_value_concat_prev_record 
  FROM agg_bvd_data
)
SELECT
  *,
  CASE 
    WHEN basket_value_concat = basket_value_concat_prev_record THEN FALSE
    WHEN basket_value_concat_prev_record IS NULL THEN FALSE
    ELSE TRUE
  END AS is_bvd_config_changed
FROM apply_lag_func_bvd
ORDER BY entity_id, asa_id, scheme_id, basket_value_config_id, active_to DESC;

###---------------------------------------------------------------------------------------END OF STEP 7---------------------------------------------------------------------------------------###

-- Step 8: Small order fee config changes
WITH pull_sof_data AS (
  SELECT DISTINCT
    fcv.entity_id,
    fcv.asa_id,
    fcv.active_from,
    fcv.active_to, -- The last record has active_to
    fcv.asa_name,
    apc.scheme_id,
    COALESCE(apc.scheme_component_configs.small_order_fee_config.hard_mov, 0) AS hard_mov,
    COALESCE(apc.scheme_component_configs.small_order_fee_config.max_top_up, 999999) AS max_top_up,
  FROM `dh-logistics-product-ops.pricing.dps_asa_full_config_versions` fcv
  LEFT JOIN UNNEST(asa_price_config) apc
  WHERE active_from IN UNNEST(active_from_array) AND entity_id = entity_id_var AND asaId_var = asa_id_var
  ORDER BY entity_id, asa_id, active_from, apc.scheme_id
),
agg_sof_data AS (
  SELECT
    * EXCEPT(hard_mov, max_top_up),
    ARRAY_TO_STRING(ARRAY_AGG(CONCAT(hard_mov, " | ", max_top_up)), ", ") AS small_order_fee_concat,
  FROM pull_sof_data
  GROUP BY 1,2,3,4,5,6
),
apply_lag_func_sof AS (
  SELECT
    *,
    LAG(small_order_fee_concat) OVER (PARTITION BY entity_id, asa_id, scheme_id ORDER BY active_from) AS small_order_fee_concat_prev_record 
  FROM agg_sof_data
)
SELECT 
  *,
  CASE 
    WHEN small_order_fee_concat = small_order_fee_concat_prev_record THEN FALSE
    WHEN small_order_fee_concat_prev_record IS NULL THEN FALSE
    ELSE TRUE
  END AS is_sof_config_changed
FROM apply_lag_func_sof
ORDER BY entity_id, asa_id, scheme_id, active_to DESC;

###---------------------------------------------------------------------------------------END OF STEP 8---------------------------------------------------------------------------------------###

-- Step 8: MOV config changes
WITH pull_mov_data AS (
  SELECT DISTINCT
    fcv.entity_id,
    fcv.asa_id,
    fcv.active_from,
    fcv.active_to, -- The last record has active_to
    fcv.asa_name,
    apc.scheme_id,
    apc.scheme_component_ids.mov_config_id AS mov_config_id,
    mov.minimum_order_value,
    COALESCE(mov.travel_time_threshold, 999999) AS travel_time_threshold,
    COALESCE(sur.delay_threshold, 999999) AS surge_mov_delay_threshold,
    COALESCE(sur.surge_mov_value, 0) AS surge_mov_value
  FROM `dh-logistics-product-ops.pricing.dps_asa_full_config_versions` fcv
  LEFT JOIN UNNEST(asa_price_config) apc
  LEFT JOIN UNNEST(apc.scheme_component_configs.mov_config) mov
  LEFT JOIN UNNEST(mov.surge_mov_config) sur
  WHERE entity_id = entity_id_var AND asaId_var = 570
  ORDER BY entity_id, asa_id, active_from, apc.scheme_id, apc.scheme_component_ids.mov_config_id
),
agg_mov_data AS (
  SELECT
    * EXCEPT(minimum_order_value, travel_time_threshold, surge_mov_delay_threshold, surge_mov_value),
    ARRAY_TO_STRING(ARRAY_AGG(CONCAT(minimum_order_value, " | ", travel_time_threshold, " | ", surge_mov_delay_threshold, " | ", surge_mov_value) ORDER BY travel_time_threshold, surge_mov_delay_threshold), ", ") AS mov_concat,
  FROM pull_mov_data
  GROUP BY 1,2,3,4,5,6,7
),
apply_lag_func_mov AS (
  SELECT
    *,
    LAG(mov_concat) OVER (PARTITION BY entity_id, asa_id, scheme_id, mov_config_id ORDER BY active_from) AS mov_concat_prev_record 
  FROM agg_mov_data
)
SELECT
  *,
  CASE 
    WHEN mov_concat = mov_concat_prev_record THEN FALSE
    WHEN mov_concat_prev_record IS NULL THEN FALSE
    ELSE TRUE
  END AS is_mov_config_changed
FROM apply_lag_func_mov
ORDER BY entity_id, asa_id, scheme_id, mov_config_id, active_to DESC;

###---------------------------------------------------------------------------------------END OF STEP 9---------------------------------------------------------------------------------------###

-- Step 10: Surge DF config changes
WITH pull_surge_df_data AS (
  SELECT DISTINCT
    fcv.entity_id,
    fcv.asa_id,
    fcv.active_from,
    fcv.active_to, -- The last record has active_to
    fcv.asa_name,
    apc.scheme_id,
    apc.scheme_component_ids.delay_config_id AS surge_df_config_id,
    COALESCE(sur.travel_time_threshold, 999999) AS surge_df_travel_time_threshold,
    COALESCE(del.delay_threshold, 999999) AS surge_df_delay_threshold,
    COALESCE(del.delay_fee, 0) AS surge_df_value
  FROM `dh-logistics-product-ops.pricing.dps_asa_full_config_versions` fcv
  LEFT JOIN UNNEST(asa_price_config) apc
  LEFT JOIN UNNEST(apc.scheme_component_configs.fleet_delay_config) sur
  LEFT JOIN UNNEST(sur.delay_config) del
  WHERE active_from IN UNNEST(active_from_array) AND entity_id = entity_id_var AND asa_id = asa_id_var
  ORDER BY entity_id, asa_id, active_from, apc.scheme_id, apc.scheme_component_ids.delay_config_id, surge_df_travel_time_threshold, surge_df_delay_threshold
),
agg_surge_df_data AS (
  SELECT
    * EXCEPT(surge_df_travel_time_threshold, surge_df_delay_threshold, surge_df_value),
    ARRAY_TO_STRING(ARRAY_AGG(CONCAT(surge_df_travel_time_threshold, " | ", surge_df_delay_threshold, " | ", surge_df_value) ORDER BY surge_df_travel_time_threshold, surge_df_delay_threshold), ", ") AS surge_df_concat,
  FROM pull_surge_df_data
  GROUP BY 1,2,3,4,5,6,7
),
apply_lag_func_surge_df AS (
  SELECT
    *,
    LAG(surge_df_concat) OVER (PARTITION BY entity_id, asa_id, scheme_id, surge_df_config_id ORDER BY active_from) AS surge_df_concat_prev_record
  FROM agg_surge_df_data
)
SELECT
  *,
  CASE 
    WHEN surge_df_concat = surge_df_concat_prev_record THEN FALSE
    WHEN surge_df_concat_prev_record IS NULL THEN FALSE
    ELSE TRUE
  END AS is_surge_df_config_changed
FROM apply_lag_func_surge_df
ORDER BY entity_id, asa_id, scheme_id, surge_df_config_id, active_to DESC;