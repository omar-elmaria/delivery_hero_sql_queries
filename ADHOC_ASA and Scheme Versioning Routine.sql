CREATE OR REPLACE PROCEDURE `logistics-data-storage-staging.long_term_pricing.bq_routine_asa_and_scheme_versioning`(IN entity_id_var STRING, IN test_start_date DATE, IN test_end_date DATE, IN asa_ids ARRAY<INT64>, IN user STRING)
BEGIN
  /*
  If you do not provide any parameters to the routine, the following default values are used
  entity_id = "FP_PH"
  test_start_date = DATE("2022-10-01")
  test_end_date = DATE("2022-10-31")
  asa_ids = [487, 573]
  user = "john_doe"
  */

  -- Define query names
  DECLARE asa_overview_query STRING;
  DECLARE asa_change_log_query STRING;
  DECLARE asa_flags_non_component_related_changes_details_query STRING;
  DECLARE asa_flags_non_component_related_changes_query STRING;
  DECLARE asa_flags_component_related_changes_details_query STRING;
  DECLARE asa_flags_component_related_changes_query STRING;
  DECLARE asa_flags_query STRING;

  -- Part A: ASA Versioning
  -- Step 1: Create a table with the versions of a particular ASA sorted from the newest to the oldest
  SET asa_overview_query = '''
    CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.asa_overview_asa_and_scheme_versioning_'''|| COALESCE(user, "john_doe") ||'''`
    OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)) AS
    WITH versioning_tbl AS (
      SELECT
        entity_id,
        asa_id,
        active_from,
        COALESCE(active_to, TIMESTAMP_ADD(MAX(active_to) OVER (PARTITION BY entity_id, asa_id), INTERVAL 999 DAY)) AS active_to, -- The last record has active_to
        asa_name,
        priority,
        vendor_group_id,
        -- The assigned vendor hash changes if the assigned vendor count changes **OR** the number of assigned vendor stays the BUT an old vendor was replaced with a new one
        CAST(TO_BASE64(SHA256(asa_hashes.assigned_vendor_hash)) AS STRING) AS assigned_vendor_hash,
        assigned_vendors_count,
        n_schemes
      FROM `dh-logistics-product-ops.pricing.dps_asa_full_config_versions` -- A table created by @Sebastian Lafaurie
      WHERE TRUE
        AND asa_id IN UNNEST(COALESCE(@asa_ids_bqr, [487, 573]))
        AND entity_id = COALESCE(@entity_id_var_bqr, "FP_PH")
    )

    SELECT
      ROW_NUMBER() OVER (PARTITION BY entity_id, asa_id ORDER BY active_to DESC) AS active_to_ranking,	
      *,
    FROM versioning_tbl
    -- This is the part where we only filter for the ASA versions that existed during the experiment
    WHERE DATE(active_from) BETWEEN COALESCE(@test_start_date_bqr, DATE("2022-10-01")) AND COALESCE(@test_end_date_bqr, DATE("2022-10-31"))
    ORDER BY active_to DESC;
  ''';

  -- Step 2: Create flags to identify, which changes have implemented for a particular ASA
  -- Step 2.2: Filter for the ASA versions that existed during the lifetime of the experiment
  SET asa_change_log_query = '''
    CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.asa_change_log_asa_and_scheme_versioning_'''|| COALESCE(user, "john_doe") ||'''`
    OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)) AS
    WITH filter_asas AS (
      SELECT DISTINCT -- This "DISTINCT" is important to eliminate duplicates
        -- Grouping variables
        fcv.entity_id,
        fcv.asa_id,
        fcv.active_from,
        fcv.active_to, -- The last record has active_to
        TIMESTAMP_ADD(MAX(fcv.active_to) OVER (PARTITION BY fcv.entity_id, fcv.asa_id), INTERVAL 999 DAY) AS max_active_to,
        fcv.asa_name,
        fcv.priority AS asa_priority,
        fcv.vendor_group_id,
        CAST(TO_BASE64(SHA256(fcv.asa_hashes.assigned_vendor_hash)) AS STRING) AS assigned_vendor_hash,
        fcv.assigned_vendors_count,
        fcv.n_schemes,
        
        -- ASA price config
        ARRAY_TO_STRING(ARRAY_AGG(CAST(apc.asa_price_config_id AS STRING) ORDER BY apc.asa_price_config_id), ", ") AS asa_price_config_id,
        ARRAY_TO_STRING(ARRAY_AGG(CAST(apc.scheme_id AS STRING) ORDER BY apc.scheme_id), ", ") AS scheme_id,
        ARRAY_TO_STRING(ARRAY_AGG(CAST(apc.priority AS STRING) ORDER BY apc.priority), ", ") AS scheme_priority,
        -- Conditions
        ARRAY_TO_STRING(ARRAY_AGG(COALESCE(CAST(apc.customer_condition_id AS STRING), "NULL") ORDER BY apc.customer_condition_id), ", ") AS customer_condition_id,
        ARRAY_TO_STRING(ARRAY_AGG(COALESCE(CAST(apc.schedule_id AS STRING), "NULL") ORDER BY apc.schedule_id), ", ") AS schedule_id,
        -- Component ID
        ARRAY_TO_STRING(ARRAY_AGG(COALESCE(CAST(apc.scheme_component_ids.travel_time_config_id AS STRING), "NULL") ORDER BY apc.scheme_component_ids.travel_time_config_id), ", ") AS travel_time_config_id,
        ARRAY_TO_STRING(ARRAY_AGG(COALESCE(CAST(apc.scheme_component_ids.mov_config_id AS STRING), "NULL") ORDER BY apc.scheme_component_ids.mov_config_id), ", ") AS mov_config_id,	
        ARRAY_TO_STRING(ARRAY_AGG(COALESCE(CAST(apc.scheme_component_ids.delay_config_id AS STRING), "NULL") ORDER BY apc.scheme_component_ids.delay_config_id), ", ") AS delay_config_id,
        ARRAY_TO_STRING(ARRAY_AGG(COALESCE(CAST(apc.scheme_component_ids.basket_value_config_id AS STRING), "NULL") ORDER BY apc.scheme_component_ids.basket_value_config_id), ", ") AS basket_value_config_id,
        ARRAY_TO_STRING(ARRAY_AGG(COALESCE(CAST(apc.scheme_component_ids.service_fee_config_id AS STRING), "NULL") ORDER BY apc.scheme_component_ids.service_fee_config_id), ", ") AS service_fee_config_id,
      FROM `dh-logistics-product-ops.pricing.dps_asa_full_config_versions` fcv
      LEFT JOIN UNNEST(asa_price_config) apc
      INNER JOIN `dh-logistics-product-ops.pricing.asa_overview_asa_and_scheme_versioning_'''|| COALESCE(user, "john_doe") ||'''` ver
        ON fcv.entity_id = ver.entity_id AND fcv.asa_id = ver.asa_id AND fcv.active_from = ver.active_from
      GROUP BY 1,2,3,4,6,7,8,9,10,11
    )

    SELECT 
      entity_id,
      asa_id,
      active_from,
      COALESCE(active_to, max_active_to) AS active_to,
      * EXCEPT (entity_id, asa_id, active_to, active_from, max_active_to)
    FROM filter_asas
    ORDER BY COALESCE(active_to, max_active_to) DESC;
  ''';

  -- Step 3: Identify which parts of the ASA have been changed
  /* 
  We will split this section into two parts
  Part 1: Non-component configuration related changes
  1. ASA name
  2. ASA priority
  3. Assigned vendor hash (This changes if the assigned vendor count changes **OR** the number of assigned vendor stays the BUT an old vendor was replaced with a new one)
  4. Assigned vendor count
  5. Number of schemes
  6. Customer condition addition or removal
  7. Time condition addition or removal
  8. Travel time component addition or removal
  9. MOV component addition or removal
  10. Surge DF component addition or removal
  11. Basket value deals component addition or removal
  12. Service fee component addition or removal

  Part 2: Component configuration related changes
  1. Travel time component amendment (same config ID)
  2. MOV component amendment (same config ID)
  3. Surge DF component amendment (same config ID)
  4. Basket value deals component amendment (same config ID)
  5. Service fee component amendment (same config ID)
  */

  -- Part 1: Non-Component related changes
  -- Step 3.1: ASA flags non-component related changes details
  SET asa_flags_non_component_related_changes_details_query = '''
    CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.asa_flags_non_component_related_changes_details_asa_and_scheme_versioning_'''|| COALESCE(user, "john_doe") ||'''`
    OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)) AS
    WITH apply_lag_func AS (
      SELECT 
        *,
        LAG(asa_name) OVER (PARTITION BY entity_id, asa_id ORDER BY active_from) AS asa_name_prev_record,
        LAG(asa_priority) OVER (PARTITION BY entity_id, asa_id ORDER BY active_from) AS asa_priority_prev_record,
        LAG(assigned_vendor_hash) OVER (PARTITION BY entity_id, asa_id ORDER BY active_from) AS assigned_vendor_hash_prev_record,
        LAG(assigned_vendors_count) OVER (PARTITION BY entity_id, asa_id ORDER BY active_from) AS assigned_vendors_count_prev_record,
        LAG(n_schemes) OVER (PARTITION BY entity_id, asa_id ORDER BY active_from) AS n_schemes_prev_record,
        LAG(scheme_id) OVER (PARTITION BY entity_id, asa_id ORDER BY active_from) AS scheme_id_prev_record,
        LAG(customer_condition_id) OVER (PARTITION BY entity_id, asa_id ORDER BY active_from) AS customer_condition_id_prev_record,
        LAG(schedule_id) OVER (PARTITION BY entity_id, asa_id ORDER BY active_from) AS schedule_id_prev_record,
        LAG(travel_time_config_id) OVER (PARTITION BY entity_id, asa_id ORDER BY active_from) AS travel_time_config_id_prev_record,
        LAG(mov_config_id) OVER (PARTITION BY entity_id, asa_id ORDER BY active_from) AS mov_config_id_prev_record,
        LAG(delay_config_id) OVER (PARTITION BY entity_id, asa_id ORDER BY active_from) AS delay_config_id_prev_record,
        LAG(basket_value_config_id) OVER (PARTITION BY entity_id, asa_id ORDER BY active_from) AS basket_value_config_id_prev_record,
        LAG(service_fee_config_id) OVER (PARTITION BY entity_id, asa_id ORDER BY active_from) AS service_fee_config_id_prev_record,
      FROM `dh-logistics-product-ops.pricing.asa_change_log_asa_and_scheme_versioning_'''|| COALESCE(user, "john_doe") ||'''`
    )

    SELECT
      *,
      -- ASA name
      CASE 
        WHEN asa_name = asa_name_prev_record THEN FALSE
        WHEN asa_name_prev_record IS NULL THEN FALSE
        ELSE TRUE
      END AS is_asa_name_changed,
      
      -- ASA priority
      CASE 
        WHEN asa_priority = asa_priority_prev_record THEN FALSE
        WHEN asa_priority_prev_record IS NULL THEN FALSE
        ELSE TRUE
      END AS is_asa_priority_changed,
      
      -- Assigned vendor hash
      CASE 
        WHEN assigned_vendor_hash = assigned_vendor_hash_prev_record THEN FALSE
        WHEN assigned_vendor_hash_prev_record IS NULL THEN FALSE
        ELSE TRUE
      END AS is_assigned_vendor_hash_changed,
      
      -- Assigned vendor count
      CASE 
        WHEN assigned_vendors_count = assigned_vendors_count_prev_record THEN FALSE
        WHEN assigned_vendors_count_prev_record IS NULL THEN FALSE
        ELSE TRUE
      END AS is_assigned_vendor_count_changed,
      
      -- Number of schemes
      CASE 
        WHEN n_schemes = n_schemes_prev_record THEN FALSE
        WHEN n_schemes_prev_record IS NULL THEN FALSE
        ELSE TRUE
      END AS is_n_schemes_changed,
      
      -- Scheme ID
      CASE 
        WHEN scheme_id = scheme_id_prev_record THEN FALSE
        WHEN scheme_id_prev_record IS NULL THEN FALSE
        ELSE TRUE
      END AS is_scheme_id_changed,
      
      -- Customer condition
      CASE 
        WHEN customer_condition_id = customer_condition_id_prev_record THEN FALSE
        WHEN customer_condition_id_prev_record IS NULL THEN FALSE
        ELSE TRUE
      END AS is_customer_condition_changed,
      
      -- Time condition
      CASE 
        WHEN schedule_id = schedule_id_prev_record THEN FALSE
        WHEN schedule_id_prev_record IS NULL THEN FALSE
        ELSE TRUE
      END AS is_schedule_id_changed,
      
      -- Travel time config ID
      CASE 
        WHEN travel_time_config_id = travel_time_config_id_prev_record THEN FALSE
        WHEN travel_time_config_id_prev_record IS NULL THEN FALSE
        ELSE TRUE
      END AS is_tt_config_id_added_or_removed,
      
      -- MOV config ID
      CASE 
        WHEN mov_config_id = mov_config_id_prev_record THEN FALSE
        WHEN mov_config_id_prev_record IS NULL THEN FALSE
        ELSE TRUE
      END AS is_mov_config_id_added_or_removed,
      
      -- Surge DF config ID
      CASE 
        WHEN delay_config_id = delay_config_id_prev_record THEN FALSE
        WHEN delay_config_id_prev_record IS NULL THEN FALSE
        ELSE TRUE
      END AS is_surge_df_config_id_added_or_removed,
      
      -- Basket Value config ID
      CASE 
        WHEN basket_value_config_id = basket_value_config_id_prev_record THEN FALSE
        WHEN basket_value_config_id_prev_record IS NULL THEN FALSE
        ELSE TRUE
      END AS is_basket_value_config_id_added_or_removed,
      
      -- Service fee config ID
      CASE 
        WHEN service_fee_config_id = service_fee_config_id_prev_record THEN FALSE
        WHEN service_fee_config_id_prev_record IS NULL THEN FALSE
        ELSE TRUE
      END AS is_service_fee_config_id_added_or_removed,
    FROM apply_lag_func
    ORDER BY active_to DESC;
  ''';

  -- Step 3.2: Aggregate `dh-logistics-product-ops.pricing.asa_flags_non_component_related_changes_details_asa_and_scheme_versioning` to get the outcome on an ASA level
  SET asa_flags_non_component_related_changes_query = '''
    CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.asa_flags_non_component_related_changes_asa_and_scheme_versioning_'''|| COALESCE(user, "john_doe") ||'''`
    OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)) AS
    SELECT
      entity_id,
      asa_id,
      MAX(is_asa_name_changed) AS is_asa_name_changed,
      SUM(CASE WHEN is_asa_name_changed THEN 1 ELSE 0 END) AS asa_name_changes_count,
      
      MAX(is_asa_priority_changed) AS is_asa_priority_changed,
      SUM(CASE WHEN is_asa_priority_changed THEN 1 ELSE 0 END) AS asa_priority_changes_count,
      
      MAX(is_assigned_vendor_hash_changed) AS is_assigned_vendor_hash_changed,
      SUM(CASE WHEN is_assigned_vendor_hash_changed THEN 1 ELSE 0 END) AS assigned_vendor_hash_changes_count,

      MAX(is_assigned_vendor_count_changed) AS is_assigned_vendor_count_changed,
      SUM(CASE WHEN is_assigned_vendor_count_changed THEN 1 ELSE 0 END) AS assigned_vendor_count_changes_count,

      MAX(is_n_schemes_changed) AS is_n_schemes_changed,
      SUM(CASE WHEN is_n_schemes_changed THEN 1 ELSE 0 END) AS n_schemes_changes_count,

      MAX(is_scheme_id_changed) AS is_scheme_id_changed,
      SUM(CASE WHEN is_scheme_id_changed THEN 1 ELSE 0 END) AS scheme_id_changes_count,

      MAX(is_customer_condition_changed) AS is_customer_condition_changed,
      SUM(CASE WHEN is_customer_condition_changed THEN 1 ELSE 0 END) AS customer_condition_changes_count,

      MAX(is_schedule_id_changed) AS is_schedule_id_changed,
      SUM(CASE WHEN is_schedule_id_changed THEN 1 ELSE 0 END) AS schedule_id_changes_count,

      MAX(is_tt_config_id_added_or_removed) AS is_tt_config_id_added_or_removed,
      SUM(CASE WHEN is_tt_config_id_added_or_removed THEN 1 ELSE 0 END) AS tt_config_id_added_or_removed_count,

      MAX(is_mov_config_id_added_or_removed) AS is_mov_config_id_added_or_removed,
      SUM(CASE WHEN is_mov_config_id_added_or_removed THEN 1 ELSE 0 END) AS mov_config_id_added_or_removed_count,
      
      MAX(is_surge_df_config_id_added_or_removed) AS is_surge_df_config_id_added_or_removed,
      SUM(CASE WHEN is_surge_df_config_id_added_or_removed THEN 1 ELSE 0 END) AS is_surge_df_config_id_added_or_removed_count,

      MAX(is_basket_value_config_id_added_or_removed) AS is_basket_value_config_id_added_or_removed,
      SUM(CASE WHEN is_basket_value_config_id_added_or_removed THEN 1 ELSE 0 END) AS is_basket_value_config_id_added_or_removed_count,

      MAX(is_service_fee_config_id_added_or_removed) AS is_service_fee_config_id_added_or_removed,
      SUM(CASE WHEN is_service_fee_config_id_added_or_removed THEN 1 ELSE 0 END) AS is_service_fee_config_id_added_or_removed_count,
    FROM `dh-logistics-product-ops.pricing.asa_flags_non_component_related_changes_details_asa_and_scheme_versioning_'''|| COALESCE(user, "john_doe") ||'''`
    GROUP BY 1,2;
  ''';

  -- Part 2: Component related changes
  -- Step 3.3: ASA flags component related changes details
  SET asa_flags_component_related_changes_details_query = '''
    CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.asa_flags_component_related_changes_details_asa_and_scheme_versioning_'''|| COALESCE(user, "john_doe") ||'''`
    OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)) AS
    WITH pull_component_data AS (
      -- Note this sub-query will produce a lot of duplicates as we are joining components of different granularities to one another. We treat this in the following step
      SELECT DISTINCT
        -- Identifiers (grouping variables)
        fcv.entity_id,
        fcv.asa_id,
        fcv.active_from,
        COALESCE(fcv.active_to, ver.active_to) AS active_to, -- The last record has active_to
        fcv.asa_name,
        apc.scheme_id,

        -- Config IDs (grouping variables)
        apc.scheme_component_ids.travel_time_config_id AS travel_time_config_id,
        apc.scheme_component_ids.service_fee_config_id AS service_fee_config_id,
        apc.scheme_component_ids.basket_value_config_id AS basket_value_config_id,
        apc.scheme_component_ids.mov_config_id AS mov_config_id,
        apc.scheme_component_ids.delay_config_id AS surge_df_config_id,

        -- Component Details
        -- Travel time
        ROW_NUMBER() OVER (PARTITION BY fcv.entity_id, fcv.asa_id, fcv.active_from, apc.scheme_id, apc.scheme_component_ids.travel_time_config_id ORDER BY COALESCE(ttc.travel_time_threshold, 999999)) AS tt_tier_num,
        ttc.travel_time_fee,
        COALESCE(ttc.travel_time_threshold, 999999) AS travel_time_threshold,

        -- Service fee
        apc.scheme_component_configs.service_fee_config.service_fee,
        COALESCE(apc.scheme_component_configs.service_fee_config.min_service_fee, 0) AS min_service_fee,
        COALESCE(apc.scheme_component_configs.service_fee_config.max_service_fee, 999999) AS max_service_fee,

        -- Basket value deals
        bvd.basket_value_fee,
        COALESCE(bvd.basket_value_threshold, 999999) AS basket_value_threshold,

        -- Small order fee
        COALESCE(apc.scheme_component_configs.small_order_fee_config.hard_mov, 0) AS hard_mov,
        COALESCE(apc.scheme_component_configs.small_order_fee_config.max_top_up, 999999) AS max_top_up,

        -- MOV
        mov.minimum_order_value,
        COALESCE(mov.travel_time_threshold, 999999) AS mov_travel_time_threshold,
        COALESCE(sur.delay_threshold, 999999) AS surge_mov_delay_threshold,
        COALESCE(sur.surge_mov_value, 0) AS surge_mov_value,

        -- Surge DF
        COALESCE(surd.travel_time_threshold, 999999) AS surge_df_travel_time_threshold,
        COALESCE(del.delay_threshold, 999999) AS surge_df_delay_threshold,
        COALESCE(del.delay_fee, 0) AS surge_df_value
      FROM `dh-logistics-product-ops.pricing.dps_asa_full_config_versions` fcv
      LEFT JOIN UNNEST(asa_price_config) apc
      LEFT JOIN UNNEST(apc.scheme_component_configs.travel_time_config) ttc
      LEFT JOIN UNNEST(apc.scheme_component_configs.basket_value_config) bvd
      LEFT JOIN UNNEST(apc.scheme_component_configs.mov_config) mov
      LEFT JOIN UNNEST(mov.surge_mov_row_config) sur
      LEFT JOIN UNNEST(apc.scheme_component_configs.fleet_delay_config) surd
      LEFT JOIN UNNEST(surd.delay_config) del
      INNER JOIN `dh-logistics-product-ops.pricing.asa_overview_asa_and_scheme_versioning_'''|| COALESCE(user, "john_doe") ||'''` ver
        ON fcv.entity_id = ver.entity_id AND fcv.asa_id = ver.asa_id AND fcv.active_from = ver.active_from
      ORDER BY active_to DESC
    ),

    -- Aggregate the component details into STRING ARRAYS
    agg_component_data AS (
      SELECT
        * EXCEPT(
          travel_time_fee, travel_time_threshold, tt_tier_num, -- Travel time
          service_fee, min_service_fee, max_service_fee, -- Service fee
          basket_value_fee, basket_value_threshold, -- Basket value deals
          hard_mov, max_top_up, -- Small order fee
          minimum_order_value, mov_travel_time_threshold, surge_mov_delay_threshold, surge_mov_value, -- MOV
          surge_df_travel_time_threshold, surge_df_delay_threshold, surge_df_value -- Surge DF
        ),
        
        -- Aggregating component details
        -- The DISTINCT command is important here to handle the duplicates that were generated in the previous step
        -- When there are DISTINCT and ORDER BY commans in the ARRAY_AGG function, you can only sort by expressions that are arguments to the function
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CONCAT(travel_time_fee , " | ", travel_time_threshold) ORDER BY CONCAT(travel_time_fee , " | ", travel_time_threshold)), ", ") AS tt_fee_and_thr_concat,
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CONCAT(service_fee, " | ", min_service_fee, " | ", max_service_fee) ORDER BY CONCAT(service_fee, " | ", min_service_fee, " | ", max_service_fee)), ", ") AS service_fee_concat,
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CONCAT(basket_value_fee, " | ", basket_value_threshold) ORDER BY CONCAT(basket_value_fee, " | ", basket_value_threshold)), ", ") AS basket_value_concat,
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CONCAT(hard_mov, " | ", max_top_up) ORDER BY CONCAT(hard_mov, " | ", max_top_up)), ", ") AS small_order_fee_concat,
        ARRAY_TO_STRING(
          ARRAY_AGG(DISTINCT CONCAT(minimum_order_value, " | ", mov_travel_time_threshold, " | ", surge_mov_delay_threshold, " | ", surge_mov_value) 
          ORDER BY CONCAT(minimum_order_value, " | ", mov_travel_time_threshold, " | ", surge_mov_delay_threshold, " | ", surge_mov_value)
        ), ", ") AS mov_concat,
        ARRAY_TO_STRING(
          ARRAY_AGG(DISTINCT CONCAT(surge_df_travel_time_threshold, " | ", surge_df_delay_threshold, " | ", surge_df_value)
          ORDER BY CONCAT(surge_df_travel_time_threshold, " | ", surge_df_delay_threshold, " | ", surge_df_value)
        ), ", ") AS surge_df_concat,
      FROM pull_component_data
      GROUP BY 1,2,3,4,5,6,7,8,9,10,11
    ),

    -- Apply the LAG function to compare one instance of the component to the previous one
    apply_lag_func_component AS (
      SELECT
        *,
        LAG(tt_fee_and_thr_concat) OVER (PARTITION BY entity_id, asa_id, scheme_id, travel_time_config_id ORDER BY active_from) AS tt_fee_and_thr_concat_prev_record,
        LAG(service_fee_concat) OVER (PARTITION BY entity_id, asa_id, scheme_id, service_fee_config_id ORDER BY active_from) AS service_fee_concat_prev_record,
        LAG(basket_value_concat) OVER (PARTITION BY entity_id, asa_id, scheme_id, basket_value_config_id ORDER BY active_from) AS basket_value_concat_prev_record,
        LAG(small_order_fee_concat) OVER (PARTITION BY entity_id, asa_id, scheme_id ORDER BY active_from) AS small_order_fee_concat_prev_record,
        LAG(mov_concat) OVER (PARTITION BY entity_id, asa_id, scheme_id, mov_config_id ORDER BY active_from) AS mov_concat_prev_record,
        LAG(surge_df_concat) OVER (PARTITION BY entity_id, asa_id, scheme_id, surge_df_config_id ORDER BY active_from) AS surge_df_concat_prev_record
      FROM agg_component_data
    )

    -- Create flags indicating if the components have been changed
    SELECT 
      *,
      -- Travel time
      CASE 
        WHEN tt_fee_and_thr_concat = tt_fee_and_thr_concat_prev_record THEN FALSE
        WHEN tt_fee_and_thr_concat_prev_record IS NULL THEN FALSE
        ELSE TRUE
      END AS is_tt_fee_config_changed,

      -- Service fee
      CASE 
        WHEN service_fee_concat = service_fee_concat_prev_record THEN FALSE
        WHEN service_fee_concat_prev_record IS NULL THEN FALSE
        ELSE TRUE
      END AS is_sf_config_changed,

      -- Basket value deals
      CASE 
        WHEN basket_value_concat = basket_value_concat_prev_record THEN FALSE
        WHEN basket_value_concat_prev_record IS NULL THEN FALSE
        ELSE TRUE
      END AS is_bvd_config_changed,

      -- Small order fee
      CASE 
        WHEN small_order_fee_concat = small_order_fee_concat_prev_record THEN FALSE
        WHEN small_order_fee_concat_prev_record IS NULL THEN FALSE
        ELSE TRUE
      END AS is_sof_config_changed,

      -- MOV
      CASE 
        WHEN mov_concat = mov_concat_prev_record THEN FALSE
        WHEN mov_concat_prev_record IS NULL THEN FALSE
        ELSE TRUE
      END AS is_mov_config_changed,

      -- Surge DF
      CASE 
        WHEN surge_df_concat = surge_df_concat_prev_record THEN FALSE
        WHEN surge_df_concat_prev_record IS NULL THEN FALSE
        ELSE TRUE
      END AS is_surge_df_config_changed
    FROM apply_lag_func_component
    ORDER BY entity_id, asa_id, scheme_id, active_to DESC;
  ''';

  -- Step 3.4: Aggregate `dh-logistics-product-ops.pricing.asa_flags_component_related_changes_details_asa_and_scheme_versioning` to get the outcome on an ASA level
  SET asa_flags_component_related_changes_query = '''
    CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.asa_flags_component_related_changes_asa_and_scheme_versioning_'''|| COALESCE(user, "john_doe") ||'''`
    OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)) AS
    SELECT
      entity_id,
      asa_id,
      MAX(is_tt_fee_config_changed) AS is_tt_fee_config_changed,
      SUM(CASE WHEN is_tt_fee_config_changed THEN 1 ELSE 0 END) AS tt_fee_config_changes_count,
      
      MAX(is_sf_config_changed) AS is_sf_config_changed,
      SUM(CASE WHEN is_sf_config_changed THEN 1 ELSE 0 END) AS sf_config_changes_count,
      
      MAX(is_bvd_config_changed) AS is_bvd_config_changed,
      SUM(CASE WHEN is_bvd_config_changed THEN 1 ELSE 0 END) AS bvd_config_changes_count,

      MAX(is_sof_config_changed) AS is_sof_config_changed,
      SUM(CASE WHEN is_sof_config_changed THEN 1 ELSE 0 END) AS sof_config_changes_count,

      MAX(is_mov_config_changed) AS is_mov_config_changed,
      SUM(CASE WHEN is_mov_config_changed THEN 1 ELSE 0 END) AS mov_config_changes_count,

      MAX(is_surge_df_config_changed) AS is_surge_df_config_changed,
      SUM(CASE WHEN is_surge_df_config_changed THEN 1 ELSE 0 END) AS surge_df_config_changes_count,
    FROM `dh-logistics-product-ops.pricing.asa_flags_component_related_changes_details_asa_and_scheme_versioning_'''|| COALESCE(user, "john_doe") ||'''`
    GROUP BY 1,2;
  ''';

  -- Step 4: Combine `dh-logistics-product-ops.pricing.asa_flags_non_component_related_changes_asa_and_scheme_versioning` AND `dh-logistics-product-ops.pricing.asa_flags_component_related_changes_asa_and_scheme_versioning` together
  SET asa_flags_query = '''
    CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.asa_flags_asa_and_scheme_versioning_'''|| COALESCE(user, "john_doe") ||'''`
    OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)) AS
    SELECT *
    FROM `dh-logistics-product-ops.pricing.asa_flags_non_component_related_changes_asa_and_scheme_versioning_'''|| COALESCE(user, "john_doe") ||'''` a
    LEFT JOIN `dh-logistics-product-ops.pricing.asa_flags_component_related_changes_asa_and_scheme_versioning_'''|| COALESCE(user, "john_doe") ||'''` b USING(entity_id, asa_id);
  ''';

  -- Execute the queries
  EXECUTE IMMEDIATE asa_overview_query USING asa_ids AS asa_ids_bqr, entity_id_var AS entity_id_var_bqr, test_start_date AS test_start_date_bqr, test_end_date AS test_end_date_bqr;
  EXECUTE IMMEDIATE asa_change_log_query;
  EXECUTE IMMEDIATE asa_flags_non_component_related_changes_details_query;
  EXECUTE IMMEDIATE asa_flags_non_component_related_changes_query;
  EXECUTE IMMEDIATE asa_flags_component_related_changes_details_query;
  EXECUTE IMMEDIATE asa_flags_component_related_changes_query;
  EXECUTE IMMEDIATE asa_flags_query;
END;