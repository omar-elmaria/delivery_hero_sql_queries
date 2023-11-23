-- These declared variables will be used later in the script
DECLARE cmd_snapshot STRING;
DECLARE subcmd_snapshot STRING;
DECLARE cmd_time_series STRING;
DECLARE subcmd_time_series STRING;

###---------------------------------------------SEPARATOR---------------------------------------------###

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.penetration_stats_snapshot_entity_lvl` AS
WITH num_asas_and_order_count AS (
  SELECT
    region,
    entity_id,

    -- Number of ASAs
    COUNT(DISTINCT asa_common_name) AS tot_num_asas,
    COUNT(DISTINCT CASE WHEN asa_id = master_asa_id THEN asa_common_name END) AS num_non_clustered_asas, 
    COUNT(DISTINCT CASE WHEN asa_id != master_asa_id THEN asa_common_name END) AS num_clustered_asas, 
    
    -- Order count
    SUM(num_orders) AS orders_entity,
    SUM(CASE WHEN asa_id != master_asa_id THEN num_orders ELSE 0 END) AS orders_clustered_asas, -- Includes LBs and NLBs (1)
    SUM(CASE WHEN asa_id = master_asa_id THEN num_orders ELSE 0 END) AS orders_non_clustered_asas, -- Includes LBs and NLBs (2)
  
    SUM(CASE WHEN asa_id != master_asa_id AND is_lb_lm = "Y" THEN num_orders ELSE 0 END) AS orders_lbs_in_clustered_asas, -- The actual orders generated by LBs in ASAs on live setups (3)
    SUM(CASE WHEN asa_id != master_asa_id AND is_lb_lm = "N" THEN num_orders ELSE 0 END) AS orders_nlbs_in_clustered_asas, -- The actual orders generated by NLBs in ASAs on live setups (4)
  
    SUM(CASE WHEN asa_id = master_asa_id AND is_lb_lm = "Y" THEN num_orders ELSE 0 END) AS orders_lbs_in_non_clustered_asas, -- The orders generated by LBs in ASAs that are **not** on live setups (5)
    SUM(CASE WHEN asa_id = master_asa_id AND is_lb_lm = "N" THEN num_orders ELSE 0 END) AS orders_nlbs_in_non_clustered_asas, -- The orders generated by NLBs in ASAs that are **not** on live setups (6)
  
    SUM(CASE WHEN is_lb_lm = "Y" THEN num_orders ELSE 0 END) AS orders_lbs_in_all_asas, -- The potential we could reach (7). We should aim to bring (3) closer to (7)
    SUM(CASE WHEN is_lb_lm = "N" THEN num_orders ELSE 0 END) AS orders_nlbs_in_all_asas, -- (8)
    
    -- Vendor count
    COUNT(DISTINCT vendor_code) AS vendors_entity,
    COUNT(DISTINCT CASE WHEN asa_id != master_asa_id THEN vendor_code END) AS vendors_clustered_asas, -- Includes LBs and NLBs (9)
    COUNT(DISTINCT CASE WHEN asa_id = master_asa_id THEN vendor_code END) AS vendors_non_clustered_asas, -- Includes LBs and NLBs (10)
  
    COUNT(DISTINCT CASE WHEN asa_id != master_asa_id AND is_lb_lm = "Y" THEN vendor_code END) AS vendors_lbs_in_clustered_asas, -- The actual orders generated by LBs in ASAs on live setups (11)
    COUNT(DISTINCT CASE WHEN asa_id != master_asa_id AND is_lb_lm = "N" THEN vendor_code END) AS vendors_nlbs_in_clustered_asas, -- The actual orders generated by NLBs in ASAs on live setups (12)
  
    COUNT(DISTINCT CASE WHEN asa_id = master_asa_id AND is_lb_lm = "Y" THEN vendor_code END) AS vendors_lbs_in_non_clustered_asas, -- The orders generated by LBs in ASAs that are **not** on live setups (13)
    COUNT(DISTINCT CASE WHEN asa_id = master_asa_id AND is_lb_lm = "N" THEN vendor_code END) AS vendors_nlbs_in_non_clustered_asas, -- The orders generated by NLBs in ASAs that are **not** on live setups (14)
  
    COUNT(DISTINCT CASE WHEN is_lb_lm = "Y" THEN vendor_code END) AS vendors_lbs_in_all_asas, -- The potential we could reach (15). We should aim to bring (11) closer to (15)
    COUNT(DISTINCT CASE WHEN is_lb_lm = "N" THEN vendor_code END) AS vendors_nlbs_in_all_asas, -- (16)
  FROM `dh-logistics-product-ops.pricing.final_vendor_list_all_data_loved_brands_scaled_code`
  WHERE update_timestamp = (SELECT MAX(update_timestamp) FROM `dh-logistics-product-ops.pricing.final_vendor_list_all_data_loved_brands_scaled_code`)
  GROUP BY 1,2
)

SELECT
  *,
  -- Order Shares
  -- (1') and (2') sum up to 100%
  ROUND(orders_clustered_asas / NULLIF(orders_entity, 0), 4) AS clustered_asas_order_share_of_entity, -- Includes LBs and NLBs (1')
  ROUND(orders_non_clustered_asas / NULLIF(orders_entity, 0), 4) AS non_clustered_asas_order_share_of_entity, -- Includes LBs and NLBs (2')

  -- (3'), (4'), (17), and (18) sum up to 100%
  ROUND(orders_lbs_in_clustered_asas / NULLIF(orders_entity, 0), 4) AS live_lbs_order_share_of_entity, -- The order share of LBs as a percentage of the entity's orders (3')
  ROUND(orders_nlbs_in_clustered_asas / NULLIF(orders_entity, 0), 4) AS live_nlbs_order_share_of_entity, -- The order share of NLBs as a percentage of the entity's orders (4')
  ROUND(orders_lbs_in_non_clustered_asas / NULLIF(orders_entity, 0), 4) AS non_live_lbs_order_share_of_entity, -- The order share of LBs as a percentage of the entity's orders (17)
  ROUND(orders_nlbs_in_non_clustered_asas / NULLIF(orders_entity, 0), 4) AS non_live_nlbs_order_share_of_entity, -- The order share of NLBs as a percentage of the entity's orders (18)

  -- (19) and (20) sum up to 100%
  ROUND(orders_lbs_in_clustered_asas / NULLIF(orders_clustered_asas, 0), 4) AS lbs_order_share_of_clustered_asas, -- The order share of LBs within the ASA combo (parent and child) of ASAs on live setups (19)
  ROUND(orders_nlbs_in_clustered_asas / NULLIF(orders_clustered_asas, 0), 4) AS nlbs_order_share_of_clustered_asas, -- The order share of NLBs within the ASA combo (parent and child) of ASAs on live setups (20)
  
  -- (21) and (22) sum up to 100%
  ROUND(orders_lbs_in_non_clustered_asas / NULLIF(orders_non_clustered_asas, 0), 4) AS lbs_order_share_of_non_clustered_asas, -- The order share of LBs within the ASA combo (parent and child) of ASAs **not** on live setups (21)
  ROUND(orders_nlbs_in_non_clustered_asas / NULLIF(orders_non_clustered_asas, 0), 4) AS nlbs_order_share_of_non_clustered_asas, -- The order share of NLBs within the ASA combo (parent and child) of ASAs **not** on live setups (22)

  -- (23) and (24) sum up to 100%
  ROUND(orders_lbs_in_all_asas / NULLIF(orders_entity, 0), 4) AS potential_lbs_order_share_of_entity, -- (23). We should aim to bring (3') closer to (23)
  ROUND(orders_nlbs_in_all_asas / NULLIF(orders_entity, 0), 4) AS potential_nlbs_order_share_of_entity, -- (24)
  
  -- Vendor Shares
  -- (9') and (10') sum up to 100%
  ROUND(vendors_clustered_asas / NULLIF(vendors_entity, 0), 4) AS clustered_asas_vendor_share_of_entity, -- Includes LBs and NLBs (9')
  ROUND(vendors_non_clustered_asas / NULLIF(vendors_entity, 0), 4) AS non_clustered_asas_vendor_share_of_entity, -- Includes LBs and NLBs (10')

  -- (11'), (12'), (25), and (26) sum up to 100%
  ROUND(vendors_lbs_in_clustered_asas / NULLIF(vendors_entity, 0), 4) AS live_lbs_vendor_share_of_entity, -- The order share of LBs as a percentage of the entity's orders (11')
  ROUND(vendors_nlbs_in_clustered_asas / NULLIF(vendors_entity, 0), 4) AS live_nlbs_vendor_share_of_entity, -- The order share of NLBs as a percentage of the entity's orders (12')
  ROUND(vendors_lbs_in_non_clustered_asas / NULLIF(vendors_entity, 0), 4) AS non_live_lbs_vendor_share_of_entity, -- The order share of LBs as a percentage of the entity's orders (25)
  ROUND(vendors_nlbs_in_non_clustered_asas / NULLIF(vendors_entity, 0), 4) AS non_live_nlbs_vendor_share_of_entity, -- The order share of NLBs as a percentage of the entity's orders (26)

  -- (27) and (28) sum up to 100%
  ROUND(vendors_lbs_in_clustered_asas / NULLIF(vendors_clustered_asas, 0), 4) AS lbs_vendor_share_of_clustered_asas, -- The order share of LBs within the ASA combo (parent and child) of ASAs on live setups (27)
  ROUND(vendors_nlbs_in_clustered_asas / NULLIF(vendors_clustered_asas, 0), 4) AS nlbs_vendor_share_of_clustered_asas, -- The order share of NLBs within the ASA combo (parent and child) of ASAs on live setups (28)
  
  -- (29) and (30) sum up to 100%
  ROUND(vendors_lbs_in_non_clustered_asas / NULLIF(vendors_non_clustered_asas, 0), 4) AS lbs_vendor_share_of_non_clustered_asas, -- The order share of LBs within the ASA combo (parent and child) of ASAs **not** on live setups (29)
  ROUND(vendors_nlbs_in_non_clustered_asas / NULLIF(vendors_non_clustered_asas, 0), 4) AS nlbs_vendor_share_of_non_clustered_asas, -- The order share of NLBs within the ASA combo (parent and child) of ASAs **not** on live setups (30)

  -- (31) and (32) sum up to 100%
  ROUND(vendors_lbs_in_all_asas / NULLIF(vendors_entity, 0), 4) AS potential_lbs_vendor_share_of_entity, -- (31). We should aim to bring (11') closer to (31)
  ROUND(vendors_nlbs_in_all_asas / NULLIF(vendors_entity, 0), 4) AS potential_nlbs_vendor_share_of_entity, -- (32)
FROM num_asas_and_order_count
ORDER BY 1,2
;

###---------------------------------------------SEPARATOR---------------------------------------------###

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.penetration_stats_time_series_entity_lvl` AS
WITH num_asas_and_order_count_time_series AS (
  SELECT
    region,
    entity_id,
    DATE(update_timestamp) AS pipeline_run_date,

    -- Number of ASAs
    COUNT(DISTINCT asa_common_name) AS tot_num_asas,
    COUNT(DISTINCT CASE WHEN asa_id = master_asa_id THEN asa_common_name END) AS num_non_clustered_asas, 
    COUNT(DISTINCT CASE WHEN asa_id != master_asa_id THEN asa_common_name END) AS num_clustered_asas, 
    
    -- Order count
    SUM(num_orders) AS orders_entity,
    SUM(CASE WHEN asa_id != master_asa_id THEN num_orders ELSE 0 END) AS orders_clustered_asas, -- Includes LBs and NLBs (1)
    SUM(CASE WHEN asa_id = master_asa_id THEN num_orders ELSE 0 END) AS orders_non_clustered_asas, -- Includes LBs and NLBs (2)
  
    SUM(CASE WHEN asa_id != master_asa_id AND is_lb_lm = "Y" THEN num_orders ELSE 0 END) AS orders_lbs_in_clustered_asas, -- The actual orders generated by LBs in ASAs on live setups (3)
    SUM(CASE WHEN asa_id != master_asa_id AND is_lb_lm = "N" THEN num_orders ELSE 0 END) AS orders_nlbs_in_clustered_asas, -- The actual orders generated by NLBs in ASAs on live setups (4)
  
    SUM(CASE WHEN asa_id = master_asa_id AND is_lb_lm = "Y" THEN num_orders ELSE 0 END) AS orders_lbs_in_non_clustered_asas, -- The orders generated by LBs in ASAs that are **not** on live setups (5)
    SUM(CASE WHEN asa_id = master_asa_id AND is_lb_lm = "N" THEN num_orders ELSE 0 END) AS orders_nlbs_in_non_clustered_asas, -- The orders generated by NLBs in ASAs that are **not** on live setups (6)
  
    SUM(CASE WHEN is_lb_lm = "Y" THEN num_orders ELSE 0 END) AS orders_lbs_in_all_asas, -- The potential we could reach (7). We should aim to bring (3) closer to (7)
    SUM(CASE WHEN is_lb_lm = "N" THEN num_orders ELSE 0 END) AS orders_nlbs_in_all_asas, -- (8)
    
    -- Vendor count
    COUNT(DISTINCT vendor_code) AS vendors_entity,
    COUNT(DISTINCT CASE WHEN asa_id != master_asa_id THEN vendor_code END) AS vendors_clustered_asas, -- Includes LBs and NLBs (9)
    COUNT(DISTINCT CASE WHEN asa_id = master_asa_id THEN vendor_code END) AS vendors_non_clustered_asas, -- Includes LBs and NLBs (10)
  
    COUNT(DISTINCT CASE WHEN asa_id != master_asa_id AND is_lb_lm = "Y" THEN vendor_code END) AS vendors_lbs_in_clustered_asas, -- The actual orders generated by LBs in ASAs on live setups (11)
    COUNT(DISTINCT CASE WHEN asa_id != master_asa_id AND is_lb_lm = "N" THEN vendor_code END) AS vendors_nlbs_in_clustered_asas, -- The actual orders generated by NLBs in ASAs on live setups (12)
  
    COUNT(DISTINCT CASE WHEN asa_id = master_asa_id AND is_lb_lm = "Y" THEN vendor_code END) AS vendors_lbs_in_non_clustered_asas, -- The orders generated by LBs in ASAs that are **not** on live setups (13)
    COUNT(DISTINCT CASE WHEN asa_id = master_asa_id AND is_lb_lm = "N" THEN vendor_code END) AS vendors_nlbs_in_non_clustered_asas, -- The orders generated by NLBs in ASAs that are **not** on live setups (14)
  
    COUNT(DISTINCT CASE WHEN is_lb_lm = "Y" THEN vendor_code END) AS vendors_lbs_in_all_asas, -- The potential we could reach (15). We should aim to bring (11) closer to (15)
    COUNT(DISTINCT CASE WHEN is_lb_lm = "N" THEN vendor_code END) AS vendors_nlbs_in_all_asas, -- (16)
  FROM `dh-logistics-product-ops.pricing.final_vendor_list_all_data_loved_brands_scaled_code`
  GROUP BY 1,2,3
)

SELECT
  *,
  -- Order Shares
  -- (1') and (2') sum up to 100%
  ROUND(orders_clustered_asas / NULLIF(orders_entity, 0), 4) AS clustered_asas_order_share_of_entity, -- Includes LBs and NLBs (1')
  ROUND(orders_non_clustered_asas / NULLIF(orders_entity, 0), 4) AS non_clustered_asas_order_share_of_entity, -- Includes LBs and NLBs (2')

  -- (3'), (4'), (17), and (18) sum up to 100%
  ROUND(orders_lbs_in_clustered_asas / NULLIF(orders_entity, 0), 4) AS live_lbs_order_share_of_entity, -- The order share of LBs as a percentage of the entity's orders (3')
  ROUND(orders_nlbs_in_clustered_asas / NULLIF(orders_entity, 0), 4) AS live_nlbs_order_share_of_entity, -- The order share of NLBs as a percentage of the entity's orders (4')
  ROUND(orders_lbs_in_non_clustered_asas / NULLIF(orders_entity, 0), 4) AS non_live_lbs_order_share_of_entity, -- The order share of LBs as a percentage of the entity's orders (17)
  ROUND(orders_nlbs_in_non_clustered_asas / NULLIF(orders_entity, 0), 4) AS non_live_nlbs_order_share_of_entity, -- The order share of NLBs as a percentage of the entity's orders (18)

  -- (19) and (20) sum up to 100%
  ROUND(orders_lbs_in_clustered_asas / NULLIF(orders_clustered_asas, 0), 4) AS lbs_order_share_of_clustered_asas, -- The order share of LBs within the ASA combo (parent and child) of ASAs on live setups (19)
  ROUND(orders_nlbs_in_clustered_asas / NULLIF(orders_clustered_asas, 0), 4) AS nlbs_order_share_of_clustered_asas, -- The order share of NLBs within the ASA combo (parent and child) of ASAs on live setups (20)
  
  -- (21) and (22) sum up to 100%
  ROUND(orders_lbs_in_non_clustered_asas / NULLIF(orders_non_clustered_asas, 0), 4) AS lbs_order_share_of_non_clustered_asas, -- The order share of LBs within the ASA combo (parent and child) of ASAs **not** on live setups (21)
  ROUND(orders_nlbs_in_non_clustered_asas / NULLIF(orders_non_clustered_asas, 0), 4) AS nlbs_order_share_of_non_clustered_asas, -- The order share of NLBs within the ASA combo (parent and child) of ASAs **not** on live setups (22)

  -- (23) and (24) sum up to 100%
  ROUND(orders_lbs_in_all_asas / NULLIF(orders_entity, 0), 4) AS potential_lbs_order_share_of_entity, -- (23). We should aim to bring (3') closer to (23)
  ROUND(orders_nlbs_in_all_asas / NULLIF(orders_entity, 0), 4) AS potential_nlbs_order_share_of_entity, -- (24)
  
  -- Vendor Shares
  -- (9') and (10') sum up to 100%
  ROUND(vendors_clustered_asas / NULLIF(vendors_entity, 0), 4) AS clustered_asas_vendor_share_of_entity, -- Includes LBs and NLBs (9')
  ROUND(vendors_non_clustered_asas / NULLIF(vendors_entity, 0), 4) AS non_clustered_asas_vendor_share_of_entity, -- Includes LBs and NLBs (10')

  -- (11'), (12'), (25), and (26) sum up to 100%
  ROUND(vendors_lbs_in_clustered_asas / NULLIF(vendors_entity, 0), 4) AS live_lbs_vendor_share_of_entity, -- The order share of LBs as a percentage of the entity's orders (11')
  ROUND(vendors_nlbs_in_clustered_asas / NULLIF(vendors_entity, 0), 4) AS live_nlbs_vendor_share_of_entity, -- The order share of NLBs as a percentage of the entity's orders (12')
  ROUND(vendors_lbs_in_non_clustered_asas / NULLIF(vendors_entity, 0), 4) AS non_live_lbs_vendor_share_of_entity, -- The order share of LBs as a percentage of the entity's orders (25)
  ROUND(vendors_nlbs_in_non_clustered_asas / NULLIF(vendors_entity, 0), 4) AS non_live_nlbs_vendor_share_of_entity, -- The order share of NLBs as a percentage of the entity's orders (26)

  -- (27) and (28) sum up to 100%
  ROUND(vendors_lbs_in_clustered_asas / NULLIF(vendors_clustered_asas, 0), 4) AS lbs_vendor_share_of_clustered_asas, -- The order share of LBs within the ASA combo (parent and child) of ASAs on live setups (27)
  ROUND(vendors_nlbs_in_clustered_asas / NULLIF(vendors_clustered_asas, 0), 4) AS nlbs_vendor_share_of_clustered_asas, -- The order share of NLBs within the ASA combo (parent and child) of ASAs on live setups (28)
  
  -- (29) and (30) sum up to 100%
  ROUND(vendors_lbs_in_non_clustered_asas / NULLIF(vendors_non_clustered_asas, 0), 4) AS lbs_vendor_share_of_non_clustered_asas, -- The order share of LBs within the ASA combo (parent and child) of ASAs **not** on live setups (29)
  ROUND(vendors_nlbs_in_non_clustered_asas / NULLIF(vendors_non_clustered_asas, 0), 4) AS nlbs_vendor_share_of_non_clustered_asas, -- The order share of NLBs within the ASA combo (parent and child) of ASAs **not** on live setups (30)

  -- (31) and (32) sum up to 100%
  ROUND(vendors_lbs_in_all_asas / NULLIF(vendors_entity, 0), 4) AS potential_lbs_vendor_share_of_entity, -- (31). We should aim to bring (11') closer to (31)
  ROUND(vendors_nlbs_in_all_asas / NULLIF(vendors_entity, 0), 4) AS potential_nlbs_vendor_share_of_entity, -- (32)
FROM num_asas_and_order_count_time_series
ORDER BY 3,1,2
;

###---------------------------------------------SEPARATOR---------------------------------------------###

-- PANDAS MELT FUNCTION IN GOOGLE BIGQUERY

-- run this script with Google BigQuery Web UI in the Cloud Console

-- this piece of code functions like the pandas melt function
-- pandas.melt(id_vars, value_vars, var_name, value_name, col_level=None)
-- without utilizing user defined functions (UDFs)
-- see below for where to input corresponding arguments

SET cmd_snapshot = ('''
    CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.penetration_stats_snapshot_melted_entity_lvl` AS
    WITH original AS (
      -- query to retrieve the original table
      %s
    ),
    nested AS (
      SELECT
      [
        -- sub command to be automatically generated
        %s
      ] as s,
      -- equivalent to id_vars in pandas.melt()
      %s,
      FROM original
    )
    SELECT
      -- equivalent to id_vars in pandas.melt()
      %s,
      -- equivalent to var_name in pandas.melt()
      s.key AS %s,
      -- equivalent to value_name in pandas.melt()
      s.value AS %s,
    FROM nested
    CROSS JOIN UNNEST(nested.s) AS s
  ''');

  SET subcmd_snapshot = ('''
    WITH
    columns AS (
      -- query to retrieve the column names
      -- equivalent to value_vars in pandas.melt()
      -- the resulting table should have only one column
      -- with the name: column_name
      %s
    ),
    scs AS (
      SELECT FORMAT("STRUCT('%%s' as key, %%s as value)", column_name, column_name) AS sc
      FROM columns
    )
    SELECT ARRAY_TO_STRING(ARRAY (SELECT sc FROM scs), ",\\n")
''')
;

-- -- -- EXAMPLE BELOW -- -- --

-- MELTING STARTS --
-- execute these two command to melt the table

-- the first generates the STRUCT commands
-- and saves a string in subcmd_snapshot
EXECUTE IMMEDIATE FORMAT(
  -- please do not change this argument
  subcmd_snapshot,
  -- query to retrieve the column names
  -- equivalent to value_vars in pandas.melt()
  -- the resulting table should have only one column
  -- with the name: column_name
  '''
    SELECT column_name
    FROM `dh-logistics-product-ops.pricing.INFORMATION_SCHEMA.COLUMNS`
    WHERE (table_name = "penetration_stats_snapshot_entity_lvl") AND column_name NOT IN ("region", "entity_id")
  '''
) INTO subcmd_snapshot;

-- the second implements the melting
EXECUTE IMMEDIATE FORMAT(
  -- please do not change this argument
  cmd_snapshot,
  -- query to retrieve the original table
  '''
    SELECT
      region,
      entity_id,
      CAST(tot_num_asas AS FLOAT64) AS tot_num_asas,
      CAST(num_non_clustered_asas AS FLOAT64) AS num_non_clustered_asas,
      CAST(num_clustered_asas AS FLOAT64) AS num_clustered_asas,
      CAST(orders_entity AS FLOAT64) AS orders_entity,
      CAST(orders_clustered_asas AS FLOAT64) AS orders_clustered_asas,
      CAST(orders_non_clustered_asas AS FLOAT64) AS orders_non_clustered_asas,
      CAST(orders_lbs_in_clustered_asas AS FLOAT64) AS orders_lbs_in_clustered_asas,
      CAST(orders_nlbs_in_clustered_asas AS FLOAT64) AS orders_nlbs_in_clustered_asas,
      CAST(orders_lbs_in_non_clustered_asas AS FLOAT64) AS orders_lbs_in_non_clustered_asas,
      CAST(orders_nlbs_in_non_clustered_asas AS FLOAT64) AS orders_nlbs_in_non_clustered_asas,
      CAST(orders_lbs_in_all_asas AS FLOAT64) AS orders_lbs_in_all_asas,
      CAST(orders_nlbs_in_all_asas AS FLOAT64) AS orders_nlbs_in_all_asas,
      CAST(vendors_entity AS FLOAT64) AS vendors_entity,
      CAST(vendors_clustered_asas AS FLOAT64) AS vendors_clustered_asas,
      CAST(vendors_non_clustered_asas AS FLOAT64) AS vendors_non_clustered_asas,
      CAST(vendors_lbs_in_clustered_asas AS FLOAT64) AS vendors_lbs_in_clustered_asas,
      CAST(vendors_nlbs_in_clustered_asas AS FLOAT64) AS vendors_nlbs_in_clustered_asas,
      CAST(vendors_lbs_in_non_clustered_asas AS FLOAT64) AS vendors_lbs_in_non_clustered_asas,
      CAST(vendors_nlbs_in_non_clustered_asas AS FLOAT64) AS vendors_nlbs_in_non_clustered_asas,
      CAST(vendors_lbs_in_all_asas AS FLOAT64) AS vendors_lbs_in_all_asas,
      CAST(vendors_nlbs_in_all_asas AS FLOAT64) AS vendors_nlbs_in_all_asas,
      * EXCEPT (
        region,
        entity_id,
        tot_num_asas,
        num_non_clustered_asas,
        num_clustered_asas,
        orders_entity,
        orders_clustered_asas,
        orders_non_clustered_asas,
        orders_lbs_in_clustered_asas,
        orders_nlbs_in_clustered_asas,
        orders_lbs_in_non_clustered_asas,
        orders_nlbs_in_non_clustered_asas,
        orders_lbs_in_all_asas,
        orders_nlbs_in_all_asas,
        vendors_entity,
        vendors_clustered_asas,
        vendors_non_clustered_asas,
        vendors_lbs_in_clustered_asas,
        vendors_nlbs_in_clustered_asas,
        vendors_lbs_in_non_clustered_asas,
        vendors_nlbs_in_non_clustered_asas,
        vendors_lbs_in_all_asas,
        vendors_nlbs_in_all_asas
      )
    FROM `dh-logistics-product-ops.pricing.penetration_stats_snapshot_entity_lvl`
  ''',
  -- please do not change this argument
  subcmd_snapshot,
  -- equivalent to id_vars in pandas.melt()
  -- !!please type these twice!!
  "region, entity_id", "region, entity_id",
  -- equivalent to var_name in pandas.melt()
  "metric",
  -- equivalent to value_name in pandas.melt()
  "value"
)
;

###---------------------------------------------SEPARATOR---------------------------------------------###

-- PANDAS MELT FUNCTION IN GOOGLE BIGQUERY

-- run this script with Google BigQuery Web UI in the Cloud Console

-- this piece of code functions like the pandas melt function
-- pandas.melt(id_vars, value_vars, var_name, value_name, col_level=None)
-- without utilizing user defined functions (UDFs)
-- see below for where to input corresponding arguments

SET cmd_time_series = ('''
    CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.penetration_stats_time_series_melted_entity_lvl` AS
    WITH original AS (
        -- query to retrieve the original table
        %s
    ),
    nested AS (
        SELECT
        [
            -- sub command to be automatically generated
            %s
        ] as s,
        -- equivalent to id_vars in pandas.melt()
        %s,
        FROM original
    )
    SELECT
        -- equivalent to id_vars in pandas.melt()
        %s,
        -- equivalent to var_name in pandas.melt()
        s.key AS %s,
        -- equivalent to value_name in pandas.melt()
        s.value AS %s,
    FROM nested
    CROSS JOIN UNNEST(nested.s) AS s
    ''');

    SET subcmd_time_series = ('''
    WITH columns AS (
        -- query to retrieve the column names
        -- equivalent to value_vars in pandas.melt()
        -- the resulting table should have only one column
        -- with the name: column_name
        %s
    ),
    scs AS (
        SELECT FORMAT("STRUCT('%%s' as key, %%s as value)", column_name, column_name) AS sc
        FROM columns
    )
    SELECT ARRAY_TO_STRING(ARRAY (SELECT sc FROM scs), ",\\n")
''');

-- -- -- EXAMPLE BELOW -- -- --

-- MELTING STARTS --
-- execute these two command to melt the table

-- the first generates the STRUCT commands
-- and saves a string in subcmd_time_series
EXECUTE IMMEDIATE FORMAT(
  -- please do not change this argument
  subcmd_time_series,
  -- query to retrieve the column names
  -- equivalent to value_vars in pandas.melt()
  -- the resulting table should have only one column
  -- with the name: column_name
  '''
    SELECT column_name
    FROM `dh-logistics-product-ops.pricing.INFORMATION_SCHEMA.COLUMNS`
    WHERE (table_name = "penetration_stats_snapshot_entity_lvl") AND column_name NOT IN ("region", "entity_id", "pipeline_run_date")
  '''
) INTO subcmd_time_series;

-- the second implements the melting
EXECUTE IMMEDIATE FORMAT(
  -- please do not change this argument
  cmd_time_series,
  -- query to retrieve the original table
  '''
    SELECT
      region,
      entity_id,
      pipeline_run_date,
      CAST(tot_num_asas AS FLOAT64) AS tot_num_asas,
      CAST(num_non_clustered_asas AS FLOAT64) AS num_non_clustered_asas,
      CAST(num_clustered_asas AS FLOAT64) AS num_clustered_asas,
      CAST(orders_entity AS FLOAT64) AS orders_entity,
      CAST(orders_clustered_asas AS FLOAT64) AS orders_clustered_asas,
      CAST(orders_non_clustered_asas AS FLOAT64) AS orders_non_clustered_asas,
      CAST(orders_lbs_in_clustered_asas AS FLOAT64) AS orders_lbs_in_clustered_asas,
      CAST(orders_nlbs_in_clustered_asas AS FLOAT64) AS orders_nlbs_in_clustered_asas,
      CAST(orders_lbs_in_non_clustered_asas AS FLOAT64) AS orders_lbs_in_non_clustered_asas,
      CAST(orders_nlbs_in_non_clustered_asas AS FLOAT64) AS orders_nlbs_in_non_clustered_asas,
      CAST(orders_lbs_in_all_asas AS FLOAT64) AS orders_lbs_in_all_asas,
      CAST(orders_nlbs_in_all_asas AS FLOAT64) AS orders_nlbs_in_all_asas,
      CAST(vendors_entity AS FLOAT64) AS vendors_entity,
      CAST(vendors_clustered_asas AS FLOAT64) AS vendors_clustered_asas,
      CAST(vendors_non_clustered_asas AS FLOAT64) AS vendors_non_clustered_asas,
      CAST(vendors_lbs_in_clustered_asas AS FLOAT64) AS vendors_lbs_in_clustered_asas,
      CAST(vendors_nlbs_in_clustered_asas AS FLOAT64) AS vendors_nlbs_in_clustered_asas,
      CAST(vendors_lbs_in_non_clustered_asas AS FLOAT64) AS vendors_lbs_in_non_clustered_asas,
      CAST(vendors_nlbs_in_non_clustered_asas AS FLOAT64) AS vendors_nlbs_in_non_clustered_asas,
      CAST(vendors_lbs_in_all_asas AS FLOAT64) AS vendors_lbs_in_all_asas,
      CAST(vendors_nlbs_in_all_asas AS FLOAT64) AS vendors_nlbs_in_all_asas,
      * EXCEPT (
        region,
        entity_id,
        pipeline_run_date,
        tot_num_asas,
        num_non_clustered_asas,
        num_clustered_asas,
        orders_entity,
        orders_clustered_asas,
        orders_non_clustered_asas,
        orders_lbs_in_clustered_asas,
        orders_nlbs_in_clustered_asas,
        orders_lbs_in_non_clustered_asas,
        orders_nlbs_in_non_clustered_asas,
        orders_lbs_in_all_asas,
        orders_nlbs_in_all_asas,
        vendors_entity,
        vendors_clustered_asas,
        vendors_non_clustered_asas,
        vendors_lbs_in_clustered_asas,
        vendors_nlbs_in_clustered_asas,
        vendors_lbs_in_non_clustered_asas,
        vendors_nlbs_in_non_clustered_asas,
        vendors_lbs_in_all_asas,
        vendors_nlbs_in_all_asas
      )
    FROM `dh-logistics-product-ops.pricing.penetration_stats_time_series_entity_lvl`
  ''',
  -- please do not change this argument
  subcmd_time_series,
  -- equivalent to id_vars in pandas.melt()
  -- !!please type these twice!!
  "region, entity_id, pipeline_run_date", "region, entity_id, pipeline_run_date",
  -- equivalent to var_name in pandas.melt()
  "metric",
  -- equivalent to value_name in pandas.melt()
  "value"
)
;
