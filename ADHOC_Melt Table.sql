#standardSQL

-- PANDAS MELT FUNCTION IN GOOGLE BIGQUERY

-- run this script with Google BigQuery Web UI in the Cloud Console

-- this piece of code functions like the pandas melt function
-- pandas.melt(id_vars, value_vars, var_name, value_name, col_level=None)
-- without utilizing user defined functions (UDFs)
-- see below for where to input corresponding arguments

DECLARE cmd STRING;
DECLARE subcmd STRING;
SET cmd = ("""
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
""");
SET subcmd = ("""
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
""")
;

-- -- -- EXAMPLE BELOW -- -- --

-- MELTING STARTS --
-- execute these two command to melt the table

-- the first generates the STRUCT commands
-- and saves a string in subcmd
EXECUTE IMMEDIATE FORMAT(
  -- please do not change this argument
  subcmd,
  -- query to retrieve the column names
  -- equivalent to value_vars in pandas.melt()
  -- the resulting table should have only one column
  -- with the name: column_name
  """
    SELECT column_name
    FROM `dh-logistics-product-ops.pricing.INFORMATION_SCHEMA.COLUMNS`
    WHERE (table_name = "penetration_stats_snapshot_entity_lvl") AND column_name NOT IN ("region", "entity_id")
  """
) INTO subcmd;

-- the second implements the melting
EXECUTE IMMEDIATE FORMAT(
  -- please do not change this argument
  cmd,
  -- query to retrieve the original table
  """
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
  """,
  -- please do not change this argument
  subcmd,
  -- equivalent to id_vars in pandas.melt()
  -- !!please type these twice!!
  "region, entity_id", "region, entity_id",
  -- equivalent to var_name in pandas.melt()
  "metric",
  -- equivalent to value_name in pandas.melt()
  "value"
);