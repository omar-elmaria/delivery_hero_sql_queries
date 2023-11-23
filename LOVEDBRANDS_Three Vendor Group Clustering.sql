DECLARE entity_id_var STRING;
DECLARE asa_id_var INT64;
SET entity_id_var = "TB_AE";
SET asa_id_var = 255;

WITH lbs AS (
  SELECT
    *,
    "lbs" AS vendor_type
  FROM `dh-logistics-product-ops.pricing.final_vendor_list_all_data_loved_brands_scaled_code`
  WHERE TRUE
    AND entity_id = entity_id_var
    AND is_lb_lm = "Y"
    AND asa_id = asa_id_var
    AND update_timestamp = (SELECT MAX(update_timestamp) FROM `dh-logistics-product-ops.pricing.final_vendor_list_all_data_loved_brands_scaled_code`)
),

nlbs AS (
  SELECT
    *,
    "nlbs" AS vendor_type
  FROM `dh-logistics-product-ops.pricing.final_vendor_list_all_data_loved_brands_scaled_code`
  WHERE TRUE
    AND entity_id = entity_id_var
    AND is_lb_lm = "N"
    AND asa_id = asa_id_var
    AND vendor_rank = "Top 25%"
    AND update_timestamp = (SELECT MAX(update_timestamp) FROM `dh-logistics-product-ops.pricing.final_vendor_list_all_data_loved_brands_scaled_code`)
),

non_analyzed_vendors AS (
  SELECT
    *,
    "non_analyzed_vendors" AS vendor_type
  FROM `dh-logistics-product-ops.pricing.final_vendor_list_all_data_loved_brands_scaled_code`
  WHERE TRUE
    AND entity_id = entity_id_var
    AND is_lb_lm = "N"
    AND asa_id = asa_id_var
    AND vendor_rank = "Bottom 75%"
    AND update_timestamp = (SELECT MAX(update_timestamp) FROM `dh-logistics-product-ops.pricing.final_vendor_list_all_data_loved_brands_scaled_code`)
)

SELECT *
FROM lbs
UNION ALL
SELECT *
FROM nlbs
UNION ALL
SELECT *
FROM non_analyzed_vendors;
