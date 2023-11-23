### A query to evaluate the most suitable GADM level for each DH market
CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.gadm_data_evaluation_per_dh_market` AS
WITH dh_geo_data AS (
  SELECT
      co.region,
      p.entity_id,
      co.country_code,
      co.country_name,
      COUNT(DISTINCT zo.name) AS num_logistical_zones
  FROM `fulfillment-dwh-production.cl.countries` AS co
  LEFT JOIN UNNEST(co.platforms) AS p
  LEFT JOIN UNNEST(co.cities) AS ci
  LEFT JOIN UNNEST(ci.zones) AS zo
  INNER JOIN `fulfillment-dwh-production.curated_data_shared_coredata.global_entities` ent ON p.entity_id = ent.global_entity_id AND LOWER(ent.country_code) = co.country_code
  WHERE TRUE
    AND ent.is_reporting_enabled
    AND ent.is_entity_online
    AND ent.brand_name IN (
      "Foodpanda",
      "Baemin",
      "Talabat",
      "Yemeksepeti",
      "HungerStation",
      "eFood",
      "Foodora",
      "PedidosYa"
    )
    AND zo.is_active -- Active city
    AND ci.is_active -- Active zone
  GROUP BY 1, 2, 3, 4
),

gadm_data AS (
  SELECT
    a.gid_0 AS country_iso_a3,
    a.country AS country_name,
    COUNT(DISTINCT CONCAT(a.name_1, " | ", a.name_2)) AS count_distinct_city_area_lvl_2,
    COUNT(DISTINCT CONCAT(b.name_1, " | ", b.name_2, " | ", b.name_3)) AS count_distinct_city_area_lvl_3
  FROM `logistics-data-storage-staging.long_term_pricing.gadm_geo_spatial_data_level_2` a
  LEFT JOIN `logistics-data-storage-staging.long_term_pricing.gadm_geo_spatial_data_level_3` b USING(gid_0, country)
  WHERE a.gid_0 != "Z06" -- Eliminate this weird country ISO code belonging to PK
  GROUP BY 1, 2
  ORDER BY 2
)

SELECT
  *,
  CASE
    WHEN b.country_iso_a3 IS NOT NULL THEN
      CASE
        WHEN b.count_distinct_city_area_lvl_2 > a.num_logistical_zones * 1.1 THEN 'level_2' -- Check if the # of level 2 areas exceeds the # of logistics zones * 1.1 
        WHEN b.count_distinct_city_area_lvl_3 > a.num_logistical_zones * 1.1 THEN 'level_3' -- Check if the # of level 3 areas exceeds the # of logistics zones * 1.1
        ELSE 'gadm_data_not_suitable'
      END
    ELSE "gadm_data_not_available"
  END AS gadm_data_verdict
FROM dh_geo_data a
LEFT JOIN gadm_data b USING (country_name);

###-----------------------------------------###------------------------------------------------###

### A query to pull the WOF data for each market where the data is unsuitable or unavailable
CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.wof_data_per_dh_market` AS
SELECT
  geoid,
  geometry_type,
  geom AS geometry,
  last_modified_timestamp,
  body,
  REGEXP_REPLACE(JSON_EXTRACT(body, '$.properties.wof:country'), '"', "") AS wof_country_code,
  REGEXP_REPLACE(JSON_EXTRACT(body, '$.properties.wof:name'), '"', "") AS wof_name,
  REGEXP_REPLACE(JSON_EXTRACT(body, '$.properties.wof:placetype'), '"', "") AS wof_placetype_eng,
FROM `bigquery-public-data.geo_whos_on_first.geojson`
WHERE TRUE
  AND geometry_type IN ("Polygon", "MultiPolygon")
  AND REGEXP_REPLACE(JSON_EXTRACT(body, '$.properties.iso:country'), '"', "") IN (SELECT UPPER(country_code) FROM `logistics-data-storage-staging.long_term_pricing.gadm_data_evaluation_per_dh_market`)
ORDER BY REGEXP_REPLACE(JSON_EXTRACT(body, '$.properties.iso:country'), '"', "");

###-----------------------------------------###------------------------------------------------###

### A query to pull the right level of granularity (locality, neighbourhood, or county) from the WoF data
CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.wof_proper_granularity_per_dh_market` AS
WITH wof_area_count AS (
  SELECT
    wof_country_code,
    wof_placetype_eng,
    COUNT(wof_name) AS count_wof_areas
  FROM `logistics-data-storage-staging.long_term_pricing.wof_data_per_dh_market`
  GROUP BY 1,2
)

SELECT *
FROM wof_area_count
QUALIFY ROW_NUMBER() OVER (PARTITION BY wof_country_code ORDER BY count_wof_areas DESC) = 1;

###-----------------------------------------###------------------------------------------------###

### A query to determine the final data source verdict for each DH market
CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.customer_area_data_evaluation_per_dh_market` AS
WITH wof_data_verdict_tbl AS (
  SELECT
    a.*,
    b.count_wof_areas,
    CASE
      WHEN b.count_wof_areas > a.num_logistical_zones * 1.1 THEN 'wof_data_works' -- Check if the # of WoF areas exceeds the # of logistics zones * 1.1 
      ELSE 'wof_data_not_suitable'
    END AS wof_data_verdict,
  FROM `logistics-data-storage-staging.long_term_pricing.gadm_data_evaluation_per_dh_market` a
  LEFT JOIN `logistics-data-storage-staging.long_term_pricing.wof_proper_granularity_per_dh_market` b ON UPPER(a.country_code) = b.wof_country_code
)

SELECT
  *,
  CASE
    WHEN gadm_data_verdict NOT IN ("gadm_data_not_suitable", "gadm_data_not_available") THEN gadm_data_verdict
    WHEN gadm_data_verdict IN ("gadm_data_not_suitable", "gadm_data_not_available") AND wof_data_verdict = "wof_data_works" THEN "wof_data"
    WHEN gadm_data_verdict IN ("gadm_data_not_suitable", "gadm_data_not_available") AND wof_data_verdict != "wof_data_works" THEN "dh_proprietary_data"
  END AS combined_data_source_verdict
FROM wof_data_verdict_tbl;

###-----------------------------------------###------------------------------------------------###

### A query to collect admin area data from different sources into one table
CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.customer_location_admin_area_data` AS
SELECT
  b.country_code,
  country AS country_name,
  CONCAT(name_1, " | ", name_2) AS admin_area_name,
  LOWER(engtype_2) AS admin_area_type,
  ST_GEOGFROMTEXT(geometry) AS admin_area_geometry,
  "gadm_level_2" AS data_source
FROM `logistics-data-storage-staging.long_term_pricing.gadm_geo_spatial_data_level_2` a
LEFT JOIN `logistics-data-storage-staging.long_term_pricing.customer_area_data_evaluation_per_dh_market` b ON a.gid_0 = b.country_iso_a3
WHERE gid_0 IN (SELECT country_iso_a3 FROM `logistics-data-storage-staging.long_term_pricing.customer_area_data_evaluation_per_dh_market` WHERE combined_data_source_verdict = 'level_2')

UNION ALL

SELECT
  b.country_code,
  country AS country_name,
  CONCAT(name_1, " | ", name_2, " | ", name_3) AS admin_area_name,
  LOWER(engtype_3) AS admin_area_type,
  ST_GEOGFROMTEXT(geometry) AS admin_area_geometry,
  "gadm_level_3" AS data_source
FROM `logistics-data-storage-staging.long_term_pricing.gadm_geo_spatial_data_level_3` a
LEFT JOIN `logistics-data-storage-staging.long_term_pricing.customer_area_data_evaluation_per_dh_market` b ON a.gid_0 = b.country_iso_a3
WHERE gid_0 IN (SELECT country_iso_a3 FROM `logistics-data-storage-staging.long_term_pricing.customer_area_data_evaluation_per_dh_market` WHERE combined_data_source_verdict = 'level_3')

UNION ALL

SELECT
  wof_country_code,
  b.country_name AS country_name,
  wof_name AS admin_area_name,
  wof_placetype_eng AS admin_area_type,
  geometry AS admin_area_geometry,
  "wof" AS data_source
FROM `logistics-data-storage-staging.long_term_pricing.wof_data_per_dh_market` a
LEFT JOIN `logistics-data-storage-staging.long_term_pricing.customer_area_data_evaluation_per_dh_market` b ON LOWER(a.wof_country_code) = b.country_code
WHERE wof_country_code IN (SELECT country_code FROM `logistics-data-storage-staging.long_term_pricing.customer_area_data_evaluation_per_dh_market` WHERE combined_data_source_verdict = 'wof_data')