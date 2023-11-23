DECLARE start_date DATE;
DECLARE end_date DATE;
DECLARE country_code_list ARRAY <STRING>;
SET start_date = DATE("2023-07-30");
SET end_date = DATE("2023-08-27");
SET country_code_list = ["PH", "SG", "MY", "TH"];

CREATE OR REPLACE TABLE `logistics-data-storage-staging.temp_pricing.anakin_data_surge_exercise` AS
WITH geo_data AS (
    SELECT
      co.region,
      p.entity_id,
      co.country_code,
      ci.name AS city_name,
      ci.id AS city_id,
      ci.shape AS city_shape
    FROM `fulfillment-dwh-production.cl.countries` AS co
    LEFT JOIN UNNEST(co.platforms) AS p
    LEFT JOIN UNNEST(co.cities) AS ci
    WHERE ci.is_active -- Active zone
),

distinct_lat_longs AS (
    SELECT DISTINCT
      LOWER(country_code_iso) As country_code_iso,
      request_latitude,
      request_longitude,
      CONCAT(request_latitude, " | ", request_longitude) AS lat_long,
    FROM `fulfillment-dwh-production.pandata_curated.anakin_bq_vendor_hourly`
    WHERE TRUE
      AND country_code_iso IN UNNEST(country_code_list)
      AND created_date_utc BETWEEN start_date AND end_date
      AND delivery_fee_local IS NOT NULL -- Remove NULL DFs
),

city_names_with_request_lat_longs AS (
    SELECT
      an.*,
      cn.city_name
    FROM distinct_lat_longs an
    LEFT JOIN geo_data cn ON LOWER(an.country_code_iso) = LOWER(cn.country_code) AND ST_INTERSECTS(ST_GEOGPOINT(an.request_longitude, an.request_latitude), cn.city_shape)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country_code_iso, lat_long ORDER BY lat_long) = 1
),

vendor_hourly AS (
  SELECT DISTINCT
    -- Grouping variables
    LOWER(an.country_code_iso) AS country_code_iso,
    ci.city_name,
    an.source AS competitor,
    CONCAT(an.request_latitude, " | ", an.request_longitude) AS request_lat_long,
    an.created_date_utc,
    EXTRACT(HOUR FROM an.created_at_utc) AS created_hour_utc,
    an.name AS vendor_name,
    an.local_name AS vendor_local_name,
    CONCAT(an.vendor_latitude, " | ", an.vendor_longitude) AS vendor_lat_long,
    LOWER(an.vendor_type) AS parent_vertical_type,
    
    -- Metrics
    ROUND(AVG(an.distance_haversine_in_meters), 2) AS distance_haversine_m,
    ROUND(AVG(an.distance_manhattan_in_meters), 2) AS distance_manhattan_m,
    ROUND(AVG(an.delivery_fee_local), 2) AS delivery_fee_local,
    AVG(CAST(an.delivery_time_in_minutes AS FLOAT64)) AS delivery_time_min,
    AVG(an.minimum_order_price_local) AS mov_local
  FROM `fulfillment-dwh-production.pandata_curated.anakin_bq_vendor_hourly` an
  LEFT JOIN city_names_with_request_lat_longs ci ON LOWER(an.country_code_iso) = ci.country_code_iso AND CONCAT(an.request_latitude, " | ", an.request_longitude) = ci.lat_long
  WHERE TRUE
    AND created_date_utc BETWEEN start_date AND end_date
    AND delivery_fee_local IS NOT NULL -- Remove NULL DFs
    AND delivery_fee_source IN ("website_vendor_listings_page", "website_vendor_details_page") -- Remove DFs from the app_checkout_page so we don't get the Priority/Saver/Standard split
  GROUP BY 1,2,3,4,5,6,7,8,9,10
)

SELECT
  -- Grouping variables
    country_code_iso,
    city_name,
    competitor,
    request_lat_long,
    created_date_utc,
    created_hour_utc,
    
    -- Metrics
    ROUND(AVG(distance_haversine_m), 2) AS distance_haversine_m,
    ROUND(AVG(distance_manhattan_m), 2) AS distance_manhattan_m,
    ROUND(AVG(delivery_fee_local), 2) AS delivery_fee_local,
    AVG(CAST(delivery_time_min AS FLOAT64)) AS delivery_time_min,
    AVG(mov_local) AS mov_local
FROM vendor_hourly
GROUP BY 1,2,3,4,5,6
ORDER BY 1,2,3,4,5,6,7;