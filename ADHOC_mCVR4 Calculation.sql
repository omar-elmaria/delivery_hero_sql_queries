WITH countries AS (
  SELECT country_code
  , ci.id as city_id
  , ci.name as city_name
  , z.id as zone_id
  FROM `fulfillment-dwh-production.cl.countries`
  LEFT JOIN UNNEST (cities) ci
  LEFT JOIN UNNEST (zones) z
  WHERE ci.name IN ("General santos", "Tarlac")
), 

cvr_events AS (
  SELECT e.country_code
  , SPLIT(e.dps_zones_id, ',')[SAFE_OFFSET(0)] as zone_id -- I only need 1 zone in the country to know which city it is in. 
  , e.created_date
  , COUNT (DISTINCT e.ga_session_id) AS total_sessions
  , COUNT (DISTINCT e.shop_list_no) AS shop_list_sessions
  , COUNT (DISTINCT e.shop_menu_no) AS shop_menu_sessions
  , COUNT (DISTINCT e.checkout_no) AS checkout_sessions
  , COUNT (DISTINCT e.checkout_transaction) AS checkout_transaction_sessions
  , COUNT (DISTINCT e.transaction_no) AS transactions
  , COUNT (DISTINCT e.perseus_client_id) AS users
  FROM `fulfillment-dwh-production.cl.dps_cvr_events` e
  INNER JOIN `dh-logistics-product-ops.pricing.surge_mov_ph_individual_orders` ior ON e.entity_id = ior.entity_id AND e.ga_session_id = ior.ga_session_id
  WHERE e.created_date BETWEEN DATE('2021-09-01') AND DATE('2021-10-31') AND ior.mean_delay > 9 AND ior.travel_time > 6
  GROUP BY 1, 2, 3
)

SELECT 
  city_name
  , SUM (users) AS users
  , SUM (total_sessions) AS total_sessions
  , SUM (shop_list_sessions) AS shop_list_sessions
  , SUM (shop_menu_sessions) AS shop_menu_sessions
  , SUM (checkout_sessions) AS checkout_sessions
  , SUM (checkout_transaction_sessions) AS checkout_transaction_sessions
  , SUM (transactions) AS transactions
  , ROUND(SUM (transactions) / SUM (total_sessions), 3) AS CVR1
  , ROUND(SUM (transactions) / SUM (shop_menu_sessions ), 3) AS CVR3
  , ROUND(SUM (transactions) / SUM (checkout_sessions), 3) AS mCVR4
  , ROUND(SUM (checkout_transaction_sessions) / SUM (checkout_sessions), 3) AS mCVR4_prime
FROM cvr_events e
LEFT JOIN countries c ON e.country_code = c.country_code
AND CAST(c.zone_id AS STRING) = e.zone_id
GROUP BY 1
ORDER BY 1 DESC