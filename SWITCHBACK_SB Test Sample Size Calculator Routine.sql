-- V2 contains a filter for the scheme_id and a different way of defining the field "zone_cluster". Instead of defining zone_cluster from raw_orders, we define it from the declared variable zone_name_var
CREATE OR REPLACE PROCEDURE `logistics-data-storage-staging.long_term_pricing.switchback_testing_sample_size_calculation_in_days` (
  entity_var STRING, 
  country_code_var STRING,
  city_name_var STRING, -- One city choice is allowed and it has to be located in the entity specified above
  zone_name_var ARRAY <STRING>, -- Multiple zone choices are allowed, but they ALL have to be in the city specified above 
  vert_type STRING, -- Vertical type (e.g., restaurants, darkstores, health_and_wellness, groceries, electronics, etc.)
  scheme_id_var ARRAY <INT64>, -- Are there specific scheme IDs that you want to filter for in the zones you are interested in?
  /* 
  Number of "completed weeks" in the past that you want to look back at to calculate the historical fluctuations of the KPI of choice. Recommended values are 4, 6, 8, and 10
  We use completed weeks and not days so that we always consider full business cycles. One business cycle is considered one week
  */
  n_weeks INT64, 
  kpi STRING, -- Logistics metric whose volatility you want to examine. This should take one of the following choices ['delivery_time', 'dps_mean_delay', 'p_d_dist']
  /*
  Probability of type I error (i.e., false positive - incorrectly deeming a result significant AND rejecting the NULL hypothesis when it is indeed TRUE). 1 - "alpha" = Confidence level
  "alpha" can only take 3 values (0.1, 0.05, or 0.01)
  */
  alpha FLOAT64,
  /*
  Experiment's power. 1 - "beta" = Probability of type II error (i.e., false negative - failing to deem a result significant when it should be/failing to reject the NULL hypothesis when it is FALSE)
  "beta" can only take 3 values (0.8, 0.9, or 0.95)
  */
  beta FLOAT64,
  mde FLOAT64 -- Minimum detectable effect. This is the difference from the historical mean that you would like to detect
)

BEGIN
/*
You can run the routine right away without specifying any parameters. The defaults are given below:
    1. entity_var = 'FP_SG'
    2. country_code_var = 'sg'
    3. city_name_var = 'Singapore' --> One city choice is allowed and it has to be located in the entity specified above. Beware of incorrect spelling
    4. zone_name_var = ['Amk', 'Bedok'] --> 
        -- Multiple zone choices are allowed, but they ALL have to be in the city specified above
        -- Keep in mind that the final zone cluster string that will be shown after running the routine may have more zones than just these two. This is due to zone overlaps
    5. vert_type = 'restaurants' --> Vertical type (e.g., restaurants, darkstores, health_and_wellness, groceries, electronics, etc.)
    6. n_weeks = 4 --> Number of "completed weeks" in the past. Emphasis on **completed**. A week starts on a 'Sunday'
    7. kpi = 'dps_mean_delay' --> Logistics metric whose volatility you want to examine. This should take one of the following choices ['delivery_time', 'mean_delay', 'p_d_dist'] 
    8. alpha = 0.05
        -- Probability of type I error (i.e., false positive - incorrectly deeming a result significant AND rejecting the NULL hypothesis when it is indeed TRUE). 1 - "alpha" = Confidence level
        -- "alpha" can only take 3 values (0.1, 0.05, or 0.01)
    9. beta = 0.8
        -- Experiment's power. 1 - "beta" = Probability of type II error (i.e., false negative - failing to deem a result significant when it should be/failing to reject the NULL hypothesis when it is FALSE)
        -- "beta" can only take 3 values (0.8, 0.9, or 0.95)
    10. mde = 1.5 --> Minimum detectable effect. This is the difference from the historical mean that you would like to detect. The MDE depends on the KPI. Because we dafult to "dps_mean_delay", +/-1.5 minutes is a reasonable MDE 
*/

-- Declare some query parameters that will be used to calculate the sample size in the last step of the procedure
DECLARE z_score_alpha_one_sided FLOAT64;
DECLARE z_score_alpha_two_sided FLOAT64;
DECLARE z_score_beta FLOAT64;

SET z_score_alpha_one_sided = CASE WHEN alpha = 0.1 THEN 1.282 WHEN alpha = 0.05 THEN 1.644 WHEN alpha = 0.01 THEN 2.326 ELSE 1.644 END; -- Default is alpha = 0.05
SET z_score_alpha_two_sided = CASE WHEN alpha = 0.1 THEN 1.644 WHEN alpha = 0.05 THEN 1.96 WHEN alpha = 0.01 THEN 2.576 ELSE 1.96 END; -- Default is alpha = 0.05
SET z_score_beta = CASE WHEN beta = 0.8 THEN 0.842 WHEN beta = 0.9 THEN 1.282 WHEN beta = 0.95 THEN 1.645 ELSE 0.842 END; -- Default is beta = 0.8

##--------------------------------------------------------------END OF QUERY PARAMETER DECLARATION PART--------------------------------------------------------------##

WITH city_data AS ( -- Get the zones that will be clustered together to pull historical data
    SELECT 
      p.entity_id,
      cl.country_code,
      ci.id AS city_id,
      ci.name AS city_name,
      z.shape AS zone_shape,
      z.id AS zone_id,
      z.name AS zone_name
    FROM `fulfillment-dwh-production.cl.countries` cl
    LEFT JOIN UNNEST (platforms) p
    LEFT JOIN UNNEST (cities) ci
    LEFT JOIN UNNEST (zones) z
    WHERE TRUE
        AND p.entity_id = COALESCE(entity_var, 'FP_SG') -- Default value is FP_SG
        AND LOWER(cl.country_code) = COALESCE(country_code_var, 'sg') -- Default value is sg
        AND ci.name = COALESCE(city_name_var, 'Singapore') -- Default value is 'Singapore'
        AND z.name IN UNNEST(COALESCE(zone_name_var, ['Amk', 'Bedok'])) -- Default value is ['Amk', 'Bedok'] 
),

orders_raw_data AS ( -- Pull historical data on the "orders" level from dps_sessions_mapped_to_orders_v2
    SELECT
        dps.entity_id,
        dps.country_code,
        dps.city_name,
        dps.city_id,
        dps.zone_name,
        dps.zone_id,
        dps.created_date,
        DATE_DIFF(dps.created_date, DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY), WEEK), INTERVAL COALESCE(n_weeks, 4) WEEK), DAY) + 1 AS diff_in_days_from_start_date,
        CASE WHEN MOD(DATE_DIFF(dps.created_date, DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY), WEEK), INTERVAL COALESCE(n_weeks, 4) WEEK), DAY) + 1, 2) = 0 THEN 'even' ELSE 'odd' END AS even_or_odd_day,
        dps.platform_order_code,
        dps.dps_mean_delay, -- A.K.A Average fleet delay --> Average lateness in minutes of an order placed at this time (Used by dashboard, das, dps). This data point is only available for OD orders.
        dps.delivery_distance_m, -- A.K.A p_d distance
        dps.actual_DT -- A.K.A Actual delivery time
    FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` dps
    LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` dwh ON dps.entity_id = dwh.global_entity_id AND dps.platform_order_code = dwh.order_id
    INNER JOIN city_data cd ON TRUE
        AND dps.entity_id = cd.entity_id
        AND dps.country_code = cd.country_code
        AND ST_CONTAINS(cd.zone_shape, ST_GEOGPOINT(delivery_location.longitude,delivery_location.latitude)) -- Filter for delivery locations in the city of choice
    WHERE TRUE
        -- This date filter searches for days spanning the last "n_weeks" completed weeks
        AND dps.created_date BETWEEN DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY), WEEK), INTERVAL COALESCE(n_weeks, 4) WEEK) AND DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY), WEEK), INTERVAL 1 DAY)
        AND dps.delivery_status = 'completed' -- Successful order
        AND dps.vertical_type = COALESCE(vert_type, 'restaurants')
        AND ( -- If scheme_id_var IS NULL (i.e., the user does NOT specify any scheme values, THEN include ALL schemes. Otherwise, filter for the orders that were priced using the schemes specified by the user)
            CASE WHEN scheme_id_var IS NULL THEN TRUE 
            ELSE CAST(dps.scheme_id AS INT64) IN UNNEST(scheme_id_var) END
        )
),

log_kpis_daily_means AS ( -- Calculate the daily means of several KPIs for the zone cluster chosen
    SELECT
        entity_id,
        country_code,
        city_name,
        city_id,
        ARRAY_TO_STRING(COALESCE(zone_name_var, ['Amk', 'Bedok']), ', ') AS zone_cluster,
        created_date,
        even_or_odd_day,
        ROUND(AVG(dps_mean_delay), 2) AS daily_avg_dps_mean_delay,
        ROUND(AVG(delivery_distance_m), 2) AS daily_avg_p_d_dist,
        ROUND(AVG(actual_DT), 2) AS daily_avg_delivery_time
    FROM orders_raw_data
    GROUP BY 1,2,3,4,6,7
),

sample_size_calc_params AS ( -- Calculate the overall average and standard deviation of the historical distribution obtained above. They will be used as inputs to the sample size calculator
    SELECT
        entity_id,
        country_code,
        city_name,
        city_id,
        zone_cluster,
        ROUND(AVG(daily_avg_dps_mean_delay), 2) AS overall_avg_dps_mean_delay,
        ROUND(STDDEV_SAMP(daily_avg_dps_mean_delay), 2) AS overall_stdev_dps_mean_delay,

        ROUND(AVG(daily_avg_p_d_dist), 2) AS overall_avg_p_d_dist,
        ROUND(STDDEV_SAMP(daily_avg_p_d_dist), 2) AS overall_stdev_p_d_dist,

        ROUND(AVG(daily_avg_delivery_time), 2) AS overall_avg_delivery_time,
        ROUND(STDDEV_SAMP(daily_avg_delivery_time), 2) AS overall_stdev_delivery_time,
    FROM log_kpis_daily_means
    GROUP BY 1,2,3,4,5
),

sample_size_calc_params_recast AS ( -- Recast the table above such that the KPI becomes a separate column instead of being blended with the calculation type
    SELECT r.* 
    FROM (
        SELECT [
            STRUCT<entity_id STRING, country_code STRING, city_name STRING, city_id INT64, zone_cluster STRING, success_metric STRING, overall_avg FLOAT64, overall_stdev FLOAT64>
            (entity_id, country_code, city_name, city_id, zone_cluster, 'dps_mean_delay', AVG(overall_avg_dps_mean_delay), AVG(overall_stdev_dps_mean_delay)),

            (entity_id, country_code, city_name, city_id, zone_cluster, 'p_d_dist', AVG(overall_avg_p_d_dist), AVG(overall_stdev_p_d_dist)),

            (entity_id, country_code, city_name, city_id, zone_cluster, 'delivery_time', AVG(overall_avg_delivery_time), AVG(overall_stdev_delivery_time))
        ] AS entries
        FROM sample_size_calc_params
        GROUP BY entity_id, country_code, city_name, city_id, zone_cluster
    )
    LEFT JOIN UNNEST(entries) AS r
),

sample_size_calc_result AS ( -- Calculate the sample size using the formula for calculating the sample size required for a hypothesis test of the difference between two means
    SELECT
        entity_id,
        country_code,
        city_name,
        city_id,
        zone_cluster,
        success_metric,
        overall_avg,
        overall_stdev,
        -- Check a beautified version of the formula here (https://select-statistics.co.uk/calculators/sample-size-calculator-two-means/)
        -- This code should return a value equal to the one returned by this calculator (https://www.stat.ubc.ca/~rollin/stats/ssize/n2.html)
        CEILING((POW(overall_stdev, 2) * 2 * POW((z_score_alpha_two_sided + z_score_beta), 2)) / POW(COALESCE(mde, 1.5), 2)) AS sample_size_in_days_two_sided_test,
        CEILING((POW(overall_stdev, 2) * 2 * POW((z_score_alpha_one_sided + z_score_beta), 2)) / POW(COALESCE(mde, 1.5), 2)) AS sample_size_in_days_one_sided_test,
    FROM sample_size_calc_params_recast
    WHERE success_metric = COALESCE(kpi, 'dps_mean_delay')
)

SELECT * FROM sample_size_calc_result;
END
