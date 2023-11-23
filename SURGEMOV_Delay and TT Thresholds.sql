DECLARE entity, vertical, d_type STRING;
DECLARE city_name_var, zone_name_var ARRAY <STRING>;
DECLARE start_date, end_date DATE;
-- DECLARE pricing_scheme ARRAY <INT64>;
SET (entity, vertical, d_type) = ('FP_HK', 'restaurants', 'OWN_DELIVERY');
SET city_name_var = ['Hong kong'];
-- SET zone_name_var = ['Central rider', 'Central walker', 'Tin shui wai rider', 'Tin shui wai walker']; -- Chosen zones
SET zone_name_var = ['Central rider', 'Central walker', 'Lai chi kok rider', 'Lai chi kok walker', 'Ma on shan rider', 'Ma on shan walker', 'Tin shui wai rider', 'Tin shui wai walker', 'Yuen long rider', 'Yuen long walker'];
SET (start_date, end_date) = (DATE('2021-12-20'), DATE('2022-02-20'));
-- SET pricing_scheme = [206, 1392];

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.surge_mov_hk_individual_orders` AS 
WITH city_data AS (
    SELECT
        p.entity_id,
        co.country_code,
        ci.name AS city_name,
        ci.id AS city_id,
        zo.id AS zone_id,
        zo.name AS zone_name,
        CONCAT(zo.id,'-',zo.name) AS zone_id_name_concat,
        zo.shape AS zone_shape,
    FROM fulfillment-dwh-production.cl.countries co
    LEFT JOIN UNNEST(co.platforms) p
    LEFT JOIN UNNEST(co.cities) ci
    LEFT JOIN UNNEST(ci.zones) zo
    WHERE TRUE 
        AND p.entity_id = entity --HERE
        AND ci.name IN UNNEST(city_name_var) --HERE
        AND zo.name IN UNNEST(zone_name_var) --HERE
        AND ci.is_active -- Active city
        AND zo.is_active -- Active zone
),

delivery_costs AS (
    SELECT
        p.entity_id,
        p.city_name,
        p.city_id,
        p.zone_name,
        p.zone_id,
        p.order_id, 
        o.platform_order_code,
        SUM(p.delivery_costs) AS delivery_costs_local,
        SUM(p.delivery_costs_eur) AS delivery_costs_eur
    FROM `fulfillment-dwh-production.cl.utr_timings` p
    LEFT JOIN `fulfillment-dwh-production.cl.orders` o ON p.entity_id = o.entity.id AND p.order_id = o.order_id -- Use the platform_order_code in this table as a bridge to join the order_id from utr_timings to order_id from central_dwh.orders 
    WHERE 1=1
        AND p.created_date BETWEEN start_date AND end_date -- For partitioning elimination and speeding up the query
        AND o.created_date BETWEEN start_date AND end_date -- For partitioning elimination and speeding up the query
    GROUP BY 1,2,3,4,5,6,7
),

orders AS ( -- Get the number of orders of **all** vendors in the chosen city over the specified timeframe. We don't need to specify the expedition type (OD vs. pickup) or the vertical (restaurants, darkstores, pharmacies, etc.) because we already have a list of vendors
    SELECT
        o.created_date,
        o.entity_id,
        o.city_name,
        o.city_id,
        o.zone_name,
        o.zone_id,
        CASE 
            WHEN o.zone_name = 'Central rider' OR o.zone_name = 'Central walker' THEN 'Central (rider & walker)'
            WHEN o.zone_name = 'Lai chi kok rider' OR o.zone_name = 'Lai chi kok walker' THEN 'Lai chi kok (rider & walker)'
            WHEN o.zone_name = 'Ma on shan rider' OR o.zone_name = 'Ma on shan walker' THEN 'Ma on shan (rider & walker)'
            WHEN o.zone_name = 'Tin shui wai rider' OR o.zone_name = 'Tin shui wai walker' THEN 'Tin shui wai (rider & walker)'
            WHEN o.zone_name = 'Yuen long rider' OR o.zone_name = 'Yuen long walker' THEN 'Yuen long wai (rider & walker)'
            ELSE o.zone_name
        END AS zone_name_mod,
        o.vendor_id,
        o.order_id,
        o.platform_order_code,
        o.scheme_id,
        o.dps_sessionid,
        o.ga_session_id,
        o.dps_customer_tag,
        CONCAT(o.scheme_id, ' | ', o.zone_name) AS scheme_zone_concat,
        o.is_delivery_fee_covered_by_discount,
        o.is_delivery_fee_covered_by_voucher,
        o.joker_customer_discount_local,
        basket_value_current_fee_value,
        o.dps_standard_fee_local,
        o.dps_surge_fee_local,
        o.dps_travel_time_fee_local AS dps_tt_fee_local,
        o.dps_delivery_fee_local,
        o.dps_minimum_order_value_local AS mov_local,
        o.gfv_local,
        o.to_customer_time AS p_d_time, -- The time difference between rider arrival at customer and the pickup time. This data point is only available for OD orders.
        o.delivery_distance AS p_d_dist_km_man, -- The Manhattan distance (km) between the vendor location coordinates and customer location coordinates. This distance doesn't take into account potential stacked deliveries, and it's not the travelled distance. This data point is only available for OD orders.
        ROUND(o.travel_time_distance_km, 4) AS p_d_dist_km_travelled, -- The distance (km) between the vendor location coordinates and customer location coordinates. This data point is only available for OD orders.
        o.actual_DT, -- The time it took to deliver the order. Measured from order creation until rider at customer. This data point is only available for OD orders.
        o.order_delay_mins, -- The time (min) difference between actual delivery time and promised delivery time. This data point is only available for OD orders.
        o.travel_time, -- The time (min) it takes rider to travel from vendor location coordinates to the customers. This data point is only available for OD orders.
        ROUND(o.estimated_courier_delay, 4) AS est_courier_delay, -- Estimated quantile delay either provided by platforms or Hurrier calling TES while the order is being created in minutes. This data point is only available for OD orders.
        ROUND(o.mean_delay, 4) AS mean_delay, -- Average lateness in minutes of an order placed at this time (Used by dashboard, das, dps). This data point is only available for OD orders.

        -- Current Delay and TT Thresholds
        CASE 
            WHEN travel_time <= 7.9 THEN '(0, 7.9]' -- travel_time in dps_sessions_mapped_to_orders_v2 is in minutes. Example: 1.416667 minutes is 1 minute and 25 seconds or 1.25, but it will be displayed as 1.416667 in dps_sessions_mapped_to_orders_v2
            WHEN travel_time > 7.9 AND travel_time <= 11.517 THEN '(7.9, 11.517]'
            WHEN travel_time > 11.517 THEN '(11.517, Inf]'
        END AS travel_time_interval_current,
        
        IF (o.zone_name LIKE '%Central%', 
            CASE 
                WHEN mean_delay <= 11.983 THEN '(0, 11.983]' -- Similar to travel_time, mean_delay is displayed in minutes in dps_sessions_mapped_to_orders_v2 
                WHEN mean_delay > 11.983 AND mean_delay <= 13.983 THEN '(11.983, 13.983]'
                WHEN mean_delay > 13.983 AND mean_delay <= 15.983 THEN '(13.983, 15.983]'
                WHEN mean_delay > 15.983 THEN '(15.983, Inf]'
            END,
            CASE -- All other zones except central. We only care about Tin shui wai
                WHEN mean_delay <= 9.983 THEN '(0, 9.983]' -- Similar to travel_time, mean_delay is displayed in minutes in dps_sessions_mapped_to_orders_v2 
                WHEN mean_delay > 9.983 AND mean_delay <= 12.983 THEN '(9.983, 12.983]'
                WHEN mean_delay > 12.983 AND mean_delay <= 13.983 THEN '(12.983, 13.983]'
                WHEN mean_delay > 13.983 THEN '(13.983, Inf]'
            END
        ) AS delay_interval_current,

        CASE 
            WHEN travel_time <= 7.9 THEN 1
            WHEN travel_time > 7.9 AND travel_time <= 11.517 THEN 2
            WHEN travel_time > 11.517 THEN 3
        END AS travel_time_sorter_current,

        IF (o.zone_name LIKE '%Central%', 
            CASE 
                WHEN mean_delay <= 11.983 THEN 1 -- Similar to travel_time, mean_delay is displayed in minutes in dps_sessions_mapped_to_orders_v2 
                WHEN mean_delay > 11.983 AND mean_delay <= 13.983 THEN 2
                WHEN mean_delay > 13.983 AND mean_delay <= 15.983 THEN 3
                WHEN mean_delay > 15.983 THEN 4
            END,
            CASE -- All other zones except central. We only care about Tin shui wai
                WHEN mean_delay <= 9.983 THEN 1 -- Similar to travel_time, mean_delay is displayed in minutes in dps_sessions_mapped_to_orders_v2 
                WHEN mean_delay > 9.983 AND mean_delay <= 12.983 THEN 2
                WHEN mean_delay > 12.983 AND mean_delay <= 13.983 THEN 3
                WHEN mean_delay > 13.983 THEN 4
            END
        ) AS delay_sorter_current,

        -- Proposed Delay and TT Thresholds (Omar)
        CASE 
            WHEN travel_time <= 4 THEN '(0, 4]'
            WHEN travel_time > 4 AND travel_time <= 6 THEN '(4, 6]' -- Went back 1.809 minutes before the starting tt tier (7.9) --> Equal to the difference between 7.9 and 9.709
            WHEN travel_time > 6 AND travel_time <= 8 THEN '(6, 8]' -- Split the 7.9-11.517 tier into two
            WHEN travel_time > 8 AND travel_time <= 10 THEN '(8, 10]' -- Split the 7.9-11.517 tier into two
            WHEN travel_time > 10 THEN '(10, Inf]'
        END AS travel_time_interval_proposed,

        CASE 
            WHEN mean_delay <= 8 THEN '(0, 8]' 
            WHEN mean_delay > 8 AND mean_delay <= 10 THEN '(8, 10]' 
            WHEN mean_delay > 10 AND mean_delay <= 12 THEN '(10, 12]' 
            WHEN mean_delay > 12 AND mean_delay <= 14 THEN '(12, 14]'
            WHEN mean_delay > 14 AND mean_delay <= 16 THEN '(14, 16]'
            WHEN mean_delay > 16 THEN '(16, Inf]'
        END AS delay_interval_proposed,

        CASE 
            WHEN travel_time <= 4 THEN 1
            WHEN travel_time > 4 AND travel_time <= 6 THEN 2 -- Went back 1.809 minutes before the starting tt tier (7.9) --> Equal to the difference between 7.9 and 9.709
            WHEN travel_time > 6 AND travel_time <= 8 THEN 3 -- Split the 7.9-11.517 tier into two
            WHEN travel_time > 8 AND travel_time <= 10 THEN 4 -- Split the 7.9-11.517 tier into two
            WHEN travel_time > 10 THEN 5
        END AS travel_time_sorter_proposed,

        CASE 
            WHEN mean_delay <= 8 THEN 1
            WHEN mean_delay > 8 AND mean_delay <= 10 THEN 2
            WHEN mean_delay > 10 AND mean_delay <= 12 THEN 3 
            WHEN mean_delay > 12 AND mean_delay <= 14 THEN 4
            WHEN mean_delay > 14 AND mean_delay <= 16 THEN 5
            WHEN mean_delay > 16 THEN 6
        END AS delay_sorter_proposed,

        -- Proposed Delay and TT Thresholds (Tim)
        CASE 
            WHEN travel_time <= 4 THEN '(0, 4]'
            WHEN travel_time > 4 AND travel_time <= 8 THEN '(4, 8]' -- Split the 7.9-11.517 tier into two
            WHEN travel_time > 8 AND travel_time <= 10 THEN '(8, 10]' -- Split the 7.9-11.517 tier into two
            WHEN travel_time > 10 THEN '(10, Inf]'
        END AS travel_time_interval_proposed_tim,

        CASE 
            WHEN mean_delay <= 8 THEN '(0, 8]' 
            WHEN mean_delay > 8 AND mean_delay <= 10 THEN '(8, 10]' 
            WHEN mean_delay > 10 AND mean_delay <= 12 THEN '(10, 12]' 
            WHEN mean_delay > 12 AND mean_delay <= 14 THEN '(12, 14]'
            WHEN mean_delay > 14 AND mean_delay <= 16 THEN '(14, 16]'
            WHEN mean_delay > 16 THEN '(16, Inf]'
        END AS delay_interval_proposed_tim,

        CASE 
            WHEN travel_time <= 4 THEN 1
            WHEN travel_time > 4 AND travel_time <= 8 THEN 2 -- Split the 7.9-11.517 tier into two
            WHEN travel_time > 8 AND travel_time <= 10 THEN 3 -- Split the 7.9-11.517 tier into two
            WHEN travel_time > 10 THEN 4
        END AS travel_time_sorter_proposed_tim,

        CASE 
            WHEN mean_delay <= 8 THEN 1
            WHEN mean_delay > 8 AND mean_delay <= 10 THEN 2
            WHEN mean_delay > 10 AND mean_delay <= 12 THEN 3 
            WHEN mean_delay > 12 AND mean_delay <= 14 THEN 4
            WHEN mean_delay > 14 AND mean_delay <= 16 THEN 5
            WHEN mean_delay > 16 THEN 6
        END AS delay_sorter_proposed_tim,

        o.commission_base_local, -- Basis for calculating commission to be paid by the vendor in local currency.
        o.commission_local, -- Contracted fee/commission charged by DH to the vendors per billable orders in local currency.
        ROUND(o.commission_local / NULLIF(o.commission_base_local, 0), 4) AS comm_rate,
        
        -- If an order had a basket value below MOV (i.e. small order fee was charged), add the small order fee calculated as MOV - GFV to the profit 
        COALESCE(
            pd.delivery_fee_local, 
            IF(o.is_delivery_fee_covered_by_discount = TRUE OR o.is_delivery_fee_covered_by_voucher = TRUE, 0, o.dps_delivery_fee_local)
        ) + o.commission_local + joker_vendor_fee_local + COALESCE(o.service_fee, 0) + COALESCE(dwh.value.mov_customer_fee_local, (o.dps_minimum_order_value_local - o.gfv_local)) AS revenue_local,

        COALESCE(
            pd.delivery_fee_local / o.exchange_rate, 
            IF(o.is_delivery_fee_covered_by_discount = TRUE OR o.is_delivery_fee_covered_by_voucher = TRUE, 0, o.dps_delivery_fee_eur)
        ) + o.commission_eur + joker_vendor_fee_eur + COALESCE(o.service_fee / o.exchange_rate, 0) + COALESCE(dwh.value.mov_customer_fee_local / o.exchange_rate, (o.dps_minimum_order_value_local - o.gfv_local) / o.exchange_rate) AS revenue_eur,

        COALESCE(
            pd.delivery_fee_local, 
            IF(o.is_delivery_fee_covered_by_discount = TRUE OR o.is_delivery_fee_covered_by_voucher = TRUE, 0, o.dps_delivery_fee_local)
        ) + o.commission_local + joker_vendor_fee_local + COALESCE(o.service_fee, 0) + COALESCE(dwh.value.mov_customer_fee_local) - cst.delivery_costs_local AS gross_profit_local,

        COALESCE(
            pd.delivery_fee_local / o.exchange_rate, 
            IF(o.is_delivery_fee_covered_by_discount = TRUE OR o.is_delivery_fee_covered_by_voucher = TRUE, 0, o.dps_delivery_fee_eur)
        ) + o.commission_eur + joker_vendor_fee_eur + COALESCE(o.service_fee / o.exchange_rate, 0) + COALESCE(dwh.value.mov_customer_fee_local / o.exchange_rate, (o.dps_minimum_order_value_local - o.gfv_local) / o.exchange_rate) - cst.delivery_costs_local AS gross_profit_eur,
        
        CAST(cx.nps_score AS INT64) AS nps_score,
        cx.nps_group,
        cx.nps_reason_main,
        cx.nps_reason 
    FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` o
    LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` dwh ON o.entity_id = dwh.global_entity_id AND o.platform_order_code = dwh.order_id
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` pd ON o.entity_id = pd.global_entity_id AND o.platform_order_code = pd.code AND o.created_date = pd.created_date_utc -- Contains info on the orders in Pandora countries
    LEFT JOIN `fulfillment-dwh-production.curated_data_shared_cx.nps_after_order_all_survey_responses` cx ON o.entity_id = cx.global_entity_id AND o.platform_order_code = cx.order_id
    LEFT JOIN delivery_costs cst ON o.entity_id = cst.entity_id AND o.order_id = cst.order_id -- The table that stores the CPO
    WHERE TRUE
        AND o.entity_id = entity -- Consider orders only in the entity of choice
        AND o.created_date BETWEEN start_date AND end_date -- Consider orders between certain start and end dates
        AND o.delivery_status = 'completed' -- Successful order
        -- AND o.is_commissionable -- Order qualifies for commission
        AND o.is_own_delivery -- OD orders
        AND o.vertical_type = vertical -- Restaurant order
        AND o.city_name IN UNNEST(city_name_var) -- Consider orders only in the city of choice
        AND o.zone_name IN UNNEST(zone_name_var) -- Consider orders only in the city of choice
        AND o.mean_delay IS NOT NULL -- Eliminate missing records where mean_delay is NULL
        AND o.travel_time IS NOT NULL -- Eliminate missing records where TT is NULL
        -- AND CAST(scheme_id AS INT64) IN UNNEST(pricing_scheme) -- Don't consider orders that were part of AB tests
)

-- SELECT * FROM orders ORDER BY entity_id, city_name, zone_name, created_date;

SELECT 
    *,
    CONCAT(travel_time_interval_current, ' | ', delay_interval_current) AS tt_delay_concat_current, 
    CONCAT(travel_time_interval_proposed, ' | ', delay_interval_proposed) AS tt_delay_concat_proposed
FROM orders
ORDER BY entity_id, city_name, zone_name, created_date;
