--VARIABLE SETTING
DECLARE EntID, CtryCode, CityName, VertType, d_type STRING;
DECLARE StartDate, EndDate DATE;
DECLARE ZoneIDs ARRAY <INT64>;
SET (EntID, CtryCode, CityName, VertType, d_type) = ('FP_PH', "ph", "Manila", "restaurants", "OWN_DELIVERY");
SET (StartDate, EndDate) = (DATE('2021-08-17'), DATE('2021-08-31'));
SET ZoneIDs = [1];

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.loved_brands_apac_orders_data_for_threshold_adj` AS
WITH
--This code block extracts accurate delivery costs.
costs AS (
    SELECT
        p.entity_id,
        l.platform_order_code AS order_code,
        p.delivery_costs AS delivery_costs,
    FROM (
        SELECT
            entity_id,
            country_code,
            city_name,
            order_id,
            SUM(delivery_costs) delivery_costs
        FROM fulfillment-dwh-production.cl.utr_timings
        GROUP BY 1,2,3,4
    ) p
    LEFT JOIN fulfillment-dwh-production.cl.orders l on p.order_id = l.order_id AND p.country_code = l.country_code AND p.entity_id = l.entity.id
    WHERE TRUE 
        AND p.entity_id = EntID --HERE
        AND p.city_name = CityName --HERE
    GROUP BY 1,2,3
),

--This code block extracts data right down to zone level for a given country.
city_data AS (
    SELECT
        p.entity_id,
        co.country_code,
        ci.name AS city_name,
        ci.id AS city_id,
        zo.shape AS zone_shape,
        zo.id AS zone_id,
        zo.name AS zone_name,
        CONCAT(zo.id,'-',zo.name) AS zone
    FROM fulfillment-dwh-production.cl.countries co
    LEFT JOIN UNNEST(co.platforms) p
    LEFT JOIN UNNEST(co.cities) ci
    LEFT JOIN UNNEST(ci.zones) zo
    WHERE TRUE 
        AND co.country_code = CtryCode --HERE
        AND ci.name = CityName --HERE
        AND ci.is_active
        AND zo.is_active
),

--This code block provides vendor information in a given zone, AND is used in conjunction with the city_data code block.
vendors AS (
    SELECT 
        v.entity_id,
        cd.city_name,
        chain.id AS chain_id,
        chain.name AS chain_name,
        v.vendor_code,
        v.name AS vendor_name,
        v.vertical_type,
        cd.zone_id,
        cd.zone_name,
        dps.scheme_id,
        dps.assignment_type
    FROM fulfillment-dwh-production.cl.vendors_v2 v
    LEFT JOIN UNNEST(v.dps) dps
    CROSS JOIN UNNEST(delivery_provider) AS delivery_type -- "delivery_provider" is an array that sometimes contains multiple elements, so we need to unnest it and break it down to its individual components
    LEFT JOIN city_data cd ON v.entity_id = cd.entity_id AND ST_CONTAINS(cd.zone_shape, v.location)
    WHERE TRUE 
        AND v.entity_id = EntID --HERE
        AND v.is_active
        AND v.vertical_type = VertType
        AND delivery_type = d_type -- Filter for OD vendors -- STOPPED HERE
        --AND dps.assignment_type <> 'Country Fallback'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
    ORDER BY 1,2,3,5
),

--This code block takes all schemes within an entity/country/city AND ranks them by tier.
tiers AS (
    SELECT 
        *,
        RANK() OVER (PARTITION BY scheme_id, component_id ORDER BY threshold) AS tier
    FROM (
        SELECT DISTINCT 
            v.entity_id,
            d.scheme_id,
            d.scheme_name,
            pc.travel_time_config.id AS component_id,
            pc.travel_time_config.name AS component_name,
            CASE WHEN pc.travel_time_config.threshold IS NULL then 9999999 ELSE pc.travel_time_config.threshold END AS threshold,
            pc.travel_time_config.fee
        FROM fulfillment-dwh-production.cl.vendors_v2 v
        LEFT JOIN UNNEST(hurrier) hur
        LEFT JOIN UNNEST(zones) z
        LEFT JOIN UNNEST(dps) d
        LEFT JOIN UNNEST(d.vendor_config) vc
        LEFT JOIN UNNEST(vc.pricing_config) pc
        WHERE TRUE 
            AND v.entity_id = EntID --HERE
            AND hur.city.name = CityName --HERE
            AND d.is_active
        ORDER BY d.scheme_id
    )
    ORDER BY 1,2,4
),

--This code block aggregates metrics (DF, travel times) on an order level.
agg1 AS (
    SELECT 
        od.created_date,
        od.order_id AS platform_order_code_ga,
        od.vendor_id,
        v.vendor_name,
        v.chain_id,
        v.chain_name, 
        od.vertical_type,
        IFNULL(od.zone_name, v.zone_name) AS zone_name,
        IFNULL(od.zone_id, v.zone_id) AS zone_id,
        od.entity_id,
        od.scheme_id,
        od.vendor_price_scheme_type,
        AVG(od.delivery_fee_local - od.delivery_fee_vat_local + od.commission_local + od.joker_vendor_fee_local) AS revenue_local,
        AVG(od.delivery_fee_local - od.delivery_fee_vat_local + od.commission_local + od.joker_vendor_fee_local - c.delivery_costs) AS profit_local,
        AVG(CAST(co.value.delivery_fee_local AS FLOAT64)) AS cen_delivery_fee_local,
        ROUND(AVG(travel_time_distance_km), 4) AS travel_time_distance_km,
        AVG(travel_time) AS travel_time, -- The AVG is used to eliminate duplicates
    FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` od
    LEFT JOIN `fulfillment-dwh-production.cl.orders` ord ON od.entity_id = ord.entity.id AND od.order_id = ord.order_id -- A bridge table to join 1st and 3rd tables
    LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` co ON ord.entity.id = co.global_entity_id AND ord.platform_order_code = co.order_id
    LEFT JOIN costs c on od.entity_id = c.entity_id AND CAST(od.platform_order_code AS STRING) = c.order_code
    LEFT JOIN vendors v on od.entity_id = v.entity_id AND od.vendor_id = v.vendor_code
    WHERE 1=1
        AND LOWER(od.country_code) = CtryCode --HERE
        AND od.created_date BETWEEN StartDate AND EndDate --HERE
        AND od.is_own_delivery
        --AND CAST(placed_at_local AS DATE) BETWEEN DATE_ADD(StartDate, INTERVAL -2 DAY)  AND DATE_ADD(EndDate, INTERVAL 2 DAY) --HERE
        AND od.is_sent
        AND od.vendor_price_scheme_type <> 'Campaigns' --EXCLUDE CAMPAIGN ORDERS
        --AND IFNULL(od.zone_id, v.zone_id) IN UNNEST(ZoneIDs) --HERE
        AND od.vertical_type = VertType --HERE
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
)

--This final section joins orders to their respective tiers. Not done in the previous code block due to aggregation & multiple travel times.
SELECT 
    created_date,
    platform_order_code_ga,
    vendor_id,
    vendor_name,
    chain_id,
    chain_name,
    vertical_type,
    zone_name,
    zone_id,
    a.entity_id,
    a.scheme_id,
    vendor_price_scheme_type,
    cen_delivery_fee_local,
    revenue_local,
    profit_local,
    travel_time_distance_km,
    travel_time,
    SUM(CASE WHEN travel_time > t.threshold THEN 1 ELSE 0 END) + 1 AS tier
FROM agg1 a
INNER JOIN tiers t ON t.entity_id = a.entity_id AND t.scheme_id = a.scheme_id
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
ORDER BY 2;

-----------------------------------------------------------------END OF PART 1-----------------------------------------------------------------

WITH tiers AS (
    SELECT 
        *,
        RANK() OVER (PARTITION BY scheme_id, component_id ORDER BY threshold) AS tier
    FROM (
        SELECT DISTINCT 
            v.entity_id,
            d.scheme_id,
            d.scheme_name,
            pc.travel_time_config.id AS component_id,
            pc.travel_time_config.name AS component_name,
            CASE WHEN pc.travel_time_config.threshold IS NULL then 9999999 ELSE pc.travel_time_config.threshold END AS threshold,
            pc.travel_time_config.fee
        FROM fulfillment-dwh-production.cl.vendors_v2 v
        LEFT JOIN UNNEST(hurrier) hur
        LEFT JOIN UNNEST(zones) z
        LEFT JOIN UNNEST(dps) d
        LEFT JOIN UNNEST(d.vendor_config) vc
        LEFT JOIN UNNEST(vc.pricing_config) pc
        WHERE TRUE 
            AND v.entity_id = 'FP_PH' --HERE
            AND hur.city.name = 'Manila' --HERE
            AND d.is_active
        ORDER BY d.scheme_id
    )
    ORDER BY 1,2,4
),

current_scheme_stg AS (
    SELECT 
        med.tier,
        AVG(median_df_local) AS median_df_local,
        ROUND(SUM(cen_delivery_fee_local) / COUNT(DISTINCT platform_order_code_ga), 2) AS avg_df_local,
        t.fee AS theo_df,
        COUNT(DISTINCT platform_order_code_ga) AS order_count
    FROM (
        SELECT 
            entity_id,
            scheme_id,
            chain_name,
            tier,
            cen_delivery_fee_local,
            platform_order_code_ga,
            PERCENTILE_CONT(cen_delivery_fee_local, 0.5) OVER (PARTITION BY tier) AS median_df_local
        FROM `dh-logistics-product-ops.pricing.loved_brands_apac_orders_data_for_threshold_adj`
        WHERE TRUE
            --AND scheme_id = 785
            AND chain_name != 'Jollibee'
    ) med
    LEFT JOIN tiers t ON med.entity_id = t.entity_id AND med.scheme_id = t.scheme_id AND med.tier = t.tier
    GROUP BY 1, t.fee
    ORDER BY 1
),

current_scheme_stg_2 AS (
    SELECT
        *,
        ROUND(order_count / SUM(order_count) OVER (), 4) AS order_share
    FROM current_scheme_stg
    ORDER BY 1
),

current_scheme_stg_3 AS (
    SELECT
        *,
        ROUND(SUM(order_share) OVER (ORDER BY tier), 4) AS cumulative_order_share
    FROM current_scheme_stg_2
    ORDER BY 1
)

SELECT * FROM current_scheme_stg_3;



