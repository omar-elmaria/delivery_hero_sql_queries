WITH kennedy_walker AS (
    SELECT 
        zo.name AS zone_name_test,
        zo.id AS zone_id_test,
        zo.shape AS zone_shape_test, 
    FROM fulfillment-dwh-production.cl.countries co
    LEFT JOIN UNNEST(co.platforms) p
    LEFT JOIN UNNEST(co.cities) ci
    LEFT JOIN UNNEST(ci.zones) zo
    WHERE TRUE 
        AND entity_id IN ('FP_HK') -- HERE: Filter for the entity of interest
        AND zo.id IN (39) -- Test zones
        AND ci.is_active -- Filter for active cities
        AND zo.is_active -- Filter for active zones
),

kennedy_rider AS (
    SELECT 
        zo.name AS zone_name_test,
        zo.id AS zone_id_test,
        zo.shape AS zone_shape_test, 
    FROM fulfillment-dwh-production.cl.countries co
    LEFT JOIN UNNEST(co.platforms) p
    LEFT JOIN UNNEST(co.cities) ci
    LEFT JOIN UNNEST(ci.zones) zo
    WHERE TRUE 
        AND entity_id IN ('FP_HK') -- HERE: Filter for the entity of interest
        AND zo.id IN (8) -- Test zones
        AND ci.is_active -- Filter for active cities
        AND zo.is_active -- Filter for active zones
)

SELECT *
FROM kennedy_rider
UNION ALL 
SELECT *
FROM kennedy_walker
UNION ALL 
SELECT
    'Intersection' AS zone_name_test,
    10 AS zone_id_test,
    ST_INTERSECTION(r.zone_shape_test, w.zone_shape_test) AS intersection_zone, -- Find the polygon coordinates forming intersection between the test zone and each neighboring zone in HK
FROM kennedy_rider r
LEFT JOIN kennedy_walker w ON TRUE
UNION ALL
SELECT
    'non-overlap' AS zone_name_test,
    11 AS zone_id_test,
    ST_DIFFERENCE(r.zone_shape_test, w.zone_shape_test) AS non_overlapping_zone,  -- Find the polygon coordinates forming the non-intersecting area between the test zone and each neighboring zone in HK
FROM kennedy_rider r
LEFT JOIN kennedy_walker w ON TRUE