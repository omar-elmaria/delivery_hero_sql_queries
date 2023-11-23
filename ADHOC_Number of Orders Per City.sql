WITH city_data AS (
    SELECT 
        p.entity_id,
        country_code,
        ci.name AS city_name,
        ci.id AS city_id,
        zo.shape AS zone_shape, 
        zo.name AS zone_name,
        zo.id AS zone_id
    FROM fulfillment-dwh-production.cl.countries co
    LEFT JOIN UNNEST(co.platforms) p
    LEFT JOIN UNNEST(co.cities) ci
    LEFT JOIN UNNEST(ci.zones) zo
    WHERE TRUE 
        AND entity_id = 'FP_MY'
        --AND zo.id IN UNNEST(zn_id)
),

orders_city AS ( -- Orders of ALL OD restaurant vendors in the city
    SELECT
        o.global_entity_id,
        cd.city_name,
        COUNT(DISTINCT order_id) AS Orders_city
    FROM `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` o
    INNER JOIN city_data cd ON o.global_entity_id = cd.entity_id AND ST_CONTAINS(cd.zone_shape, ST_GEOGPOINT(o.delivery_location.longitude, o.delivery_location.latitude))
    WHERE TRUE
        AND o.global_entity_id = 'FP_MY'
        AND DATE(o.placed_at) BETWEEN DATE('2021-09-01') AND DATE('2021-10-27')
        AND o.is_sent -- Successful order
        AND o.is_own_delivery -- Own delivery vendors (no need to filter for restaurant vendors as we already filter for those in the vendors sub-table)
        AND o.is_qcommerce = FALSE -- restaurant orders  
    GROUP BY 1,2
    ORDER BY 3 DESC
)
SELECT * FROM orders_city