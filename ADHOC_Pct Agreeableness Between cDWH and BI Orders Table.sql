WITH entities AS (
    SELECT
        region,
        p.entity_id
    FROM `fulfillment-dwh-production.cl.entities`
    LEFT JOIN UNNEST(platforms) p
    WHERE (p.entity_id NOT LIKE 'ODR%') AND (p.entity_id NOT LIKE 'DN_%') AND (p.entity_id NOT IN ('FP_DE', 'FP_JP'))  -- Eliminate entities starting with ODR (on-demand riders) OR DN_ as they are not part of DPS
)

SELECT
    ent.region,
    o.entity_id,
    SUM(CASE WHEN o.delivery_fee_local = dwh.value.delivery_fee_local THEN 1 ELSE 0 END) / COUNT(DISTINCT o.order_id) AS pct_agreeableness_df,
    SUM(CASE WHEN o.customer_paid_local = dwh.value.customer_paid_local THEN 1 ELSE 0 END) / COUNT(DISTINCT o.order_id) AS pct_agreeableness_customer_paid,
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` o
INNER JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` dwh ON o.entity_id = dwh.global_entity_id AND o.platform_order_code = dwh.order_id
LEFT JOIN entities ent ON o.entity_id = ent.entity_id
WHERE TRUE
    -- AND o.entity_id LIKE 'FP%' -- Consider orders only in the entity of choice
    AND ent.region IN ('Americas', 'MENA')
    AND o.created_date BETWEEN DATE('2022-01-01') AND DATE('2022-01-31') -- Consider orders between certain start and end dates
    AND o.delivery_status = 'completed' -- Successful order
    AND o.is_own_delivery -- OD orders
    AND o.vertical_type = 'restaurants' -- Restaurant order
GROUP BY 1,2
ORDER BY 1,2