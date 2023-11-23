WITH delivery_costs AS (
    SELECT
        p.entity_id,
        p.order_id, 
        o.platform_order_code,
        SUM(p.delivery_costs) AS delivery_costs_local,
        SUM(p.delivery_costs_eur) AS delivery_costs_eur
    FROM `fulfillment-dwh-production.cl.utr_timings` p
    LEFT JOIN `fulfillment-dwh-production.cl.orders` o ON p.entity_id = o.entity.id AND p.order_id = o.order_id -- Use the platform_order_code in this table as a bridge to join the order_id from utr_timings to order_id from central_dwh.orders 
    WHERE 1=1
        AND p.created_date BETWEEN DATE('2021-01-01') AND DATE('2021-12-31') -- For partitioning elimination and speeding up the query
        AND o.created_date BETWEEN DATE('2021-01-01') AND DATE('2021-12-31') -- For partitioning elimination and speeding up the query
    GROUP BY 1,2,3
),

raw_orders AS (
    SELECT
    a.*,
        COALESCE(
        pd.delivery_fee_local, 
        IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.dps_delivery_fee_local)
    ) + a.commission_local + a.joker_vendor_fee_local + COALESCE(a.service_fee, 0) + COALESCE(dwh.value.mov_customer_fee_local, IF(a.gfv_local < a.dps_minimum_order_value_local, (a.dps_minimum_order_value_local - a.gfv_local), 0)) AS revenue_local,

    COALESCE(
        pd.delivery_fee_local, 
        IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.dps_delivery_fee_local)
    ) + a.commission_local + a.joker_vendor_fee_local + COALESCE(a.service_fee, 0) + COALESCE(dwh.value.mov_customer_fee_local, IF(a.gfv_local < a.dps_minimum_order_value_local, (a.dps_minimum_order_value_local - a.gfv_local), 0)) - cst.delivery_costs_local AS gross_profit_local

    FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` a
    LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` dwh ON a.entity_id = dwh.global_entity_id AND a.platform_order_code = dwh.order_id
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` pd ON a.entity_id = pd.global_entity_id AND a.platform_order_code = pd.code AND a.created_date = pd.created_date_utc -- Contains info on the orders in Pandora countries
    LEFT JOIN delivery_costs cst ON a.entity_id = cst.entity_id AND a.order_id = cst.order_id -- The table that stores the CPO
    WHERE TRUE
        AND a.entity_id = 'FP_MY'
        AND a.created_date BETWEEN DATE('2021-01-01') AND DATE('2021-12-31')
        AND a.is_own_delivery -- OD or MP
        AND a.vertical_type = 'restaurants' -- Orders from a particular vertical (restuarants, groceries, darkstores, etc.)
        AND a.delivery_status = 'completed'
        AND city_name = 'Klang valley' -- Successful orders 
)

SELECT
    DATE_TRUNC(a.created_date, MONTH),
    COUNT(DISTINCT a.order_id),
    SUM(gross_profit_local),
    SUM(gross_profit_local) / COUNT(DISTINCT order_id)
FROM raw_orders a
GROUP BY 1
ORDER BY 1