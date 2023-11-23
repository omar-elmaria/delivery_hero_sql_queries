with int_step as (
Select 
    o.perseus_client_id
    , o.created_date
    , o.platform_order_code
    , o.dps_customer_tag
    , row_number() over (partition by o.perseus_client_id order by o.created_date) as order_rank
    , o.dps_delivery_fee_eur
    , o.delivery_fee_eur
from `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` o 
where o.country_code = "my"
 and o.is_own_delivery is TRUE
 and o.perseus_client_id is not null
  and o.created_date >= '2021-08-06'
 and o.city_name = 'Perlis'
 and o.zone_id IN (362, 363, 610, 253, 254)
 and o.vertical_type = 'restaurants'
 and not o.travel_time <= 2
Order by 1,4)

-- Select *
-- from int_step
-- where order_rank > 1 and dps_customer_tag = "New";



SELECT 
    dps.entity_id,
    dps.city_name,
    dps.created_date,
    -- variant,
    -- o.is_acquisition,
    COUNT(DISTINCT dps.order_id) AS total_orders,
    COUNT(DISTINCT CASE WHEN o.is_acquisition AND dps.delivery_fee_local = 0 THEN dps.order_id ELSE NULL END) AS acq_customers_FD, -- New customers in variation
    COUNT(DISTINCT CASE WHEN o.is_acquisition AND dps.delivery_fee_local > 0 THEN dps.order_id ELSE NULL END) AS acq_customers_Non_FD, -- New customers in control
    COUNT(DISTINCT CASE WHEN o.is_acquisition THEN dps.order_id ELSE NULL END) AS acq_customers,
    COUNT(DISTINCT CASE WHEN o.is_acquisition IS FALSE AND dps.delivery_fee_local = 0 THEN dps.order_id ELSE NULL END) AS old_and_paid_DF, -- New customers in variation
    COUNT(DISTINCT CASE WHEN o.is_acquisition IS FALSE AND dps.delivery_fee_local = 0 THEN dps.order_id ELSE NULL END) / 3100 AS old_and_paid_DF_share, -- New customers in variation
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` dps
LEFT JOIN `fulfillment-dwh-production.cl.orders` ord ON dps.entity_id = ord.entity.id AND dps.order_id = ord.order_id
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` o ON dps.entity_id = o.global_entity_id AND ord.platform_order_code = o.order_id
WHERE 1=1
    AND dps.created_date >= DATE("2021-08-06")
    AND dps.entity_id = 'FP_MY'
    AND dps.city_name = 'Perlis'
    AND dps.zone_id IN (362, 363, 610, 253, 254)
    AND dps.vertical_type = 'restaurants'
    AND dps.platform_order_code IN (SELECT platform_order_code FROM int_step where order_rank > 1 and dps_customer_tag = "New")
    AND dps.is_own_delivery
    AND dps.travel_time > 2
GROUP BY 1,2,3
ORDER BY 1,2,3