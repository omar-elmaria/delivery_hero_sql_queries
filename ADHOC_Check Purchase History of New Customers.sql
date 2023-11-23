CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.purchase_history_of_new_customers` AS
WITH customer AS (
    SELECT DISTINCT
        o.customer_account_id,
    FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` dps 
    LEFT JOIN `fulfillment-dwh-production.cl.orders` ord ON dps.entity_id = ord.entity.id AND dps.order_id = ord.order_id -- A bridge table to join 1st and 3rd tables
    LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` o ON ord.entity.id = o.global_entity_id AND ord.platform_order_code = o.order_id
    WHERE dps.entity_id = 'FP_MY' -- Malaysia
        AND dps.city_name = 'Perlis' -- Filter for the city of Perlis where the FDNC test in running
        AND dps.zone_id IN (362, 363, 610, 253, 254) -- Filter for the 5 zones where the FDNC test in running
        AND dps.is_own_delivery IS TRUE -- Focus on OD orders only
        --AND dps.perseus_client_id IS NOT NULL -- Remove records where perseus_client_id are missing because we need this field to determine the purchase history of individual customers 
        AND dps.created_date BETWEEN DATE('2021-09-17') AND DATE('2021-09-28') -- Set the timeline from the start of the fix until the end of the test
        AND dps.vertical_type = 'restaurants' -- Filter for the restaurants vertical as it's the only one included in the FDNC test
        AND dps.travel_time > 2 -- The standard price scheme in Perlis offers free delivery for travel times less than 2 minutes. We filter out orders which were delivered in under two minutes to focus on customer identification-related free deliveries
        AND is_acquisition IS NOT NULL -- Remove any records where is_acquisition is NULL because it's the main field that we are using here to identify new and old customers
        AND dps.variant IN ('Control', 'Variation1') -- Filter out other variant groups (e.g. original, variation2, etc)
        AND dps.dps_customer_tag = 'New'
)


SELECT 
    dps.created_date, -- Order date
    dps.operating_system,
    o.customer_account_id,
    dps.perseus_client_id,
    ROW_NUMBER() OVER (PARTITION BY o.customer_account_id ORDER BY dps.order_placed_at) AS order_rank, -- Ranks the orders that one customer has made by the date on which they were placed
    dps.order_placed_at, -- Order time stamp
    dps.order_id,
    dps.platform_order_code,
    o.is_acquisition,
    dps.dps_customer_tag,
    dps.variant,
    dps.delivery_fee_eur,
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` dps 
LEFT JOIN `fulfillment-dwh-production.cl.orders` ord ON dps.entity_id = ord.entity.id AND dps.order_id = ord.order_id -- A bridge table to join 1st and 3rd tables
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` o ON ord.entity.id = o.global_entity_id AND ord.platform_order_code = o.order_id
WHERE o.customer_account_id IN (SELECT customer_account_id FROM customer) AND dps.created_date >= DATE('2020-01-01') -- Set the timeline from way back in the past
ORDER BY 3,5