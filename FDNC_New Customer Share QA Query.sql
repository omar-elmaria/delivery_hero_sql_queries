SELECT 
    dps.created_date, -- Order date
    COUNT(DISTINCT CASE WHEN dps.dps_customer_tag = 'New' THEN dps.order_id ELSE NULL END) AS orders_new_dps_tag, -- A flag that distinguishes between new and old users
    ROUND(COUNT(DISTINCT CASE WHEN dps.dps_customer_tag = 'New' THEN dps.order_id ELSE NULL END) / COUNT(DISTINCT dps.order_id), 3) AS orders_new_dps_tag_share_of_total, -- A flag that distinguishes between new and old users
    COUNT(DISTINCT CASE WHEN dps.dps_customer_tag = 'New' AND is_acquisition = FALSE THEN dps.order_id ELSE NULL END) AS false_positives,
    COUNT(DISTINCT CASE WHEN dps.dps_customer_tag = 'Existing' THEN dps.order_id ELSE NULL END) AS orders_old_dps_tag, -- A flag that distinguishes between new and old users
    COUNT(DISTINCT CASE WHEN dps.dps_customer_tag IS NULL THEN dps.order_id ELSE NULL END) AS orders_null_dps_tag, -- A flag that distinguishes between new and old users

    COUNT(DISTINCT CASE WHEN o.is_acquisition THEN dps.order_id ELSE NULL END) AS orders_new_is_acq, -- Another flag that distinguishes between new and old users. Can be used when dps_customer_tag is faulty
    ROUND(COUNT(DISTINCT CASE WHEN o.is_acquisition = TRUE THEN dps.order_id ELSE NULL END) / COUNT(DISTINCT dps.order_id), 3) AS orders_new_is_acq_share_of_total, -- A flag that distinguishes between new and old users
    COUNT(DISTINCT CASE WHEN o.is_acquisition = FALSE THEN dps.order_id ELSE NULL END) AS orders_old_is_acq, -- Another flag that distinguishes between new and old users. Can be used when dps_customer_tag is faulty
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` dps 
LEFT JOIN `fulfillment-dwh-production.cl.orders` ord ON dps.entity_id = ord.entity.id AND dps.order_id = ord.order_id -- A bridge table to join 1st and 3rd tables
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` o ON ord.entity.id = o.global_entity_id AND ord.platform_order_code = o.order_id
WHERE dps.entity_id = 'FP_MY' -- Malaysia
    AND dps.city_name = 'Perlis' -- Filter for the city of Perlis where the FDNC test in running
    AND dps.zone_id IN (362, 363, 610, 253, 254) -- Filter for the 5 zones where the FDNC test in running
    AND dps.is_own_delivery IS TRUE -- Focus on OD orders only
    AND dps.perseus_client_id IS NOT NULL -- Remove records where perseus_client_id are missing because we need this field to determine the purchase history of individual customers 
    AND dps.created_date BETWEEN DATE('2021-09-06') AND DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) -- Set the timeline from the start of the Perlis FDNC test
    AND dps.vertical_type = 'restaurants' -- Filter for the restaurants vertical as it's the only one included in the FDNC test
    AND dps.travel_time > 2 -- The standard price scheme in Perlis offers free delivery for travel times less than 2 minutes. We filter out orders which were delivered in under two minutes to focus on customer identification-related free deliveries
    AND is_acquisition IS NOT NULL -- Remove any records where is_acquisition is NULL because it's the main field that we are using here to identify new and old customers
    AND dps.variant IN ('Control', 'Variation1') -- Filter out other variant groups (e.g. original, variation2, etc)
GROUP BY 1
ORDER BY 1