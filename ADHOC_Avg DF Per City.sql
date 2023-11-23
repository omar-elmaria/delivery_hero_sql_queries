SELECT  
    o.global_entity_id,
    o.delivery_location.city_english,
    ROUND(AVG(o.value.delivery_fee_eur), 2) AS Avg_DF_eur,
    ROUND(AVG(o.value.delivery_fee_local), 2) AS Avg_DF_local,
FROM `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` o 
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.vendors` ven ON o.global_entity_id = ven.global_entity_id AND o.vendor_id = ven.vendor_id
WHERE DATE(o.placed_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
    AND o.is_sent -- Filter for successful orders
    AND o.is_commissionable -- Filter for orders that should be taken into account for billing, e.g. if commission can be charged to the restaurant for this order.
    AND o.is_own_delivery
    AND o.global_entity_id = 'FP_TW'
    AND ven.store_type_l2 = 'restaurants'
GROUP BY 1,2
ORDER BY 1,2