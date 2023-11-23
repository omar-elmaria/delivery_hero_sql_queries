SELECT 
    SUM(Num_orders) AS Total_Orders,
    COUNT(DISTINCT vendor_code) AS Total_Vendors,
    SUM(Num_orders) / AVG(b.Orders_city) AS OrderShare,
FROM `dh-logistics-product-ops.pricing.loved_brands_apac_all_vendors_sabah` a
LEFT JOIN (SELECT DISTINCT Orders_city FROM `dh-logistics-product-ops.pricing.loved_brands_apac_final_vendor_list_all_data_sabah`) b ON TRUE
WHERE TRUE 
    AND	Unique_visits_pct_rank >= 0.75
    AND Orders_pct_rank >= 0.75	
    AND CVR3_pct_rank >= 0.75	


SELECT 
    SUM(Num_orders) AS Total_Orders,
    COUNT(DISTINCT vendor_code) AS Total_Vendors,
    SUM(Num_orders) / AVG(Orders_city) AS OrderShare,
FROM `dh-logistics-product-ops.pricing.loved_brands_apac_final_vendor_list_all_data_sabah`