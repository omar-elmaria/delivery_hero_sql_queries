CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.loved_brands_apac_non_lbs_with_same_order_share_as_lbs_klang_valley` AS 

-- Order the "Loved Brands" from most loved to least loved
WITH lb_rank AS (
    SELECT 
        a.*,
        ROUND(SUM(a.Unique_visits_pct_rank) OVER (PARTITION BY a.entity_id, a.vendor_code) + SUM(a.Orders_pct_rank) OVER (PARTITION BY a.entity_id, a.vendor_code) + SUM(a.CVR3_pct_rank) OVER (PARTITION BY a.entity_id, a.vendor_code), 4) AS Filter_Sum,
        'Yes' AS LB_Flag
    FROM `dh-logistics-product-ops.pricing.loved_brands_apac_all_vendors_klang_valley` a
    INNER JOIN `dh-logistics-product-ops.pricing.loved_brands_apac_final_vendor_list_all_data_klang_valley` b ON a.entity_id = b.entity_id AND a.vendor_code = b.vendor_code -- LBs
),

non_lb_rank AS (
    SELECT 
        *,
        ROUND(SUM(Unique_visits_pct_rank) OVER (PARTITION BY entity_id, vendor_code) + SUM(Orders_pct_rank) OVER (PARTITION BY entity_id, vendor_code) + SUM(CVR3_pct_rank) OVER (PARTITION BY entity_id, vendor_code), 4) AS Filter_Sum,
        'No' AS LB_Flag
    FROM `dh-logistics-product-ops.pricing.loved_brands_apac_all_vendors_klang_valley`
    WHERE vendor_code NOT IN (SELECT vendor_code FROM `dh-logistics-product-ops.pricing.loved_brands_apac_final_vendor_list_all_data_klang_valley`) -- Non-LBs
),

union_all_tbl AS (
    SELECT *
    FROM lb_rank a

    UNION ALL

    SELECT *
    FROM non_lb_rank b
)

SELECT 
    *,
    ROUND(SUM(Num_orders) OVER (PARTITION BY LB_Flag ORDER BY Filter_Sum DESC) / b.Orders_city, 4) AS CumSum_Order_Share
FROM union_all_tbl a
LEFT JOIN (SELECT DISTINCT Orders_city FROM `dh-logistics-product-ops.pricing.loved_brands_apac_final_vendor_list_all_data_klang_valley`) b ON TRUE
ORDER BY LB_Flag DESC, Filter_Sum DESC
