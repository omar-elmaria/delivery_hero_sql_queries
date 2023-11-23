WITH raw_orders AS (
    SELECT 
        a.variant,
        a.order_id,
        a.scheme_id,
        a.delivery_fee_local,
        a.dps_delivery_fee_local,
        a.dps_travel_time_fee_local,
        a.dps_surge_fee_local,
        COALESCE( -- Get the delivery fee data of Pandora countries from Pandata tables
            pd.delivery_fee_local, 
            IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.dps_delivery_fee_local)
        ) AS actual_df_paid_by_customer,
        pd.delivery_fee_local AS pd_delivery_fee_local,
        pd.delivery_fee_original_local,
        pd.delivery_fee_vat_local,
        is_delivery_fee_covered_by_discount,
        is_delivery_fee_covered_by_voucher
    FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` a
    INNER JOIN `dh-logistics-product-ops.pricing.loved_brands_apac_final_vendor_list_all_data_klang_valley` b ON a.entity_id = b.entity_id AND a.vendor_id = b.vendor_code -- LBs
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` pd ON a.entity_id = pd.global_entity_id AND a.platform_order_code = pd.code AND a.created_date = pd.created_date_utc -- Contains info on the orders in Pandora countries
    WHERE a.entity_id = 'FP_MY' AND a.city_name = 'Klang valley' AND a.delivery_status = 'completed' AND a.vertical_type = 'restaurants' AND a.is_own_delivery AND a.created_date >= DATE('2022-03-02')
    AND scheme_id IN (216, 404, 405, 406, 407) AND variant IN ('Control', 'Variation1', 'Variation2', 'Variation3', 'Variation4')
)

SELECT 
    variant, 
    scheme_id,
    COUNT(DISTINCT order_id) AS order_count,
    ROUND(SUM(delivery_fee_local) / COUNT(DISTINCT order_id), 2) AS avg_df,
    ROUND(SUM(dps_delivery_fee_local) / COUNT(DISTINCT order_id), 2) AS avg_dps_df,
    ROUND(SUM(dps_travel_time_fee_local) / COUNT(DISTINCT order_id), 2) AS avg_tt_fee,
    ROUND(SUM(dps_surge_fee_local) / COUNT(DISTINCT order_id), 2) AS avg_surge_fee,
    ROUND(SUM(actual_df_paid_by_customer) / COUNT(DISTINCT order_id), 2) AS avg_pd_df,
    ROUND(SUM(delivery_fee_original_local - actual_df_paid_by_customer) / COUNT(DISTINCT order_id), 2) AS voucher_amount,
    ROUND(SUM(delivery_fee_vat_local) / COUNT(DISTINCT order_id), 2) AS VAT
FROM raw_orders
GROUP BY 1,2
ORDER BY 1,3 DESC