SELECT -- All target groups + non-TG (separately)
    e.entity_id,
    e.experiment_id,
    target_group,
    e.variant,
    COUNT(DISTINCT e.ga_session_id) AS total_sessions,
    COUNT(DISTINCT e.shop_list_no) AS shop_list_sessions,
    COUNT(DISTINCT e.shop_menu_no) AS shop_menu_sessions,
    COUNT(DISTINCT e.checkout_no) AS checkout_sessions,
    COUNT(DISTINCT e.transaction_no) AS transactions,
    COUNT(DISTINCT e.menu_checkout) AS menu_checkout,
    COUNT(DISTINCT e.checkout_transaction) AS checkout_transaction,
    COUNT(DISTINCT e.list_menu) AS list_menu,
    COUNT(DISTINCT e.perseus_client_id) AS users,
    COUNT(DISTINCT ven_id) AS vendor_count,
    COUNT(DISTINCT dps_zone) AS zone_count,
    ROUND(COUNT(DISTINCT e.transaction_no) / COUNT(DISTINCT e.shop_list_no), 4) AS CVR2,
    ROUND(COUNT(DISTINCT e.transaction_no) / COUNT(DISTINCT e.shop_menu_no), 4) AS CVR3,
    ROUND(COUNT(DISTINCT e.checkout_transaction) / COUNT(DISTINCT e.checkout_no), 4) AS mCVR4,
FROM `dh-logistics-product-ops.pricing.ab_test_cvr_data_cleaned_loved_brands_sg_shops` e
GROUP BY 1,2,3,4

UNION ALL

SELECT -- All target groups + non-TG combined
    e.entity_id,
    e.experiment_id,
    'TGx_Non_TG' AS target_group,
    e.variant,
    COUNT(DISTINCT e.ga_session_id) AS total_sessions,
    COUNT(DISTINCT e.shop_list_no) AS shop_list_sessions,
    COUNT(DISTINCT e.shop_menu_no) AS shop_menu_sessions,
    COUNT(DISTINCT e.checkout_no) AS checkout_sessions,
    COUNT(DISTINCT e.transaction_no) AS transactions,
    COUNT(DISTINCT e.menu_checkout) AS menu_checkout,
    COUNT(DISTINCT e.checkout_transaction) AS checkout_transaction,
    COUNT(DISTINCT e.list_menu) AS list_menu,
    COUNT(DISTINCT e.perseus_client_id) AS users,
    COUNT(DISTINCT ven_id) AS vendor_count,
    COUNT(DISTINCT dps_zone) AS zone_count,
    ROUND(COUNT(DISTINCT e.transaction_no) / COUNT(DISTINCT e.shop_list_no), 4) AS CVR2,
    ROUND(COUNT(DISTINCT e.transaction_no) / COUNT(DISTINCT e.shop_menu_no), 4) AS CVR3,
    ROUND(COUNT(DISTINCT e.checkout_transaction) / COUNT(DISTINCT e.checkout_no), 4) AS mCVR4,
FROM `dh-logistics-product-ops.pricing.ab_test_cvr_data_cleaned_loved_brands_sg_shops` e
WHERE target_group IN ('TG1', 'TG2', 'TG3', 'TG4', 'Non_TG')
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;

----------------------------------------------------------------END OF BUSINESS KPIs PART----------------------------------------------------------------

-- Step 5: Pull CVR data from dps_cvr_events (PER DAY)
SELECT -- All target groups + non-TG (separately)
    e.created_date,
    e.entity_id,
    e.experiment_id,
    target_group,
    e.variant,
    COUNT(DISTINCT e.ga_session_id) AS total_sessions,
    COUNT(DISTINCT e.shop_list_no) AS shop_list_sessions,
    COUNT(DISTINCT e.shop_menu_no) AS shop_menu_sessions,
    COUNT(DISTINCT e.checkout_no) AS checkout_sessions,
    COUNT(DISTINCT e.transaction_no) AS transactions,
    COUNT(DISTINCT e.menu_checkout) AS menu_checkout,
    COUNT(DISTINCT e.checkout_transaction) AS checkout_transaction,
    COUNT(DISTINCT e.list_menu) AS list_menu,
    COUNT(DISTINCT e.perseus_client_id) AS users,
    COUNT(DISTINCT ven_id) AS vendor_count,
    COUNT(DISTINCT dps_zone) AS zone_count,
    ROUND(COUNT(DISTINCT e.transaction_no) / NULLIF(COUNT(DISTINCT e.shop_list_no), 0), 4) AS CVR2,
    ROUND(COUNT(DISTINCT e.transaction_no) / NULLIF(COUNT(DISTINCT e.shop_menu_no), 0), 4) AS CVR3,
    ROUND(COUNT(DISTINCT e.checkout_transaction) / NULLIF(COUNT(DISTINCT e.checkout_no), 0), 4) AS mCVR4,
FROM `dh-logistics-product-ops.pricing.ab_test_cvr_data_cleaned_loved_brands_sg_shops` e
GROUP BY 1,2,3,4,5

UNION ALL

SELECT -- All target groups + non-TG (combined)
    e.created_date,
    e.entity_id,
    e.experiment_id,
    'TGx_Non_TG' AS target_group,
    e.variant,
    COUNT(DISTINCT e.ga_session_id) AS total_sessions,
    COUNT(DISTINCT e.shop_list_no) AS shop_list_sessions,
    COUNT(DISTINCT e.shop_menu_no) AS shop_menu_sessions,
    COUNT(DISTINCT e.checkout_no) AS checkout_sessions,
    COUNT(DISTINCT e.transaction_no) AS transactions,
    COUNT(DISTINCT e.menu_checkout) AS menu_checkout,
    COUNT(DISTINCT e.checkout_transaction) AS checkout_transaction,
    COUNT(DISTINCT e.list_menu) AS list_menu,
    COUNT(DISTINCT e.perseus_client_id) AS users,
    COUNT(DISTINCT ven_id) AS vendor_count,
    COUNT(DISTINCT dps_zone) AS zone_count,
    ROUND(COUNT(DISTINCT e.transaction_no) / NULLIF(COUNT(DISTINCT e.shop_list_no), 0), 4) AS CVR2,
    ROUND(COUNT(DISTINCT e.transaction_no) / NULLIF(COUNT(DISTINCT e.shop_menu_no), 0), 4) AS CVR3,
    ROUND(COUNT(DISTINCT e.checkout_transaction) / NULLIF(COUNT(DISTINCT e.checkout_no), 0), 4) AS mCVR4,
FROM `dh-logistics-product-ops.pricing.ab_test_cvr_data_cleaned_loved_brands_sg_shops` e
WHERE target_group IN ('TG1', 'TG2', 'TG3', 'TG4', 'Non_TG')
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5;