WITH perseus_id_tbl AS ( -- Extract the orders that are labelled as FN
    SELECT 
        * EXCEPT(perseus_id),
        CAST(perseus_id AS STRING) AS perseus_id
    FROM `dh-logistics-product-ops.pricing.new_cust_identification_false_positive_analysis_no_data_loss_with_adjust_and_perseus`
    WHERE created_date >= DATE('2021-09-29') AND entity_id IN ('FP_SG', 'FP_MY') AND dps_customer_tag IS NULL AND manual_order_rank_aas_grouped = '1 - New'
),

ci_logs AS (
    SELECT
        global_entity_id,
        DATE(timestamp) AS Datum,
        CASE WHEN SUBSTR(message, 1, 15) = 'no device found' THEN 'no device found' ELSE message END AS message_mod,
        CASE WHEN SUBSTR(message, 1, 15) = 'no device found' THEN ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(message, r'id (.*?) and '), '') ELSE NULL END AS perseus_from_logs,
        message
    FROM `datachef-production.customer_identity_fraud.ci_service_logs_backfill` logs
    WHERE DATE(timestamp) >= "2021-09-29" AND global_entity_id IN ('FP_SG', 'FP_MY')
)

SELECT 
    a.global_entity_id,
    a.Datum,
    a.message_mod,
    b.total_FN_orders,
    b.order_count_with_NULL_perseus_ids,
    b.order_count_with_perseus_ids,
    COUNT(DISTINCT a.perseus_from_logs) AS Count_Orders_Device_Not_Found,
    ROUND(COUNT(DISTINCT a.perseus_from_logs) / b.order_count_with_perseus_ids, 3) AS Share_Orders_Device_Not_Found_Out_Of_Orders_With_Non_NULL_Perseus,
    ROUND(COUNT(DISTINCT a.perseus_from_logs) / b.total_FN_orders , 3) AS Share_Orders_Device_Not_Found_Out_Of_ALL_Orders
FROM ci_logs a
INNER JOIN perseus_id_tbl ord ON a.global_entity_id = ord.entity_id AND a.Datum = ord.created_date AND a.perseus_from_logs = ord.perseus_id
LEFT JOIN (
        SELECT 
            created_date,
            entity_id,
            COUNT(*) AS total_FN_orders, 
            SUM(CASE WHEN perseus_id IS NULL THEN 1 ELSE 0 END) AS order_count_with_NULL_perseus_ids,
            SUM(CASE WHEN perseus_id IS NOT NULL THEN 1 ELSE 0 END) AS order_count_with_perseus_ids 
        FROM perseus_id_tbl
        GROUP BY 1,2
) b ON a.global_entity_id = b.entity_id AND a.Datum = b.created_date
GROUP BY 1,2,3,4,5,6
ORDER BY 1,2,3,4,5,6