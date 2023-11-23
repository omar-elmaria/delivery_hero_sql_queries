WITH tbl1 AS (
    SELECT 
        entity_id,
        COUNT(*) AS FullRecords 
    FROM (
        SELECT
            a.*,
            b.perseus_id,
            b.adjust_id,
        FROM `datachef-staging.customer_identity_fraud.new_cust_identification_false_positive_analysis_no_data_loss` a
        LEFT JOIN `datachef-production.facts.orders` b 
        ON (CASE WHEN a.entity_id = 'Perlis' THEN 'FP_MY' ELSE entity_id END) = b.global_entity_id AND a.order_id = b.order_id
        WHERE a.created_date >= DATE('2021-01-01') AND customer_channel IN ('ios', 'android')
    )
    GROUP BY 1
),

tbl2 AS (
    SELECT 
        entity_id,
        COUNT(*) AS NULLRecords 
    FROM (
        SELECT
            a.*,
            b.perseus_id,
            b.adjust_id,
        FROM `datachef-staging.customer_identity_fraud.new_cust_identification_false_positive_analysis_no_data_loss` a
        LEFT JOIN `datachef-production.facts.orders` b ON (CASE WHEN a.entity_id = 'Perlis' THEN 'FP_MY' ELSE entity_id END) = b.global_entity_id AND a.order_id = b.order_id
        WHERE b.adjust_id IS NULL AND a.created_date >= DATE('2021-01-01') AND customer_channel IN ('ios', 'android')
    )
    GROUP BY 1
),

tbl3 AS (
    SELECT 
        entity_id,
        COUNT(*) AS NULLRecords 
    FROM (
        SELECT
            a.*,
            b.perseus_id,
            b.perseus_id,
        FROM `datachef-staging.customer_identity_fraud.new_cust_identification_false_positive_analysis_no_data_loss` a
        LEFT JOIN `datachef-production.facts.orders` b ON (CASE WHEN a.entity_id = 'Perlis' THEN 'FP_MY' ELSE entity_id END) = b.global_entity_id AND a.order_id = b.order_id
        WHERE b.perseus_id IS NULL AND a.created_date >= DATE('2021-01-01') AND customer_channel IN ('ios', 'android')
    )
    GROUP BY 1
)

SELECT 
    a.*, 
    b.NULLRecords AS NULLRecords_adjust,
    c.NULLRecords AS NULLRecords_perseus,
    b.NULLRecords/a.FULLRecords AS Share_of_NULLs_adjust,
    c.NULLRecords/a.FULLRecords AS Share_of_NULLs_perseus
FROM tbl1 a
LEFT JOIN tbl2 b USING(entity_id)
LEFT JOIN tbl3 c USING(entity_id)
ORDER BY 1