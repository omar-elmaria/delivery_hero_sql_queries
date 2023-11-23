SELECT
    response_id, 
    global_entity_id, 
    order_id, 
    survey_id, 
    start_at_utc, 
    end_at_utc, 
    recorded_at_utc, 
    created_date, 
    survey_type, 
    segment_loyalty, 
    progress, 
    is_pickup, 
    is_failed, 
    finished, 
    nps_score, 
    nps_group, 
    nps_reason, 
    nps_reason_main, 
    browser, 
    version, 
    operating_system, 
    customer_uses_other_brands
FROM `fulfillment-dwh-production.curated_data_shared_cx.nps_after_order_all_survey_responses` a
WHERE TRUE
    AND global_entity_id = 'FP_PH'
    AND created_date BETWEEN DATE('2021-09-01') AND DATE('2021-10-31') -- Consider orders between certain start and end dates
    AND is_failed IS NULL
    AND is_pickup IS NULL -- OD orders
    AND survey_type = 'Restaurants' -- Restaurant order 