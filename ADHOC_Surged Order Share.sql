DECLARE start_date DATE;
SET start_date = DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);

WITH order_data AS (
  SELECT
    ent.segment AS region,
    entity_id,
    created_date,
    platform_order_code,
    dps_sessionid,
    dps_sessionid_created_at,
    vendor_id,
    dps_customer_tag,
    with_customer_condition,
    condition_id AS time_or_new_cust_condition_id,
    perseus_client_id,
    delivery_fee_local,
    dps_travel_time,
    dps_delivery_fee_local,
    dps_travel_time_fee_local,
    dps_surge_fee_local,
    CASE WHEN dps_surge_fee_local != 0 THEN TRUE ELSE FALSE END AS is_surge_applied,
    basket_value_current_fee_value,
    dps_incentive_discount_local,
    dps_discount,
    is_df_discount_basket_value_deal,  	
    dps_standard_fee_local,
    dps_last_non_zero_df_local,

    scheme_id,
    vendor_price_scheme_type,
    assignment_id,
    has_subscription,
    has_subscription_discount,
    subscription_discount_type,
    components.travel_time_fee_id,
    components.delay_fee_id,
    components.basket_value_fee_id,
    partial_assignments.subscription_id,
  FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` ord
  LEFT JOIN `fulfillment-dwh-production.curated_data_shared_coredata.global_entities` ent ON ent.global_entity_id = ord.entity_id
  WHERE TRUE
    AND created_date >= start_date
),

agg_stats AS (
  SELECT
    region,
    entity_id,
    created_date,
    COUNT(DISTINCT platform_order_code) AS total_order_count,
    COUNT(DISTINCT CASE WHEN is_surge_applied = TRUE THEN platform_order_code END) AS surged_order_count,
    COUNT(DISTINCT CASE WHEN is_surge_applied = FALSE THEN platform_order_code END) AS non_surged_order_count,
  FROM order_data
  GROUP BY 1,2,3
)

SELECT
  *,
  ROUND(surged_order_count / total_order_count, 4) AS surged_order_share,
  ROUND(non_surged_order_count / total_order_count, 4) AS non_surged_order_share,
FROM agg_stats
ORDER BY 1,2,3