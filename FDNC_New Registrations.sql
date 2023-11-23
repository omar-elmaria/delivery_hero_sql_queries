SELECT
  orders.datum,
  SUM(orders.number_of_orders) AS total_orders,
  SUM(IF(registrations.customer_id IS NOT NULL, orders.number_of_orders, 0)) AS orders_by_new_customers,
  SUM(IF(registrations.customer_id IS NULL, orders.number_of_orders, 0)) AS orders_by_old_customers,
  COUNTIF(registrations.customer_id IS NOT NULL) AS new_customers,
  COUNTIF(registrations.customer_id IS NULL) AS old_customers,
  SUM(IF(registrations.customer_id IS NOT NULL, orders.number_of_orders - 1 , 0)) AS repeated_orders_by_new_customers,
FROM (
  SELECT
    DATE(timestamp) AS datum,
    customer.customer_id,
    COUNT(*) AS number_of_orders
  FROM `fulfillment-dwh-production.curated_data_shared_data_stream.orders`
  WHERE TRUE
    AND global_entity_id = 'FP_MY'
    AND DATE(timestamp) > '2021-05-30'
  GROUP BY datum, customer_id 
) orders
LEFT JOIN (
  SELECT
    DISTINCT content.customer_id,
    DATE(timestamp) AS datum
  FROM
    `fulfillment-dwh-production.curated_data_shared_data_stream.customer_event_stream`
  WHERE
    global_entity_id = 'FP_MY'
    AND content.new_registration = TRUE
    AND DATE(timestamp) > '2021-05-30' 
) registrations ON orders.datum = registrations.datum AND orders.customer_id = registrations.customer_id
GROUP BY datum
ORDER BY datum DESC