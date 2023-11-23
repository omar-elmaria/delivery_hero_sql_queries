with
sorted_schemes as (
  select
      entity_id,
      test_id,
      price_scheme_id,
      dense_rank() over(partition by entity_id, test_id order by min(concat(format('%03d',cast(priority as int64)), variation_group))) scheme
  from `fulfillment-dwh-production.cl.dps_experiment_setups`
  group by 1,2,3
  )
select
    entity_id,
    left(region_short_name,2) region_short_name,
    country_code,
    test_id,
    test_name,
    test_start_date,
    test_end_date,
    variation_group,
    variation_share,
    is_active,
    priority,
    hypothesis,
    price_scheme_id,
    scheme,
    array_length(test_vertical_parents)>0 has_vertical_customer_target,
    array_length(zone_ids)>0 has_zone_customer_target,
    array_length(matching_vendor_ids) vendors,
    schedule.id is not null has_time_condition,
    customer_condition.id is not null has_customer_condition,
from `fulfillment-dwh-production.cl.dps_experiment_setups`
left join sorted_schemes using (entity_id, test_id, price_scheme_id)
left join `fulfillment-dwh-production.cl.countries` using (country_code)