## logic adapted from _vendors_dynamic_pricing_weekly_history
## https://github.com/deliveryhero/datahub-airflow/blob/0ffd043a95591c4976696cfb4a19a751c27fca57/dags/log/curated_data/sql/_vendors_dynamic_pricing_weekly_history.sql
with
temp_date_table_1 as (
  select generate_timestamp_array(timestamp_sub(last_monday, interval 84 day), last_monday, interval 7 day) as week_start_at
    , date_trunc(current_date(), week(monday)) as current_week
  from (select timestamp_trunc(current_timestamp(), week(monday)) as last_monday)
),
temp_date_table_2 as (
  select week_start_at
    , timestamp_add(week_start_at, interval 7 day) as week_end_at
    , format_date("%G-%V", date(week_start_at)) as week_start_week
  from temp_date_table_1
  left join unnest(week_start_at) as week_start_at
),
dps_active_countries as (
  select distinct
    v.country_code,
    cch.scheme_id,
    cch.active_from,
    cch.active_to,
    d.week_start_week,
  from `fulfillment-dwh-production.cl._dynamic_pricing_country_fee_configuration_versions` v
  left join unnest(country_config_history) cch
  cross join temp_date_table_2 d
  -- take only the configs that were active within the week internal
  where cch.active_from <= d.week_start_at and ifnull(cch.active_to, current_timestamp()) >= d.week_start_at
),
dps_active_vendors as (
  select distinct
    vp.entity_id,
    vp.vendor_code,
    vp.country_code,
    h.active_from,
    h.active_to,
    d.week_start_week,
  from `fulfillment-dwh-production.cl._dynamic_pricing_vendor_versions` vp
  left join unnest(dps_active_vendors_history) h
  cross join temp_date_table_2 d
  -- take only the configs that were active within the week internal
  where h.active_from <= d.week_start_at and ifnull(h.active_to, current_timestamp()) >= d.week_start_at
    and h.is_active
),
country_config_data as (
  select distinct
    vp.entity_id,
    vp.vendor_code,
    vp.country_code,
    c.scheme_id, -- country fallback scheme
    vp.active_from,
    vp.active_to,
    vp.week_start_week,
  from dps_active_vendors vp
  inner join dps_active_countries c using (country_code, week_start_week)
  cross join temp_date_table_2 d
  where vp.active_from <= d.week_start_at and ifnull(vp.active_to, current_timestamp()) >= d.week_start_at
),
asa_vendor_config_data as (
  select distinct
    asa.country_code,
    asa.entity_id,
    asa.vendor_code,
    pc.price_scheme_id as scheme_id,
    h.active_from,
    h.active_to,
    d.week_start_week,
  from `fulfillment-dwh-production.cl.dps_vendor_asa_config_versions` asa
  left join unnest(asa.dps_automatic_assignment_history) h
  left join unnest(h.price_config) pc
  cross join temp_date_table_2 d
  inner join dps_active_vendors dps on
    dps.country_code = asa.country_code and dps.entity_id = asa.entity_id and dps.vendor_code = asa.vendor_code
    and h.active_from <= ifnull(dps.active_to, current_timestamp()) and ifnull(h.active_to, current_timestamp()) >= dps.active_from
  -- take only the configs that were active within the week internal
  where h.active_from <= d.week_start_at and ifnull(h.active_to, current_timestamp()) >= d.week_start_at
),
manual_config_data as (
  -- retrieve schemes from vendor_config and country_config
  select distinct
    msa.country_code,
    msa.entity_id,
    msa.vendor_code,
    h.scheme_id,
    h.active_from,
    h.active_to,
    d.week_start_week,
  from `fulfillment-dwh-production.cl.dps_vendor_config_versions` msa
  left join unnest(vendor_config_history) h
  cross join temp_date_table_2 d
  inner join dps_active_vendors dps on
    dps.country_code = msa.country_code and dps.entity_id = msa.entity_id and dps.vendor_code = msa.vendor_code
    and h.active_from <= ifnull(dps.active_to, current_timestamp()) and ifnull(h.active_to, current_timestamp()) >= dps.active_from
  -- take only the configs that were active within the week internal
  where h.active_from <= d.week_start_at and ifnull(h.active_to, current_timestamp()) >= d.week_start_at
),
all_vendor_config_data as (
  select *
  from asa_vendor_config_data
  union distinct
  select *
  from manual_config_data
  union distinct
  select *
  from country_config_data
),
all_schemes as (
  -- details of the pricing scheme : travel time config id
  select distinct
    ps.region,
    ps.entity_id,
    ps.country_code,
    d.week_start_week,
    d.week_start_at,
    d.week_end_at,
    ps.scheme_id,
    h.travel_time_config_id,
  from `fulfillment-dwh-production.cl.dps_config_versions` ps
  cross join temp_date_table_2 d
  left join unnest(price_scheme_history) h
  left join unnest(travel_time_history) tth
  left join unnest(travel_time_config) ttc on ttc.active_from <= d.week_start_at and (ttc.active_to >= week_start_at or ttc.active_to is null)
  -- take only the configs that were active within the week internal
  where h.active_from <= d.week_start_at and ifnull(h.active_to, current_timestamp()) >= week_start_at
)
select
  region,
  entity_id,
  country_code,
  week_start_week,
  week_start_at,
  week_end_at,
  count(distinct travel_time_config_id) travel_time_components,
  count(distinct scheme_id) schemes,
  count(distinct vendor_code) vendors,
from all_vendor_config_data
left join all_schemes using (country_code, entity_id, scheme_id, week_start_week)
where week_start_at is not null
group by 1,2,3,4,5,6
order by 1,2,3,4