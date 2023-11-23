DECLARE entity_var ARRAY <STRING>;
DECLARE v_type, od_status STRING;
DECLARE start_date, end_date DATE;
DECLARE city_wave_1_my ARRAY <STRING>;
DECLARE city_wave_2_my ARRAY <STRING>;
DECLARE city_wave_3_my ARRAY <STRING>;
DECLARE city_wave_1_ph ARRAY <STRING>;
DECLARE city_wave_2_ph ARRAY <STRING>;
DECLARE city_wave_3_ph ARRAY <STRING>;
DECLARE city_wave_1_th ARRAY <STRING>;
DECLARE city_wave_2_th ARRAY <STRING>;
DECLARE city_wave_1_tw ARRAY <STRING>;

DECLARE launch_date_wave_1_my DATE;
DECLARE launch_date_wave_2_my DATE;
DECLARE launch_date_wave_3_my DATE;
DECLARE launch_date_wave_1_ph DATE;
DECLARE launch_date_wave_2_ph DATE;
DECLARE launch_date_wave_3_ph DATE;
DECLARE launch_date_wave_1_sg DATE;
DECLARE launch_date_wave_1_th DATE;
DECLARE launch_date_wave_2_th DATE;
DECLARE launch_date_wave_1_tw DATE;
SET entity_var = ['FP_MY', 'FP_PH', 'FP_SG', 'FP_TH', 'FP_TW']; -- Entities where the FDNC feature is live
SET (v_type, od_status) = ('restaurants', 'OWN_DELIVERY');
SET (start_date, end_date) = (DATE_SUB(DATE('2022-01-10'), INTERVAL 49 DAY), CURRENT_DATE()); -- 10th of Jan was tha first time a FDNC campaign was launched
SET city_wave_1_my = ['Sabah', 'Johor']; -- IN UNNEST()
SET city_wave_2_my = ['Sabah', 'Johor', 'Klang valley']; -- NOT IN UNNEST()
SET city_wave_3_my = ['Klang valley']; -- IN UNNEST()
SET city_wave_1_ph = ['Cebu']; -- IN UNNEST()
SET city_wave_2_ph = ['Manila', 'South mm']; -- IN UNNEST()
SET city_wave_3_ph = ['Cebu', 'Manila', 'South mm']; -- NOT IN UNNEST()
SET city_wave_1_th = ['Ang thong', 'Chachoengsao', 'Chiang Rai', 'Kamphaeng phet', 'Kanchanaburi', 'Lampang', 'Lopburi', 'Mae hong son', 'Maha sarakham', 'Nakhon nayok', 'Nakhon ratchasima', 'Nakhon pathom', 'Nan', 'Phitsanulok', 'Prachinburi', 'Prachuap khiri khan', 'Roi et', 'Sa kaeo', 'Samut sakhon', 'Songkhla', 'Suphanburi', 'Surat Thani', 'Tak', 'Trat', 'Uttaradit', 'Yala']; -- IN UNNEST()
SET city_wave_2_th = ['Ang thong', 'Chachoengsao', 'Chiang Rai', 'Kamphaeng phet', 'Kanchanaburi', 'Lampang', 'Lopburi', 'Mae hong son', 'Maha sarakham', 'Nakhon nayok', 'Nakhon ratchasima', 'Nakhon pathom', 'Nan', 'Phitsanulok', 'Prachinburi', 'Prachuap khiri khan', 'Roi et', 'Sa kaeo', 'Samut sakhon', 'Songkhla', 'Suphanburi', 'Surat Thani', 'Tak', 'Trat', 'Uttaradit', 'Yala']; -- MAKE SURE -- NOT IN UNNEST()
SET city_wave_1_tw = ['Hsinchu', 'Kaohsiung', 'Tainan']; -- IN UNNEST()

SET launch_date_wave_1_my = DATE('2022-01-10');
SET launch_date_wave_2_my = DATE('2022-01-27');
SET launch_date_wave_3_my = DATE('2022-02-18');
SET launch_date_wave_1_ph = DATE('2022-01-11');
SET launch_date_wave_2_ph = DATE('2022-01-19');
SET launch_date_wave_3_ph = DATE('2022-02-24'); -- MAKE SURE
SET launch_date_wave_1_sg = DATE('2022-02-03');
SET launch_date_wave_1_th = DATE('2022-01-27');
SET launch_date_wave_2_th = DATE('2022-02-24'); -- MAKE SURE
SET launch_date_wave_1_tw = DATE('2022-01-20');

##-----------------------------------------------------------------------END OF INPUT SECTION-----------------------------------------------------------------------##

-- Raw order-level data
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.fdnc_dashboard_raw_orders` AS
WITH entities AS (
    SELECT
        ent.region,
        p.entity_id,
        ent.country_iso,
        ent.country_name
FROM `fulfillment-dwh-production.cl.entities` ent
LEFT JOIN UNNEST(platforms) p
INNER JOIN (SELECT DISTINCT entity_id FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`) dps ON p.entity_id = dps.entity_id 
WHERE TRUE
    AND p.entity_id NOT LIKE 'ODR%' -- Eliminate entities starting with DN_ as they are not part of DPS
    AND p.entity_id NOT LIKE 'DN_%' -- Eliminate entities starting with ODR (on-demand riders)
    AND p.entity_id NOT IN ('FP_DE', 'FP_JP') -- Eliminate JP and DE because they are not DH markets any more
    AND p.entity_id != 'TB_SA' -- Eliminate this incorrect entity_id for Saudi
    AND p.entity_id != 'HS_BH' -- Eliminate this incorrect entity_id for Bahrain
),

geo_data AS (
    SELECT 
        p.entity_id
        , cl.country_code
        , ci.id as city_id
        , ci.name as city_name
        , z.id as zone_id
        , z.name AS zone_name
        , z.shape AS zone_shape
    FROM `fulfillment-dwh-production.cl.countries` cl
    LEFT JOIN UNNEST (platforms) p
    LEFT JOIN UNNEST (cities) ci
    LEFT JOIN UNNEST (zones) z
    WHERE TRUE
        AND p.entity_id IN UNNEST(entity_var) -- HERE
        AND cl.country_code NOT LIKE 'ds-%' -- Eliminate records where country code starts with "ds-"
        -- AND ci.name = city_name_var -- HERE
        -- AND z.name IN UNNEST(zone_name_var) -- HERE
        AND ci.is_active -- Active city
        AND z.is_active -- Active zone
),

delivery_costs AS (
    SELECT
        p.entity_id,
        p.order_id, 
        o.platform_order_code,
        SUM(p.delivery_costs) AS delivery_costs_local,
        SUM(p.delivery_costs_eur) AS delivery_costs_eur
    FROM `fulfillment-dwh-production.cl.utr_timings` p
    LEFT JOIN `fulfillment-dwh-production.cl.orders` o ON p.entity_id = o.entity.id AND p.order_id = o.order_id -- Use the platform_order_code in this table as a bridge to join the order_id from utr_timings to order_id from central_dwh.orders 
    WHERE 1=1
        AND p.created_date BETWEEN start_date AND end_date -- HERE (For partitioning elimination and speeding up the query)
        AND o.created_date BETWEEN start_date AND end_date -- HERE (For partitioning elimination and speeding up the query)
    GROUP BY 1,2,3
    ORDER BY 1,2
),

raw_orders AS (
    SELECT
    -- Date and time
    o.created_date,
    o.order_placed_at,

    -- dps_sessions_mapped_to_orders_v2 fields
    -- Location of order
    ent.region,
    ent.country_iso,
    ent.country_name,
    o.entity_id,
    o.city_name,
    o.city_id,
    o.zone_name,
    o.zone_id,

    -- Zone geo data
    geo.zone_shape,
    ST_X(ST_CENTROID(geo.zone_shape)) AS zone_centroid_longitude, 
    ST_Y(ST_CENTROID(geo.zone_shape)) AS zone_centroid_latitude,
    
    -- Order/customer identifiers and session data
    o.order_id,
    o.platform_order_code,
    o.scheme_id,
    o.vendor_price_scheme_type,	-- The assignment type of the scheme to the vendor during the time of the order, such as 'Automatic', 'Manual' and 'Country Fallback'.
    CONCAT(o.scheme_id, ' | ', o.zone_name) AS scheme_zone_concat,
    o.dps_sessionid,
    o.ga_session_id,
    o.dps_customer_tag,
    o.perseus_client_id,

    -- Vendor data and information on the delivery
    o.vendor_id,
    o.chain_id,
    o.chain_name,
    o.vertical_type,
    o.delivery_status,
    o.is_own_delivery,
    o.exchange_rate,

    -- Business KPIs
    o.basket_value_current_fee_value,
    o.standard_fee, -- The standard fee for the session sent by DPS (Not a component of revenue. It's simply the fee from DBDF setup in DPS)
    o.dps_surge_fee_local,
    o.dps_travel_time_fee_local,
    o.dps_delivery_fee_local,
    o.delivery_fee_vat_local,
    o.delivery_fee_local,
    o.is_delivery_fee_covered_by_discount,
    o.is_delivery_fee_covered_by_voucher,
    o.discount_dh_local,
    o.discount_other_local,
    o.voucher_dh_local,
    o.voucher_other_local,
    o.joker_customer_discount_local,
    o.joker_vendor_fee_local,
    o.dps_minimum_order_value_local AS mov_local,
    o.customer_paid_local,
    o.service_fee AS service_fee_local, -- The service fee amount of the session.
    o.gmv_local, -- The gmv (gross merchandise value) of the order placed from backend
    o.gfv_local, -- The gfv (gross food value) of the order placed from backend
    o.gfv_eur,
    o.commission_local,
    o.commission_base_local,
    ROUND(o.commission_local / NULLIF(o.commission_base_local, 0), 4) AS commission_rate,
    IF(gfv_local - dps_minimum_order_value_local >= 0, 0, COALESCE(dwh.value.mov_customer_fee_local, (o.dps_minimum_order_value_local - o.gfv_local))) AS sof_local,
    cst.delivery_costs_local,
    cst.delivery_costs_eur,
    
    -- If an order had a basket value below MOV (i.e. small order fee was charged), add the small order fee calculated as MOV - GFV to the profit 
    COALESCE(
        pd.delivery_fee_local, 
        IF(o.is_delivery_fee_covered_by_discount = TRUE OR o.is_delivery_fee_covered_by_voucher = TRUE, 0, o.dps_delivery_fee_local)
    ) + o.commission_local + o.joker_vendor_fee_local + COALESCE(o.service_fee, 0) + COALESCE(dwh.value.mov_customer_fee_local, IF(o.gfv_local < o.dps_minimum_order_value_local, (o.dps_minimum_order_value_local - o.gfv_local), 0)) AS revenue_local,

    COALESCE(
        pd.delivery_fee_local / o.exchange_rate, 
        IF(o.is_delivery_fee_covered_by_discount = TRUE OR o.is_delivery_fee_covered_by_voucher = TRUE, 0, o.dps_delivery_fee_eur)
    ) + o.commission_eur + o.joker_vendor_fee_eur + COALESCE(o.service_fee / o.exchange_rate, 0) + COALESCE(dwh.value.mov_customer_fee_local / o.exchange_rate, IF(o.gfv_local < o.dps_minimum_order_value_local, (o.dps_minimum_order_value_local - o.gfv_local) / o.exchange_rate, 0)) AS revenue_eur,

    COALESCE(
        pd.delivery_fee_local, 
        IF(o.is_delivery_fee_covered_by_discount = TRUE OR o.is_delivery_fee_covered_by_voucher = TRUE, 0, o.dps_delivery_fee_local)
    ) + o.commission_local + o.joker_vendor_fee_local + COALESCE(o.service_fee, 0) + COALESCE(dwh.value.mov_customer_fee_local, IF(o.gfv_local < o.dps_minimum_order_value_local, (o.dps_minimum_order_value_local - o.gfv_local), 0)) - cst.delivery_costs_local AS gross_profit_local,

    COALESCE(
        pd.delivery_fee_local / o.exchange_rate, 
        IF(o.is_delivery_fee_covered_by_discount = TRUE OR o.is_delivery_fee_covered_by_voucher = TRUE, 0, o.dps_delivery_fee_eur)
    ) + o.commission_eur + o.joker_vendor_fee_eur + COALESCE(o.service_fee / o.exchange_rate, 0) + COALESCE(dwh.value.mov_customer_fee_local / o.exchange_rate, IF(o.gfv_local < o.dps_minimum_order_value_local, (o.dps_minimum_order_value_local - o.gfv_local) / o.exchange_rate, 0)) - cst.delivery_costs_local / o.exchange_rate AS gross_profit_eur,

    -- Logistics KPIs
    o.mean_delay, -- A.K.A Average fleet delay --> Average lateness in minutes of an order placed at this time (Used by dashboard, das, dps). This data point is only available for OD orders.
    o.travel_time, -- The time (min) it takes rider to travel from vendor location coordinates to the customers. This data point is only available for OD orders.
    o.travel_time_distance_km, -- The distance (km) between the vendor location coordinates and customer location coordinates. This data point is only available for OD orders.
    o.delivery_distance_m, -- This is the "Delivery Distance" field in the overview tab in the AB test dashboard. The Manhattan distance (km) between the vendor location coordinates and customer location coordinates. This distance doesn't take into account potential stacked deliveries, and it's not the travelled distance. This data point is only available for OD orders.
    o.to_customer_time, -- The time difference between rider arrival at customer and the pickup time. This data point is only available for OD orders
    o.actual_DT,

    -- Centra DWH fields	
    dwh.value.delivery_fee_local AS delivery_fee_local_cdwh,	
    dwh.value.delivery_fee_vat_local AS delivery_fee_vat_local_cdwh,
    dwh.value.voucher_dh_local AS voucher_dh_local_cdwh,	
    dwh.value.voucher_other_local AS voucher_other_local_cdwh,	
    dwh.value.discount_dh_local AS discount_dh_local_cdwh,	
    dwh.value.discount_other_local AS discount_other_local_cdwh,	
    dwh.value.joker_customer_discount_local AS joker_customer_discount_local_cdwh,
    dwh.value.joker_vendor_fee_local AS joker_vendor_fee_local_cdwh,
    dwh.is_joker,
    dwh.value.gbv_local AS gfv_local_cdwh,
    dwh.value.customer_paid_local AS customer_paid_local_cdwh,
    dwh.value.mov_local AS mov_local_cdwh,
    dwh.value.mov_customer_fee_local AS sof_cdwh,
    dwh.payment_method,
    dwh.payment_type,

    -- Pandata fields
    pd.service_fee_total_local AS service_fee_total_local_pd,
    pd.container_price_local AS container_price_local_pd,
    pd.delivery_fee_local AS delivery_fee_local_pd,
    pd.delivery_fee_forced_local AS delivery_fee_forced_local_pd,
    pd.delivery_fee_original_local AS delivery_fee_original_local_pd,
    pd.delivery_fee_vat_rate AS delivery_fee_vat_rate_pd,	
    pd.product_vat_groups AS product_vat_groups_pd,	
    pd.vat_rate AS vat_rate_pd,
    pd.delivery_fee_vat_local AS delivery_fee_vat_local_pd,	
    pd.products_vat_amount_local AS products_vat_amount_local_pd,
    pd.vat_amount_local AS vat_amount_local_pd,

    -- Special fields
    CASE
        WHEN ent.region IN ('Europe', 'Asia') THEN COALESCE( -- Get the delivery fee data of Pandora countries from Pandata tables
            pd.delivery_fee_local, 
            IF(o.is_delivery_fee_covered_by_discount = TRUE OR o.is_delivery_fee_covered_by_voucher = TRUE, 0, o.dps_delivery_fee_local)
        )
        WHEN ent.region NOT IN ('Europe', 'Asia') THEN (CASE WHEN is_delivery_fee_covered_by_voucher = FALSE AND is_delivery_fee_covered_by_discount = FALSE THEN o.delivery_fee_local ELSE 0 END) -- If the order comes from a non-Pandora country, use delivery_fee_local
    END AS actual_df_paid_by_customer,

    -- Wave number
    CASE -- HERE
        -- MY
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_1_my) THEN 'Wave 1'
        WHEN o.entity_id = 'FP_MY' AND o.city_name NOT IN UNNEST(city_wave_2_my) THEN 'Wave 2'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_3_my) THEN 'Wave 3'

        -- PH
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_1_ph) THEN 'Wave 1'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_2_ph) THEN 'Wave 2'
        WHEN o.entity_id = 'FP_PH' AND o.city_name NOT IN UNNEST(city_wave_3_ph) THEN 'Wave 3'

        -- SG
        WHEN o.entity_id = 'FP_SG' THEN 'Wave 1'

        -- TH
        WHEN o.entity_id = 'FP_TH' AND o.city_name IN UNNEST(city_wave_1_th) THEN 'Wave 1'
        WHEN o.entity_id = 'FP_TH' AND o.city_name NOT IN UNNEST(city_wave_2_ph) THEN 'Wave 2'

        -- TW
        WHEN o.entity_id = 'FP_TW' AND o.city_name IN UNNEST(city_wave_1_tw) THEN 'Wave 1'
    ELSE 'Not Included in FDNC Waves'
    END AS wave, 

    -- Pre/Post Period
    CASE -- HERE
        -- MY
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_1_my) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_my, INTERVAL 49 DAY) AND DATE_SUB(launch_date_wave_1_my, INTERVAL 1 DAY)) THEN 'Pre-period'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_1_my) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_my, INTERVAL 1 DAY) AND DATE_ADD(launch_date_wave_1_my, INTERVAL 49 DAY)) THEN 'Post-period'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_1_my) AND o.created_date = launch_date_wave_1_my THEN 'Transition day'

        WHEN o.entity_id = 'FP_MY' AND o.city_name NOT IN UNNEST(city_wave_2_my) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_2_my, INTERVAL 49 DAY) AND DATE_SUB(launch_date_wave_2_my, INTERVAL 1 DAY)) THEN 'Pre-period'
        WHEN o.entity_id = 'FP_MY' AND o.city_name NOT IN UNNEST(city_wave_2_my) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_2_my, INTERVAL 1 DAY) AND DATE_ADD(launch_date_wave_2_my, INTERVAL 49 DAY)) THEN 'Post-period'
        WHEN o.entity_id = 'FP_MY' AND o.city_name NOT IN UNNEST(city_wave_2_my) AND o.created_date = launch_date_wave_2_my THEN 'Transition day'

        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_3_my) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_3_my, INTERVAL 49 DAY) AND DATE_SUB(launch_date_wave_3_my, INTERVAL 1 DAY)) THEN 'Pre-period'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_3_my) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_3_my, INTERVAL 1 DAY) AND DATE_ADD(launch_date_wave_3_my, INTERVAL 49 DAY)) THEN 'Post-period'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_3_my) AND o.created_date = launch_date_wave_3_my THEN 'Transition day'

        -- PH
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_1_ph) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_ph, INTERVAL 49 DAY) AND DATE_SUB(launch_date_wave_1_ph, INTERVAL 1 DAY)) THEN 'Pre-period'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_1_ph) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_ph, INTERVAL 1 DAY) AND DATE_ADD(launch_date_wave_1_ph, INTERVAL 49 DAY)) THEN 'Post-period'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_1_ph) AND o.created_date = launch_date_wave_1_ph THEN 'Transition day'

        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_2_ph) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_2_ph, INTERVAL 49 DAY) AND DATE_SUB(launch_date_wave_2_ph, INTERVAL 1 DAY)) THEN 'Pre-period'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_2_ph) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_2_ph, INTERVAL 1 DAY) AND DATE_ADD(launch_date_wave_2_ph, INTERVAL 49 DAY)) THEN 'Post-period'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_2_ph) AND o.created_date = launch_date_wave_2_ph THEN 'Transition day'

        WHEN o.entity_id = 'FP_PH' AND o.city_name NOT IN UNNEST(city_wave_3_ph) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_3_ph, INTERVAL 49 DAY) AND DATE_SUB(launch_date_wave_3_ph, INTERVAL 1 DAY)) THEN 'Pre-period'
        WHEN o.entity_id = 'FP_PH' AND o.city_name NOT IN UNNEST(city_wave_3_ph) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_3_ph, INTERVAL 1 DAY) AND DATE_ADD(launch_date_wave_3_ph, INTERVAL 49 DAY)) THEN 'Post-period'
        WHEN o.entity_id = 'FP_PH' AND o.city_name NOT IN UNNEST(city_wave_3_ph) AND o.created_date = launch_date_wave_3_ph THEN 'Transition day'

        -- SG
        WHEN o.entity_id = 'FP_SG' AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_sg, INTERVAL 49 DAY) AND DATE_SUB(launch_date_wave_1_sg, INTERVAL 1 DAY)) THEN 'Pre-period'
        WHEN o.entity_id = 'FP_SG' AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_sg, INTERVAL 1 DAY) AND DATE_ADD(launch_date_wave_1_sg, INTERVAL 49 DAY)) THEN 'Post-period'
        WHEN o.entity_id = 'FP_SG' AND o.created_date = launch_date_wave_1_sg THEN 'Transition day'

        -- TH
        WHEN o.entity_id = 'FP_TH' AND o.city_name IN UNNEST(city_wave_1_th) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_th, INTERVAL 49 DAY) AND DATE_SUB(launch_date_wave_1_th, INTERVAL 1 DAY)) THEN 'Pre-period'
        WHEN o.entity_id = 'FP_TH' AND o.city_name IN UNNEST(city_wave_1_th) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_th, INTERVAL 1 DAY) AND DATE_ADD(launch_date_wave_1_th, INTERVAL 49 DAY)) THEN 'Post-period'
        WHEN o.entity_id = 'FP_TH' AND o.city_name IN UNNEST(city_wave_1_th) AND o.created_date = launch_date_wave_1_th THEN 'Transition day'

        WHEN o.entity_id = 'FP_TH' AND o.city_name NOT IN UNNEST(city_wave_2_th) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_2_th, INTERVAL 49 DAY) AND DATE_SUB(launch_date_wave_2_th, INTERVAL 1 DAY)) THEN 'Pre-period'
        WHEN o.entity_id = 'FP_TH' AND o.city_name NOT IN UNNEST(city_wave_2_th) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_2_th, INTERVAL 1 DAY) AND DATE_ADD(launch_date_wave_2_th, INTERVAL 49 DAY)) THEN 'Post-period'
        WHEN o.entity_id = 'FP_TH' AND o.city_name NOT IN UNNEST(city_wave_2_th) AND o.created_date = launch_date_wave_2_th THEN 'Transition day'

        -- TW
        WHEN o.entity_id = 'FP_TW' AND o.city_name IN UNNEST(city_wave_1_tw) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_tw, INTERVAL 49 DAY) AND DATE_SUB(launch_date_wave_1_tw, INTERVAL 1 DAY)) THEN 'Pre-period'
        WHEN o.entity_id = 'FP_TW' AND o.city_name IN UNNEST(city_wave_1_tw) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_tw, INTERVAL 1 DAY) AND DATE_ADD(launch_date_wave_1_tw, INTERVAL 49 DAY)) THEN 'Post-period'
        WHEN o.entity_id = 'FP_TW' AND o.city_name IN UNNEST(city_wave_1_tw) AND o.created_date = launch_date_wave_1_tw THEN 'Transition day'
    ELSE 'Period Not Considered'
    END AS period,

    -- Week "N"
    CASE -- HERE
        -- MY (Wave 1 - Pre-period)
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_1_my) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_my, INTERVAL 49 DAY) AND DATE_SUB(launch_date_wave_1_my, INTERVAL 43 DAY)) THEN 'WK 1'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_1_my) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_my, INTERVAL 42 DAY) AND DATE_SUB(launch_date_wave_1_my, INTERVAL 36 DAY)) THEN 'WK 2'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_1_my) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_my, INTERVAL 35 DAY) AND DATE_SUB(launch_date_wave_1_my, INTERVAL 29 DAY)) THEN 'WK 3'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_1_my) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_my, INTERVAL 28 DAY) AND DATE_SUB(launch_date_wave_1_my, INTERVAL 22 DAY)) THEN 'WK 4'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_1_my) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_my, INTERVAL 21 DAY) AND DATE_SUB(launch_date_wave_1_my, INTERVAL 15 DAY)) THEN 'WK 5'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_1_my) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_my, INTERVAL 14 DAY) AND DATE_SUB(launch_date_wave_1_my, INTERVAL 8 DAY)) THEN 'WK 6'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_1_my) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_my, INTERVAL 7 DAY) AND DATE_SUB(launch_date_wave_1_my, INTERVAL 1 DAY)) THEN 'WK 7'

        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_1_my) AND o.created_date = launch_date_wave_1_my THEN 'Transition day'

        -- MY (Wave 1 - Post Period)
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_1_my) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_my, INTERVAL 1 DAY) AND DATE_ADD(launch_date_wave_1_my, INTERVAL 7 DAY)) THEN 'WK 1'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_1_my) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_my, INTERVAL 8 DAY) AND DATE_ADD(launch_date_wave_1_my, INTERVAL 14 DAY)) THEN 'WK 2'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_1_my) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_my, INTERVAL 15 DAY) AND DATE_ADD(launch_date_wave_1_my, INTERVAL 21 DAY)) THEN 'WK 3'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_1_my) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_my, INTERVAL 22 DAY) AND DATE_ADD(launch_date_wave_1_my, INTERVAL 28 DAY)) THEN 'WK 4'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_1_my) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_my, INTERVAL 29 DAY) AND DATE_ADD(launch_date_wave_1_my, INTERVAL 35 DAY)) THEN 'WK 5'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_1_my) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_my, INTERVAL 36 DAY) AND DATE_ADD(launch_date_wave_1_my, INTERVAL 42 DAY)) THEN 'WK 6'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_1_my) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_my, INTERVAL 43 DAY) AND DATE_ADD(launch_date_wave_1_my, INTERVAL 49 DAY)) THEN 'WK 7'

        -- MY (Wave 2 - Pre-period)
        WHEN o.entity_id = 'FP_MY' AND o.city_name NOT IN UNNEST(city_wave_2_my) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_2_my, INTERVAL 49 DAY) AND DATE_SUB(launch_date_wave_2_my, INTERVAL 43 DAY)) THEN 'WK 1'
        WHEN o.entity_id = 'FP_MY' AND o.city_name NOT IN UNNEST(city_wave_2_my) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_2_my, INTERVAL 42 DAY) AND DATE_SUB(launch_date_wave_2_my, INTERVAL 36 DAY)) THEN 'WK 2'
        WHEN o.entity_id = 'FP_MY' AND o.city_name NOT IN UNNEST(city_wave_2_my) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_2_my, INTERVAL 35 DAY) AND DATE_SUB(launch_date_wave_2_my, INTERVAL 29 DAY)) THEN 'WK 3'
        WHEN o.entity_id = 'FP_MY' AND o.city_name NOT IN UNNEST(city_wave_2_my) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_2_my, INTERVAL 28 DAY) AND DATE_SUB(launch_date_wave_2_my, INTERVAL 22 DAY)) THEN 'WK 4'
        WHEN o.entity_id = 'FP_MY' AND o.city_name NOT IN UNNEST(city_wave_2_my) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_2_my, INTERVAL 21 DAY) AND DATE_SUB(launch_date_wave_2_my, INTERVAL 15 DAY)) THEN 'WK 5'
        WHEN o.entity_id = 'FP_MY' AND o.city_name NOT IN UNNEST(city_wave_2_my) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_2_my, INTERVAL 14 DAY) AND DATE_SUB(launch_date_wave_2_my, INTERVAL 8 DAY)) THEN 'WK 6'
        WHEN o.entity_id = 'FP_MY' AND o.city_name NOT IN UNNEST(city_wave_2_my) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_2_my, INTERVAL 7  DAY) AND DATE_SUB(launch_date_wave_2_my, INTERVAL 1 DAY)) THEN 'WK 7'

        WHEN o.entity_id = 'FP_MY' AND o.city_name NOT IN UNNEST(city_wave_2_my) AND o.created_date = launch_date_wave_2_my THEN 'Transition day'

        -- MY (Wave 2 - Post-period)
        WHEN o.entity_id = 'FP_MY' AND o.city_name NOT IN UNNEST(city_wave_2_my) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_2_my, INTERVAL 1 DAY) AND DATE_ADD(launch_date_wave_2_my, INTERVAL 7 DAY)) THEN 'WK 1'
        WHEN o.entity_id = 'FP_MY' AND o.city_name NOT IN UNNEST(city_wave_2_my) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_2_my, INTERVAL 8 DAY) AND DATE_ADD(launch_date_wave_2_my, INTERVAL 14 DAY)) THEN 'WK 2'
        WHEN o.entity_id = 'FP_MY' AND o.city_name NOT IN UNNEST(city_wave_2_my) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_2_my, INTERVAL 15 DAY) AND DATE_ADD(launch_date_wave_2_my, INTERVAL 21 DAY)) THEN 'WK 3'
        WHEN o.entity_id = 'FP_MY' AND o.city_name NOT IN UNNEST(city_wave_2_my) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_2_my, INTERVAL 22 DAY) AND DATE_ADD(launch_date_wave_2_my, INTERVAL 28 DAY)) THEN 'WK 4'
        WHEN o.entity_id = 'FP_MY' AND o.city_name NOT IN UNNEST(city_wave_2_my) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_2_my, INTERVAL 29 DAY) AND DATE_ADD(launch_date_wave_2_my, INTERVAL 35 DAY)) THEN 'WK 5'
        WHEN o.entity_id = 'FP_MY' AND o.city_name NOT IN UNNEST(city_wave_2_my) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_2_my, INTERVAL 36 DAY) AND DATE_ADD(launch_date_wave_2_my, INTERVAL 42 DAY)) THEN 'WK 6'
        WHEN o.entity_id = 'FP_MY' AND o.city_name NOT IN UNNEST(city_wave_2_my) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_2_my, INTERVAL 43 DAY) AND DATE_ADD(launch_date_wave_2_my, INTERVAL 49 DAY)) THEN 'WK 7'

        -- MY (Wave 3 - Pre-period)
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_3_my) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_3_my, INTERVAL 49 DAY) AND DATE_SUB(launch_date_wave_3_my, INTERVAL 43 DAY)) THEN 'WK 1'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_3_my) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_3_my, INTERVAL 42 DAY) AND DATE_SUB(launch_date_wave_3_my, INTERVAL 36 DAY)) THEN 'WK 2'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_3_my) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_3_my, INTERVAL 35 DAY) AND DATE_SUB(launch_date_wave_3_my, INTERVAL 29 DAY)) THEN 'WK 3'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_3_my) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_3_my, INTERVAL 28 DAY) AND DATE_SUB(launch_date_wave_3_my, INTERVAL 22 DAY)) THEN 'WK 4'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_3_my) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_3_my, INTERVAL 21 DAY) AND DATE_SUB(launch_date_wave_3_my, INTERVAL 15 DAY)) THEN 'WK 5'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_3_my) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_3_my, INTERVAL 14 DAY) AND DATE_SUB(launch_date_wave_3_my, INTERVAL 8 DAY)) THEN 'WK 6'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_3_my) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_3_my, INTERVAL 7  DAY) AND DATE_SUB(launch_date_wave_3_my, INTERVAL 1 DAY)) THEN 'WK 7'

        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_3_my) AND o.created_date = launch_date_wave_3_my THEN 'Transition day'

        -- MY (Wave 3 - Post-period)
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_3_my) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_3_my, INTERVAL 1 DAY) AND DATE_ADD(launch_date_wave_3_my, INTERVAL 7 DAY)) THEN 'WK 1'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_3_my) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_3_my, INTERVAL 8 DAY) AND DATE_ADD(launch_date_wave_3_my, INTERVAL 14 DAY)) THEN 'WK 2'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_3_my) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_3_my, INTERVAL 15 DAY) AND DATE_ADD(launch_date_wave_3_my, INTERVAL 21 DAY)) THEN 'WK 3'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_3_my) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_3_my, INTERVAL 22 DAY) AND DATE_ADD(launch_date_wave_3_my, INTERVAL 28 DAY)) THEN 'WK 4'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_3_my) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_3_my, INTERVAL 29 DAY) AND DATE_ADD(launch_date_wave_3_my, INTERVAL 35 DAY)) THEN 'WK 5'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_3_my) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_3_my, INTERVAL 36 DAY) AND DATE_ADD(launch_date_wave_3_my, INTERVAL 42 DAY)) THEN 'WK 6'
        WHEN o.entity_id = 'FP_MY' AND o.city_name IN UNNEST(city_wave_3_my) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_3_my, INTERVAL 43 DAY) AND DATE_ADD(launch_date_wave_3_my, INTERVAL 49 DAY)) THEN 'WK 7'

        -- PH (Wave 1 - Pre-period)
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_1_ph) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_ph, INTERVAL 49 DAY) AND DATE_SUB(launch_date_wave_1_ph, INTERVAL 43 DAY)) THEN 'WK 1'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_1_ph) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_ph, INTERVAL 42 DAY) AND DATE_SUB(launch_date_wave_1_ph, INTERVAL 36 DAY)) THEN 'WK 2'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_1_ph) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_ph, INTERVAL 35 DAY) AND DATE_SUB(launch_date_wave_1_ph, INTERVAL 29 DAY)) THEN 'WK 3'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_1_ph) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_ph, INTERVAL 28 DAY) AND DATE_SUB(launch_date_wave_1_ph, INTERVAL 22 DAY)) THEN 'WK 4'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_1_ph) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_ph, INTERVAL 21 DAY) AND DATE_SUB(launch_date_wave_1_ph, INTERVAL 15 DAY)) THEN 'WK 5'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_1_ph) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_ph, INTERVAL 14 DAY) AND DATE_SUB(launch_date_wave_1_ph, INTERVAL 8 DAY)) THEN 'WK 6'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_1_ph) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_ph, INTERVAL 7  DAY) AND DATE_SUB(launch_date_wave_1_ph, INTERVAL 1 DAY)) THEN 'WK 7'

        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_1_ph) AND o.created_date = launch_date_wave_1_ph THEN 'Transition day'

        -- PH (Wave 1 - Post-period)
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_1_ph) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_ph, INTERVAL 1 DAY) AND DATE_ADD(launch_date_wave_1_ph, INTERVAL 7 DAY)) THEN 'WK 1'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_1_ph) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_ph, INTERVAL 8 DAY) AND DATE_ADD(launch_date_wave_1_ph, INTERVAL 14 DAY)) THEN 'WK 2'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_1_ph) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_ph, INTERVAL 15 DAY) AND DATE_ADD(launch_date_wave_1_ph, INTERVAL 21 DAY)) THEN 'WK 3'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_1_ph) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_ph, INTERVAL 22 DAY) AND DATE_ADD(launch_date_wave_1_ph, INTERVAL 28 DAY)) THEN 'WK 4'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_1_ph) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_ph, INTERVAL 29 DAY) AND DATE_ADD(launch_date_wave_1_ph, INTERVAL 35 DAY)) THEN 'WK 5'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_1_ph) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_ph, INTERVAL 36 DAY) AND DATE_ADD(launch_date_wave_1_ph, INTERVAL 42 DAY)) THEN 'WK 6'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_1_ph) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_ph, INTERVAL 43 DAY) AND DATE_ADD(launch_date_wave_1_ph, INTERVAL 49 DAY)) THEN 'WK 7'

        -- PH (Wave 2 - Pre-period)
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_2_ph) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_2_ph, INTERVAL 49 DAY) AND DATE_SUB(launch_date_wave_2_ph, INTERVAL 43 DAY)) THEN 'WK 1'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_2_ph) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_2_ph, INTERVAL 42 DAY) AND DATE_SUB(launch_date_wave_2_ph, INTERVAL 36 DAY)) THEN 'WK 2'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_2_ph) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_2_ph, INTERVAL 35 DAY) AND DATE_SUB(launch_date_wave_2_ph, INTERVAL 29 DAY)) THEN 'WK 3'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_2_ph) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_2_ph, INTERVAL 28 DAY) AND DATE_SUB(launch_date_wave_2_ph, INTERVAL 22 DAY)) THEN 'WK 4'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_2_ph) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_2_ph, INTERVAL 21 DAY) AND DATE_SUB(launch_date_wave_2_ph, INTERVAL 15 DAY)) THEN 'WK 5'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_2_ph) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_2_ph, INTERVAL 14 DAY) AND DATE_SUB(launch_date_wave_2_ph, INTERVAL 8 DAY)) THEN 'WK 6'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_2_ph) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_2_ph, INTERVAL 7  DAY) AND DATE_SUB(launch_date_wave_2_ph, INTERVAL 1 DAY)) THEN 'WK 7'
        
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_2_ph) AND o.created_date = launch_date_wave_2_ph THEN 'Transition day'

        -- PH (Wave 2 - Post-period)
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_2_ph) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_2_ph, INTERVAL 1 DAY) AND DATE_ADD(launch_date_wave_2_ph, INTERVAL 7 DAY)) THEN 'WK 1'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_2_ph) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_2_ph, INTERVAL 8 DAY) AND DATE_ADD(launch_date_wave_2_ph, INTERVAL 14 DAY)) THEN 'WK 2'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_2_ph) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_2_ph, INTERVAL 15 DAY) AND DATE_ADD(launch_date_wave_2_ph, INTERVAL 21 DAY)) THEN 'WK 3'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_2_ph) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_2_ph, INTERVAL 22 DAY) AND DATE_ADD(launch_date_wave_2_ph, INTERVAL 28 DAY)) THEN 'WK 4'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_2_ph) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_2_ph, INTERVAL 29 DAY) AND DATE_ADD(launch_date_wave_2_ph, INTERVAL 35 DAY)) THEN 'WK 5'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_2_ph) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_2_ph, INTERVAL 36 DAY) AND DATE_ADD(launch_date_wave_2_ph, INTERVAL 42 DAY)) THEN 'WK 6'
        WHEN o.entity_id = 'FP_PH' AND o.city_name IN UNNEST(city_wave_2_ph) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_2_ph, INTERVAL 43 DAY) AND DATE_ADD(launch_date_wave_2_ph, INTERVAL 49 DAY)) THEN 'WK 7'

        -- PH (Wave 3 - Pre-period)
        WHEN o.entity_id = 'FP_PH' AND o.city_name NOT IN UNNEST(city_wave_3_ph) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_3_ph, INTERVAL 49 DAY) AND DATE_SUB(launch_date_wave_3_ph, INTERVAL 43 DAY)) THEN 'WK 1'
        WHEN o.entity_id = 'FP_PH' AND o.city_name NOT IN UNNEST(city_wave_3_ph) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_3_ph, INTERVAL 42 DAY) AND DATE_SUB(launch_date_wave_3_ph, INTERVAL 36 DAY)) THEN 'WK 2'
        WHEN o.entity_id = 'FP_PH' AND o.city_name NOT IN UNNEST(city_wave_3_ph) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_3_ph, INTERVAL 35 DAY) AND DATE_SUB(launch_date_wave_3_ph, INTERVAL 29 DAY)) THEN 'WK 3'
        WHEN o.entity_id = 'FP_PH' AND o.city_name NOT IN UNNEST(city_wave_3_ph) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_3_ph, INTERVAL 28 DAY) AND DATE_SUB(launch_date_wave_3_ph, INTERVAL 22 DAY)) THEN 'WK 4'
        WHEN o.entity_id = 'FP_PH' AND o.city_name NOT IN UNNEST(city_wave_3_ph) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_3_ph, INTERVAL 21 DAY) AND DATE_SUB(launch_date_wave_3_ph, INTERVAL 15 DAY)) THEN 'WK 5'
        WHEN o.entity_id = 'FP_PH' AND o.city_name NOT IN UNNEST(city_wave_3_ph) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_3_ph, INTERVAL 14 DAY) AND DATE_SUB(launch_date_wave_3_ph, INTERVAL 8  DAY)) THEN 'WK 6'
        WHEN o.entity_id = 'FP_PH' AND o.city_name NOT IN UNNEST(city_wave_3_ph) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_3_ph, INTERVAL 7  DAY) AND DATE_SUB(launch_date_wave_3_ph, INTERVAL 1  DAY)) THEN 'WK 7'
        
        WHEN o.entity_id = 'FP_PH' AND o.city_name NOT IN UNNEST(city_wave_3_ph) AND o.created_date = launch_date_wave_3_ph THEN 'Transition day'

        -- PH (Wave 3 - Post-period)
        WHEN o.entity_id = 'FP_PH' AND o.city_name NOT IN UNNEST(city_wave_3_ph) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_3_ph, INTERVAL 1 DAY) AND DATE_ADD(launch_date_wave_3_ph, INTERVAL 7 DAY)) THEN 'WK 1'
        WHEN o.entity_id = 'FP_PH' AND o.city_name NOT IN UNNEST(city_wave_3_ph) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_3_ph, INTERVAL 8 DAY) AND DATE_ADD(launch_date_wave_3_ph, INTERVAL 14 DAY)) THEN 'WK 2'
        WHEN o.entity_id = 'FP_PH' AND o.city_name NOT IN UNNEST(city_wave_3_ph) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_3_ph, INTERVAL 15 DAY) AND DATE_ADD(launch_date_wave_3_ph, INTERVAL 21 DAY)) THEN 'WK 3'
        WHEN o.entity_id = 'FP_PH' AND o.city_name NOT IN UNNEST(city_wave_3_ph) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_3_ph, INTERVAL 22 DAY) AND DATE_ADD(launch_date_wave_3_ph, INTERVAL 28 DAY)) THEN 'WK 4'
        WHEN o.entity_id = 'FP_PH' AND o.city_name NOT IN UNNEST(city_wave_3_ph) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_3_ph, INTERVAL 29 DAY) AND DATE_ADD(launch_date_wave_3_ph, INTERVAL 35 DAY)) THEN 'WK 5'
        WHEN o.entity_id = 'FP_PH' AND o.city_name NOT IN UNNEST(city_wave_3_ph) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_3_ph, INTERVAL 36 DAY) AND DATE_ADD(launch_date_wave_3_ph, INTERVAL 42 DAY)) THEN 'WK 6'
        WHEN o.entity_id = 'FP_PH' AND o.city_name NOT IN UNNEST(city_wave_3_ph) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_3_ph, INTERVAL 43 DAY) AND DATE_ADD(launch_date_wave_3_ph, INTERVAL 49 DAY)) THEN 'WK 7'

        -- SG (Wave 1 - Pre-period)
        WHEN o.entity_id = 'FP_SG' AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_sg, INTERVAL 49 DAY) AND DATE_SUB(launch_date_wave_1_sg, INTERVAL 43 DAY)) THEN 'WK 1'
        WHEN o.entity_id = 'FP_SG' AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_sg, INTERVAL 42 DAY) AND DATE_SUB(launch_date_wave_1_sg, INTERVAL 36 DAY)) THEN 'WK 2'
        WHEN o.entity_id = 'FP_SG' AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_sg, INTERVAL 35 DAY) AND DATE_SUB(launch_date_wave_1_sg, INTERVAL 29 DAY)) THEN 'WK 3'
        WHEN o.entity_id = 'FP_SG' AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_sg, INTERVAL 28 DAY) AND DATE_SUB(launch_date_wave_1_sg, INTERVAL 22 DAY)) THEN 'WK 4'
        WHEN o.entity_id = 'FP_SG' AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_sg, INTERVAL 21 DAY) AND DATE_SUB(launch_date_wave_1_sg, INTERVAL 15 DAY)) THEN 'WK 5'
        WHEN o.entity_id = 'FP_SG' AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_sg, INTERVAL 14 DAY) AND DATE_SUB(launch_date_wave_1_sg, INTERVAL 8 DAY)) THEN 'WK 6'
        WHEN o.entity_id = 'FP_SG' AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_sg, INTERVAL 7  DAY) AND DATE_SUB(launch_date_wave_1_sg, INTERVAL 1 DAY)) THEN 'WK 7'

        WHEN o.entity_id = 'FP_SG' AND o.created_date = launch_date_wave_1_sg THEN 'Transition day'

        -- SG (Wave 1 - Post-period)
        WHEN o.entity_id = 'FP_SG' AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_sg, INTERVAL 1 DAY) AND DATE_ADD(launch_date_wave_1_sg, INTERVAL 7 DAY)) THEN 'WK 1'
        WHEN o.entity_id = 'FP_SG' AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_sg, INTERVAL 8 DAY) AND DATE_ADD(launch_date_wave_1_sg, INTERVAL 14 DAY)) THEN 'WK 2'
        WHEN o.entity_id = 'FP_SG' AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_sg, INTERVAL 15 DAY) AND DATE_ADD(launch_date_wave_1_sg, INTERVAL 21 DAY)) THEN 'WK 3'
        WHEN o.entity_id = 'FP_SG' AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_sg, INTERVAL 22 DAY) AND DATE_ADD(launch_date_wave_1_sg, INTERVAL 28 DAY)) THEN 'WK 4'
        WHEN o.entity_id = 'FP_SG' AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_sg, INTERVAL 29 DAY) AND DATE_ADD(launch_date_wave_1_sg, INTERVAL 35 DAY)) THEN 'WK 5'
        WHEN o.entity_id = 'FP_SG' AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_sg, INTERVAL 36 DAY) AND DATE_ADD(launch_date_wave_1_sg, INTERVAL 42 DAY)) THEN 'WK 6'
        WHEN o.entity_id = 'FP_SG' AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_sg, INTERVAL 43 DAY) AND DATE_ADD(launch_date_wave_1_sg, INTERVAL 49 DAY)) THEN 'WK 7'

        -- TH (Wave 1 - Pre-period)
        WHEN o.entity_id = 'FP_TH' AND o.city_name IN UNNEST(city_wave_1_th) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_th, INTERVAL 49 DAY) AND DATE_SUB(launch_date_wave_1_th, INTERVAL 43 DAY)) THEN 'WK 1'
        WHEN o.entity_id = 'FP_TH' AND o.city_name IN UNNEST(city_wave_1_th) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_th, INTERVAL 42 DAY) AND DATE_SUB(launch_date_wave_1_th, INTERVAL 36 DAY)) THEN 'WK 2'
        WHEN o.entity_id = 'FP_TH' AND o.city_name IN UNNEST(city_wave_1_th) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_th, INTERVAL 35 DAY) AND DATE_SUB(launch_date_wave_1_th, INTERVAL 29 DAY)) THEN 'WK 3'
        WHEN o.entity_id = 'FP_TH' AND o.city_name IN UNNEST(city_wave_1_th) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_th, INTERVAL 28 DAY) AND DATE_SUB(launch_date_wave_1_th, INTERVAL 22 DAY)) THEN 'WK 4'
        WHEN o.entity_id = 'FP_TH' AND o.city_name IN UNNEST(city_wave_1_th) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_th, INTERVAL 21 DAY) AND DATE_SUB(launch_date_wave_1_th, INTERVAL 15 DAY)) THEN 'WK 5'
        WHEN o.entity_id = 'FP_TH' AND o.city_name IN UNNEST(city_wave_1_th) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_th, INTERVAL 14 DAY) AND DATE_SUB(launch_date_wave_1_th, INTERVAL 8 DAY)) THEN 'WK 6'
        WHEN o.entity_id = 'FP_TH' AND o.city_name IN UNNEST(city_wave_1_th) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_th, INTERVAL 7  DAY) AND DATE_SUB(launch_date_wave_1_th, INTERVAL 1 DAY)) THEN 'WK 7'
        
        WHEN o.entity_id = 'FP_TH' AND o.city_name IN UNNEST(city_wave_1_th) AND o.created_date = launch_date_wave_1_th THEN 'Transition day'

        -- TH (Wave 1 - Post-period)
        WHEN o.entity_id = 'FP_TH' AND o.city_name IN UNNEST(city_wave_1_th) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_th, INTERVAL 1 DAY) AND DATE_ADD(launch_date_wave_1_th, INTERVAL 7 DAY)) THEN 'WK 1'
        WHEN o.entity_id = 'FP_TH' AND o.city_name IN UNNEST(city_wave_1_th) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_th, INTERVAL 8 DAY) AND DATE_ADD(launch_date_wave_1_th, INTERVAL 14 DAY)) THEN 'WK 2'
        WHEN o.entity_id = 'FP_TH' AND o.city_name IN UNNEST(city_wave_1_th) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_th, INTERVAL 15 DAY) AND DATE_ADD(launch_date_wave_1_th, INTERVAL 21 DAY)) THEN 'WK 3'
        WHEN o.entity_id = 'FP_TH' AND o.city_name IN UNNEST(city_wave_1_th) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_th, INTERVAL 22 DAY) AND DATE_ADD(launch_date_wave_1_th, INTERVAL 28 DAY)) THEN 'WK 4'
        WHEN o.entity_id = 'FP_TH' AND o.city_name IN UNNEST(city_wave_1_th) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_th, INTERVAL 29 DAY) AND DATE_ADD(launch_date_wave_1_th, INTERVAL 35 DAY)) THEN 'WK 5'
        WHEN o.entity_id = 'FP_TH' AND o.city_name IN UNNEST(city_wave_1_th) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_th, INTERVAL 36 DAY) AND DATE_ADD(launch_date_wave_1_th, INTERVAL 42 DAY)) THEN 'WK 6'
        WHEN o.entity_id = 'FP_TH' AND o.city_name IN UNNEST(city_wave_1_th) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_th, INTERVAL 43 DAY) AND DATE_ADD(launch_date_wave_1_th, INTERVAL 49 DAY)) THEN 'WK 7'

        -- TH (Wave 2 - Pre-period)
        WHEN o.entity_id = 'FP_TH' AND o.city_name NOT IN UNNEST(city_wave_2_th) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_2_th, INTERVAL 49 DAY) AND DATE_SUB(launch_date_wave_2_th, INTERVAL 43 DAY)) THEN 'WK 1'
        WHEN o.entity_id = 'FP_TH' AND o.city_name NOT IN UNNEST(city_wave_2_th) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_2_th, INTERVAL 42 DAY) AND DATE_SUB(launch_date_wave_2_th, INTERVAL 36 DAY)) THEN 'WK 2'
        WHEN o.entity_id = 'FP_TH' AND o.city_name NOT IN UNNEST(city_wave_2_th) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_2_th, INTERVAL 35 DAY) AND DATE_SUB(launch_date_wave_2_th, INTERVAL 29 DAY)) THEN 'WK 3'
        WHEN o.entity_id = 'FP_TH' AND o.city_name NOT IN UNNEST(city_wave_2_th) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_2_th, INTERVAL 28 DAY) AND DATE_SUB(launch_date_wave_2_th, INTERVAL 22 DAY)) THEN 'WK 4'
        WHEN o.entity_id = 'FP_TH' AND o.city_name NOT IN UNNEST(city_wave_2_th) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_2_th, INTERVAL 21 DAY) AND DATE_SUB(launch_date_wave_2_th, INTERVAL 15 DAY)) THEN 'WK 5'
        WHEN o.entity_id = 'FP_TH' AND o.city_name NOT IN UNNEST(city_wave_2_th) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_2_th, INTERVAL 14 DAY) AND DATE_SUB(launch_date_wave_2_th, INTERVAL 8 DAY)) THEN 'WK 6'
        WHEN o.entity_id = 'FP_TH' AND o.city_name NOT IN UNNEST(city_wave_2_th) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_2_th, INTERVAL 7  DAY) AND DATE_SUB(launch_date_wave_2_th, INTERVAL 1 DAY)) THEN 'WK 7'
        
        WHEN o.entity_id = 'FP_TH' AND o.city_name NOT IN UNNEST(city_wave_2_th) AND o.created_date = launch_date_wave_2_th THEN 'Transition day'

        -- TH (Wave 2 - Post-period)
        WHEN o.entity_id = 'FP_TH' AND o.city_name NOT IN UNNEST(city_wave_2_th) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_2_th, INTERVAL 1 DAY) AND DATE_ADD(launch_date_wave_2_th, INTERVAL 7 DAY)) THEN 'WK 1'
        WHEN o.entity_id = 'FP_TH' AND o.city_name NOT IN UNNEST(city_wave_2_th) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_2_th, INTERVAL 8 DAY) AND DATE_ADD(launch_date_wave_2_th, INTERVAL 14 DAY)) THEN 'WK 2'
        WHEN o.entity_id = 'FP_TH' AND o.city_name NOT IN UNNEST(city_wave_2_th) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_2_th, INTERVAL 15 DAY) AND DATE_ADD(launch_date_wave_2_th, INTERVAL 21 DAY)) THEN 'WK 3'
        WHEN o.entity_id = 'FP_TH' AND o.city_name NOT IN UNNEST(city_wave_2_th) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_2_th, INTERVAL 22 DAY) AND DATE_ADD(launch_date_wave_2_th, INTERVAL 28 DAY)) THEN 'WK 4'
        WHEN o.entity_id = 'FP_TH' AND o.city_name NOT IN UNNEST(city_wave_2_th) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_2_th, INTERVAL 29 DAY) AND DATE_ADD(launch_date_wave_2_th, INTERVAL 35 DAY)) THEN 'WK 5'
        WHEN o.entity_id = 'FP_TH' AND o.city_name NOT IN UNNEST(city_wave_2_th) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_2_th, INTERVAL 36 DAY) AND DATE_ADD(launch_date_wave_2_th, INTERVAL 42 DAY)) THEN 'WK 6'
        WHEN o.entity_id = 'FP_TH' AND o.city_name NOT IN UNNEST(city_wave_2_th) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_2_th, INTERVAL 43 DAY) AND DATE_ADD(launch_date_wave_2_th, INTERVAL 49 DAY)) THEN 'WK 7'

        -- TW (Wave 1 - Pre-period)
        WHEN o.entity_id = 'FP_TW' AND o.city_name IN UNNEST(city_wave_1_tw) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_tw, INTERVAL 49 DAY) AND DATE_SUB(launch_date_wave_1_tw, INTERVAL 43 DAY)) THEN 'WK 1'
        WHEN o.entity_id = 'FP_TW' AND o.city_name IN UNNEST(city_wave_1_tw) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_tw, INTERVAL 42 DAY) AND DATE_SUB(launch_date_wave_1_tw, INTERVAL 36 DAY)) THEN 'WK 2'
        WHEN o.entity_id = 'FP_TW' AND o.city_name IN UNNEST(city_wave_1_tw) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_tw, INTERVAL 35 DAY) AND DATE_SUB(launch_date_wave_1_tw, INTERVAL 29 DAY)) THEN 'WK 3'
        WHEN o.entity_id = 'FP_TW' AND o.city_name IN UNNEST(city_wave_1_tw) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_tw, INTERVAL 28 DAY) AND DATE_SUB(launch_date_wave_1_tw, INTERVAL 22 DAY)) THEN 'WK 4'
        WHEN o.entity_id = 'FP_TW' AND o.city_name IN UNNEST(city_wave_1_tw) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_tw, INTERVAL 21 DAY) AND DATE_SUB(launch_date_wave_1_tw, INTERVAL 15 DAY)) THEN 'WK 5'
        WHEN o.entity_id = 'FP_TW' AND o.city_name IN UNNEST(city_wave_1_tw) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_tw, INTERVAL 14 DAY) AND DATE_SUB(launch_date_wave_1_tw, INTERVAL 8 DAY)) THEN 'WK 6'
        WHEN o.entity_id = 'FP_TW' AND o.city_name IN UNNEST(city_wave_1_tw) AND (o.created_date BETWEEN DATE_SUB(launch_date_wave_1_tw, INTERVAL 7  DAY) AND DATE_SUB(launch_date_wave_1_tw, INTERVAL 1 DAY)) THEN 'WK 7'
        
        WHEN o.entity_id = 'FP_TW' AND o.city_name IN UNNEST(city_wave_1_tw) AND o.created_date = launch_date_wave_1_tw THEN 'Transition day'

        -- TW (Wave 1 - Post-period)
        WHEN o.entity_id = 'FP_TW' AND o.city_name IN UNNEST(city_wave_1_tw) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_tw, INTERVAL 1 DAY) AND DATE_ADD(launch_date_wave_1_tw, INTERVAL 7 DAY)) THEN 'WK 1'
        WHEN o.entity_id = 'FP_TW' AND o.city_name IN UNNEST(city_wave_1_tw) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_tw, INTERVAL 8 DAY) AND DATE_ADD(launch_date_wave_1_tw, INTERVAL 14 DAY)) THEN 'WK 2'
        WHEN o.entity_id = 'FP_TW' AND o.city_name IN UNNEST(city_wave_1_tw) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_tw, INTERVAL 15 DAY) AND DATE_ADD(launch_date_wave_1_tw, INTERVAL 21 DAY)) THEN 'WK 3'
        WHEN o.entity_id = 'FP_TW' AND o.city_name IN UNNEST(city_wave_1_tw) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_tw, INTERVAL 22 DAY) AND DATE_ADD(launch_date_wave_1_tw, INTERVAL 28 DAY)) THEN 'WK 4'
        WHEN o.entity_id = 'FP_TW' AND o.city_name IN UNNEST(city_wave_1_tw) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_tw, INTERVAL 29 DAY) AND DATE_ADD(launch_date_wave_1_tw, INTERVAL 35 DAY)) THEN 'WK 5'
        WHEN o.entity_id = 'FP_TW' AND o.city_name IN UNNEST(city_wave_1_tw) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_tw, INTERVAL 36 DAY) AND DATE_ADD(launch_date_wave_1_tw, INTERVAL 42 DAY)) THEN 'WK 6'
        WHEN o.entity_id = 'FP_TW' AND o.city_name IN UNNEST(city_wave_1_tw) AND (o.created_date BETWEEN DATE_ADD(launch_date_wave_1_tw, INTERVAL 43 DAY) AND DATE_ADD(launch_date_wave_1_tw, INTERVAL 49 DAY)) THEN 'WK 7'
        
    ELSE 'Period Not Considered'
    END AS week,

    CASE WHEN (o.created_date BETWEEN DATE('2022-02-16') AND DATE('2022-03-02')) THEN 'DPS Flaw Day' ELSE 'Good Day' END AS dps_flaw_days_flag,
    CASE WHEN (o.created_date BETWEEN DATE('2022-02-16') AND DATE('2022-03-02')) AND o.dps_customer_tag != 'New' THEN 'DPS Flaw record' ELSE 'Good record' END AS dps_flaw_records_flag
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` o
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` dwh ON o.entity_id = dwh.global_entity_id AND o.platform_order_code = dwh.order_id
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` pd ON o.entity_id = pd.global_entity_id AND o.platform_order_code = pd.code AND o.created_date = pd.created_date_utc -- Contains info on the orders in Pandora countries
LEFT JOIN delivery_costs cst ON o.entity_id = cst.entity_id AND o.order_id = cst.order_id -- The table that stores the CPO
INNER JOIN geo_data geo ON o.entity_id = geo.entity_id AND o.city_name = geo.city_name AND o.zone_name = geo.zone_name -- INNER JOIN so that we focus ONLY on the entities of interest
INNER JOIN entities ent ON o.entity_id = ent.entity_id -- INNER JOIN to only include active DH entities
WHERE TRUE
    AND o.created_date BETWEEN start_date AND end_date -- HERE. Consider orders between certain start and end dates
    AND o.delivery_status = 'completed' -- HERE. Successful order
    AND o.is_own_delivery -- HERE. OD orders
    AND o.vertical_type = v_type -- HERE. Restaurant order
)

SELECT * FROM raw_orders;

##-----------------------------------------------------------------------END OF RAW DATA SECTION-----------------------------------------------------------------------##

-- Avg number of days after 1st order
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.fdnc_dashboard_days_for_nth_order` AS
WITH all_data AS ( -- Retrieve raw order data from `dh-logistics-product-ops.pricing.fdnc_dashboard_raw_orders`, split by region/entity/city/wave/period/perseus_client_id
    SELECT 
        -- Grouping variables
        region,
        entity_id,
        city_name,
        wave,
        period,
        perseus_client_id, 

        -- Order-level data
        order_id,
        gfv_local,
        gfv_eur,
        order_placed_at AS placed_at,
        created_date,
        week,
        dps_customer_tag,
        ROW_NUMBER() OVER (PARTITION BY region, entity_id, city_name, wave, period, perseus_client_id ORDER BY order_placed_at) AS order_rank -- Partitioned by region, entity_id, city_name, wave, period, and perseus_client_id
    FROM `dh-logistics-product-ops.pricing.fdnc_dashboard_raw_orders`
    WHERE perseus_client_id IS NOT NULL AND perseus_client_id != '' -- Eliminate blank and NULL perseus client IDs
),

new_old_cust AS ( -- Get the perseus client IDs of 'New' customers in the "pre" and "post" periods 
    SELECT *
    FROM all_data
    WHERE (dps_customer_tag = 'New' AND order_rank = 1) OR (dps_customer_tag = 'Existing' AND order_rank = 1) -- Sometimes, dps_customer_tag = 'New' even after the first order, hence why we add the order_rank = 1 constraint (can also be replaced by the customer_status condition)
),

days_for_nth_order AS (
    SELECT 
        a.*,
        b.placed_at AS first_order_placed_at,
        b.period AS first_order_period,
        b.dps_customer_tag AS customer_status_at_first_order,
        ROUND(DATE_DIFF(a.placed_at, b.placed_at, HOUR) / 24, 2) AS days_since_first_order -- We calculate the differences between placed_at and first_order_placed_at in hours, then divide by 24 so that we can calculate the exact no. of days since the 1st order 
    FROM all_data a
    LEFT JOIN new_old_cust b USING (region, entity_id, city_name, wave, period, perseus_client_id) -- These keys ensure that the column first_order_placed_at is only populated for the period where the first order was placed
),

days_in_pre_and_post_periods AS ( -- To get the number of days in each period (Pre vs. Post)
    SELECT 
        region,
        entity_id,
        wave,
        period,
        COUNT(DISTINCT created_date) AS num_days_in_period
    FROM all_data
    GROUP BY 1,2,3,4
)

SELECT 
    a.*,
    b.num_days_in_period 
FROM days_for_nth_order a
LEFT JOIN days_in_pre_and_post_periods b USING (region, entity_id, wave, period)
WHERE a.perseus_client_id IN (SELECT DISTINCT perseus_client_id FROM new_old_cust) -- New customers (dps_customer_tag = 'New' AND order_rank = 1) OR Existing customers (dps_customer_tag = 'Existing' AND order_rank = 1)
ORDER BY region, entity_id, city_name, wave, customer_status_at_first_order, perseus_client_id, period DESC, order_rank;

##-----------------------------------------------------------------------END OF AVG NO. OF DAYS SINCE 1ST ORDER-----------------------------------------------------------------------##

-- Re-order rates
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.fdnc_dashboard_reorder_rates` AS
WITH all_data AS ( -- Retrieve raw order data from `dh-logistics-product-ops.pricing.fdnc_dashboard_raw_orders`, split by region/entity/city/wave/period/perseus_client_id
    SELECT 
        -- Grouping variables
        region,
        entity_id,
        city_name,
        wave,
        period,
        perseus_client_id, 

        -- Order-level data
        order_id,
        gfv_local,
        gfv_eur,
        order_placed_at AS placed_at,
        created_date,
        CONCAT(EXTRACT(YEAR FROM created_date), ' | ', 'CW ', EXTRACT(WEEK FROM created_date)) AS cw_year_order,
        week AS nth_week_in_period,
        dps_customer_tag,
        ROW_NUMBER() OVER (PARTITION BY region, entity_id, city_name, wave, period, perseus_client_id ORDER BY order_placed_at) AS order_rank -- Partitioned by region, entity_id, city_name, wave, period, and perseus_client_id
    FROM `dh-logistics-product-ops.pricing.fdnc_dashboard_raw_orders`
    WHERE perseus_client_id IS NOT NULL AND perseus_client_id != '' -- Eliminate blank and NULL perseus client IDs
),

new_old_cust AS ( -- Get the perseus client IDs of 'New' customers in the "pre" and "post" periods 
    SELECT *
    FROM all_data
    WHERE (dps_customer_tag = 'New' AND order_rank = 1) OR (dps_customer_tag = 'Existing' AND order_rank = 1) -- Sometimes, dps_customer_tag = 'New' even after the first order, hence why we add the order_rank = 1 constraint (can also be replaced by the customer_status condition)
),

first_visit_stg AS ( -- A sub-query that identifies the date on which a customer (New or Existing) placed their first order
    SELECT 
        a.region,
        a.entity_id,
        a.city_name,
        a.wave,
        a.period,
        a.perseus_client_id,
        
        b.placed_at AS first_order_placed_at,
        b.period AS first_order_period,
        b.dps_customer_tag AS customer_status_at_first_order, -- These 3 fields preceded by "b." will contain values only for orders where the perseus_id had an "Existing" or "New" customer tag attached to it.

        MIN(a.placed_at) AS time_stamp_first_order, -- The fields with MIN will have values no matter the customer tag because they come from the original table "all_data" 
        MIN(a.created_date) AS date_first_order,
    FROM all_data a
    LEFT JOIN new_old_cust b USING (region, entity_id, city_name, wave, period, perseus_client_id) -- These keys ensure that the column first_order_placed_at is only populated for the period where the first order was placed
    GROUP BY 1,2,3,4,5,6,7,8,9
),

first_visit_stg_with_cw AS ( -- Add the CW and year to the table above
    SELECT 
        *,
        CONCAT(EXTRACT(YEAR FROM date_first_order), ' | ', 'CW ', EXTRACT(WEEK FROM date_first_order)) AS cw_year_first_order,
        EXTRACT(YEAR FROM date_first_order) AS year_first_order,
        EXTRACT(WEEK FROM date_first_order) AS cw_first_order,
        DENSE_RANK() OVER (PARTITION BY region, entity_id, wave, customer_status_at_first_order, period ORDER BY EXTRACT(YEAR FROM date_first_order), EXTRACT(WEEK FROM date_first_order)) AS cw_year_sorter
    FROM first_visit_stg
),

days_in_pre_and_post_periods AS ( -- To get the number of days in each period (Pre vs. Post)
    SELECT 
        region,
        entity_id,
        wave,
        period,
        COUNT(DISTINCT created_date) AS num_days_in_period
    FROM all_data
    GROUP BY 1,2,3,4
)

SELECT 
    a.*,
    c.num_days_in_period,
    b.customer_status_at_first_order,
    b.time_stamp_first_order,
    b.date_first_order,
    b.cw_year_first_order,
    b.cw_year_sorter,
    b.year_first_order,
    b.cw_first_order,
    ROUND(DATE_DIFF(a.placed_at, b.time_stamp_first_order, HOUR) / 168, 0) AS weeks_since_first_order -- Make sure it is in integer format -- ROUND (0)
FROM all_data a  
LEFT JOIN first_visit_stg_with_cw b USING (region, entity_id, city_name, wave, period, perseus_client_id)
LEFT JOIN days_in_pre_and_post_periods c USING (region, entity_id, wave, period)
-- In the WHERE clause, focus on 'New' and 'Existing' customers only. This is NOT similar to the WHERE a.perseus_client_id IN (SELECT DISTINCT perseus_client_id FROM new_old_cust) condition in the last part of the previous query
-- Here, we filter out NULL dps_customer_tags in the query itself, whereas with the first query, we do that in the pivot. Both options are fine though
WHERE customer_status_at_first_order IS NOT NULL
ORDER BY region, entity_id, city_name, wave, customer_status_at_first_order, perseus_client_id, period DESC, order_rank;

-- Update the re-order rates table to take care of the "2022 | CW 0" issue
UPDATE `dh-logistics-product-ops.pricing.fdnc_dashboard_reorder_rates`
SET cw_year_order = '2022 | CW 1'
WHERE cw_year_order = '2022 | CW 0';

UPDATE `dh-logistics-product-ops.pricing.fdnc_dashboard_reorder_rates`
SET cw_year_first_order = '2022 | CW 1'
WHERE cw_year_first_order = '2022 | CW 0';

UPDATE `dh-logistics-product-ops.pricing.fdnc_dashboard_reorder_rates`
SET cw_first_order = 1
WHERE cw_first_order = 0;