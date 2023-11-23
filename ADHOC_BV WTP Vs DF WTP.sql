-- Step 1: Declare the input variables used throughout the script
DECLARE entity_var, v_type STRING;
DECLARE start_date, end_date DATE;
SET (entity_var, v_type) = ('FP_MY', 'restaurants'); -- Define the variables of interest
-- SET (start_date, end_date) = (DATE_SUB(CURRENT_DATE(), INTERVAL 182 DAY), CURRENT_DATE()); -- Analysis period
-- SET (start_date, end_date) = (DATE_SUB(DATE('2022-03-10'), INTERVAL 62 DAY), DATE('2022-03-10')); -- Analysis period
SET (start_date, end_date) = (DATE('2022-09-01'), DATE('2022-11-30')); -- Analysis period

##----------------------------------------------------------------------END OF INPUT SECTION----------------------------------------------------------------------##

# Orders data
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.bv_wtp_vs_df_wtp_raw_orders` AS
WITH entities AS (
    SELECT
    ent.region,
    p.entity_id,
    ent.country_iso,
    ent.country_name,
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
        p.entity_id,
        co.country_code,
        ci.name AS city_name,
        ci.id AS city_id,
        zo.shape AS zone_shape, 
        zo.name AS zone_name,
        zo.id AS zone_id
FROM fulfillment-dwh-production.cl.countries co
LEFT JOIN UNNEST(co.platforms) p
LEFT JOIN UNNEST(co.cities) ci
LEFT JOIN UNNEST(ci.zones) zo
WHERE TRUE 
    -- AND entity_id = entity_var -- Comment out if not needed
    -- AND ci.name = city_name_var -- Comment out if not needed
    AND ci.is_active -- Active city
    AND zo.is_active -- Active zone
    AND co.country_code NOT LIKE 'ds-%' -- Eliminate records where country code starts with "ds-" 
    --AND zo.id IN UNNEST(zn_id)
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
        AND p.created_date BETWEEN start_date AND end_date -- For partitioning elimination and speeding up the query
        AND o.created_date BETWEEN start_date AND end_date -- For partitioning elimination and speeding up the query
    GROUP BY 1,2,3
)

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
    o.dps_standard_fee_local, -- The standard fee for the session sent by DPS (Not a component of revenue. It's simply the fee from DBDF setup in DPS)
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
    o.service_fee_local, -- The service fee amount of the session.
    o.gmv_local, -- The gmv (gross merchandise value) of the order placed from backend
    o.gfv_local, -- The gfv (gross food value) of the order placed from backend
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
    ) + o.commission_local + o.joker_vendor_fee_local + COALESCE(o.service_fee_local, 0) + COALESCE(dwh.value.mov_customer_fee_local, IF(o.gfv_local < o.dps_minimum_order_value_local, (o.dps_minimum_order_value_local - o.gfv_local), 0)) AS revenue_local,

    COALESCE(
        pd.delivery_fee_local / o.exchange_rate, 
        IF(o.is_delivery_fee_covered_by_discount = TRUE OR o.is_delivery_fee_covered_by_voucher = TRUE, 0, o.dps_delivery_fee_eur)
    ) + o.commission_eur + o.joker_vendor_fee_eur + COALESCE(o.service_fee_local / o.exchange_rate, 0) + COALESCE(dwh.value.mov_customer_fee_local / o.exchange_rate, IF(o.gfv_local < o.dps_minimum_order_value_local, (o.dps_minimum_order_value_local - o.gfv_local) / o.exchange_rate, 0)) AS revenue_eur,

    COALESCE(
        pd.delivery_fee_local, 
        IF(o.is_delivery_fee_covered_by_discount = TRUE OR o.is_delivery_fee_covered_by_voucher = TRUE, 0, o.dps_delivery_fee_local)
    ) + o.commission_local + o.joker_vendor_fee_local + COALESCE(o.service_fee_local, 0) + COALESCE(dwh.value.mov_customer_fee_local, IF(o.gfv_local < o.dps_minimum_order_value_local, (o.dps_minimum_order_value_local - o.gfv_local), 0)) - cst.delivery_costs_local AS gross_profit_local,

    COALESCE(
        pd.delivery_fee_local / o.exchange_rate, 
        IF(o.is_delivery_fee_covered_by_discount = TRUE OR o.is_delivery_fee_covered_by_voucher = TRUE, 0, o.dps_delivery_fee_eur)
    ) + o.commission_eur + o.joker_vendor_fee_eur + COALESCE(o.service_fee_local / o.exchange_rate, 0) + COALESCE(dwh.value.mov_customer_fee_local / o.exchange_rate, IF(o.gfv_local < o.dps_minimum_order_value_local, (o.dps_minimum_order_value_local - o.gfv_local) / o.exchange_rate, 0)) - cst.delivery_costs_local / o.exchange_rate AS gross_profit_eur,

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
    END AS actual_df_paid_by_customer
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` o
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` dwh ON o.entity_id = dwh.global_entity_id AND o.platform_order_code = dwh.order_id
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` pd ON o.entity_id = pd.global_entity_id AND o.platform_order_code = pd.code AND o.created_date = pd.created_date_utc -- Contains info on the orders in Pandora countries
LEFT JOIN delivery_costs cst ON o.entity_id = cst.entity_id AND o.order_id = cst.order_id -- The table that stores the CPO
INNER JOIN entities ent ON o.entity_id = ent.entity_id -- INNER JOIN to only include active DH entities
INNER JOIN geo_data geo ON o.entity_id = geo.entity_id AND o.city_name = geo.city_name AND o.zone_name = geo.zone_name -- INNER JOIN so that we focus ONLY on the entities of interest (if one chooses to specify them)
WHERE TRUE
    AND o.created_date BETWEEN start_date AND end_date -- Consider orders between certain start and end dates
    AND o.delivery_status = 'completed' -- Successful order
    AND o.is_own_delivery -- OD orders
    AND is_delivery_fee_covered_by_voucher = FALSE -- Eliminate the DF vouchers 
    AND is_delivery_fee_covered_by_discount = FALSE -- Eliminate the DF vouchers
    AND (discount_dh_local = 0 OR discount_dh_local IS NULL) -- Eliminate DF vouchers/BVDs
    AND (discount_other_local = 0 OR discount_other_local IS NULL) -- Eliminate DF vouchers/BVDs
    AND (voucher_dh_local = 0 OR voucher_dh_local IS NULL)  -- Eliminate DF vouchers/BVDs
    AND (voucher_other_local = 0 OR voucher_other_local IS NULL) -- -- Eliminate DF vouchers/BVDs
    AND (joker_customer_discount_local = 0 OR joker_customer_discount_local IS NULL); -- Eliminate Joker discounts
    -- AND o.vertical_type = v_type -- Restaurant order (comment out if not needed)

##----------------------------------------------------------------------END OF RAW ORDERS SECTION----------------------------------------------------------------------##

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.bv_wtp_vs_df_wtp_classification_dataset` AS
WITH order_wtd_afv_and_avg_df_per_customer AS (
    SELECT
        region,
        entity_id,
        vertical_type,
        perseus_client_id,
        SUM(actual_df_paid_by_customer) / COUNT(DISTINCT order_id) AS cust_order_wtd_avg_df,
        SUM(gfv_local) / COUNT(DISTINCT order_id) AS cust_AFV,
        COUNT(order_id) AS cust_order_count
    FROM `dh-logistics-product-ops.pricing.bv_wtp_vs_df_wtp_raw_orders`
    GROUP BY 1,2,3,4
    HAVING COUNT(order_id) >= 5 -- Eliminate customers with less than 5 orders over the last 60 days so that we have enough data points per customer
),

ntiles AS ( -- From all the non-incentivized orders in a specific region + entity + vertical combination
    SELECT 
        region,
        entity_id,
        vertical_type,
        cust_order_wtd_avg_df_percentiles[OFFSET(0)] AS cust_order_wtd_avg_df_p0,
        cust_order_wtd_avg_df_percentiles[OFFSET(25)] AS cust_order_wtd_avg_df_p25,
        cust_order_wtd_avg_df_percentiles[OFFSET(33)] AS cust_order_wtd_avg_df_p33,
        cust_order_wtd_avg_df_percentiles[OFFSET(50)] AS cust_order_wtd_avg_df_p50,
        cust_order_wtd_avg_df_percentiles[OFFSET(66)] AS cust_order_wtd_avg_df_p66,
        cust_order_wtd_avg_df_percentiles[OFFSET(75)] AS cust_order_wtd_avg_df_p75,
        cust_order_wtd_avg_df_percentiles[OFFSET(100)] AS cust_order_wtd_avg_df_p100,

        cust_AFV_percentiles[OFFSET(0)] AS cust_AFV_p0,
        cust_AFV_percentiles[OFFSET(25)] AS cust_AFV_p25,
        cust_AFV_percentiles[OFFSET(33)] AS cust_AFV_p33,
        cust_AFV_percentiles[OFFSET(50)] AS cust_AFV_p50,
        cust_AFV_percentiles[OFFSET(66)] AS cust_AFV_p66,
        cust_AFV_percentiles[OFFSET(75)] AS cust_AFV_p75,
        cust_AFV_percentiles[OFFSET(100)] AS cust_AFV_p100,
    FROM (
        SELECT 
            region,
            entity_id,
            vertical_type,
            APPROX_QUANTILES(cust_order_wtd_avg_df, 100) AS cust_order_wtd_avg_df_percentiles,
            APPROX_QUANTILES(cust_AFV, 100) AS cust_AFV_percentiles
        FROM order_wtd_afv_and_avg_df_per_customer -- Instead of calculating the percentiles from data on the order level, we calculate them from data on the customer level
        GROUP BY 1,2,3
    )
),

df_universe AS ( -- All DF values that were seen and paid by customers per region/entity/vertical
    SELECT
        region,
        entity_id,
        vertical_type,
        actual_df_paid_by_customer
    FROM `dh-logistics-product-ops.pricing.bv_wtp_vs_df_wtp_raw_orders`
),

gfv_universe AS ( -- All GFV values that were seen and paid by customers per region/entity/vertical
    SELECT
        region,
        entity_id,
        vertical_type,
        gfv_local
    FROM `dh-logistics-product-ops.pricing.bv_wtp_vs_df_wtp_raw_orders`
),

ntiles_df_universe AS (
    SELECT 
        region,
        entity_id,
        vertical_type,
        df_universe_percentiles[OFFSET(0)] AS df_universe_p0,
        df_universe_percentiles[OFFSET(25)] AS df_universe_p25,
        df_universe_percentiles[OFFSET(33)] AS df_universe_p33,
        df_universe_percentiles[OFFSET(50)] AS df_universe_p50,
        df_universe_percentiles[OFFSET(66)] AS df_universe_p66,
        df_universe_percentiles[OFFSET(75)] AS df_universe_p75,
        df_universe_percentiles[OFFSET(100)] AS df_universe_p100
    FROM (
        SELECT 
            region,
            entity_id,
            vertical_type,
            APPROX_QUANTILES(actual_df_paid_by_customer, 100) AS df_universe_percentiles
        FROM df_universe
        GROUP BY 1,2,3
    )
),

ntiles_gfv_universe AS (
    SELECT 
        region,
        entity_id,
        vertical_type,
        gfv_universe_percentiles[OFFSET(0)] AS gfv_universe_p0,
        gfv_universe_percentiles[OFFSET(25)] AS gfv_universe_p25,
        gfv_universe_percentiles[OFFSET(33)] AS gfv_universe_p33,
        gfv_universe_percentiles[OFFSET(50)] AS gfv_universe_p50,
        gfv_universe_percentiles[OFFSET(66)] AS gfv_universe_p66,
        gfv_universe_percentiles[OFFSET(75)] AS gfv_universe_p75,
        gfv_universe_percentiles[OFFSET(100)] AS gfv_universe_p100
    FROM (
        SELECT 
            region,
            entity_id,
            vertical_type,
            APPROX_QUANTILES(gfv_local, 100) AS gfv_universe_percentiles
        FROM gfv_universe
        GROUP BY 1,2,3
    )
)

SELECT 
    ord.*,
    rnk.cust_order_wtd_avg_df_p0,
    rnk.cust_order_wtd_avg_df_p25,
    rnk.cust_order_wtd_avg_df_p33,
    rnk.cust_order_wtd_avg_df_p50,
    rnk.cust_order_wtd_avg_df_p66,
    rnk.cust_order_wtd_avg_df_p75,
    rnk.cust_order_wtd_avg_df_p100,
    rnk.cust_AFV_p0,
    rnk.cust_AFV_p25,
    rnk.cust_AFV_p33,
    rnk.cust_AFV_p50,
    rnk.cust_AFV_p66,
    rnk.cust_AFV_p75,
    rnk.cust_AFV_p100,

    undf.df_universe_p0,
    undf.df_universe_p25,
    undf.df_universe_p33,
    undf.df_universe_p50,
    undf.df_universe_p66,
    undf.df_universe_p75,
    undf.df_universe_p100,
    unfv.gfv_universe_p0,
    unfv.gfv_universe_p25,
    unfv.gfv_universe_p33,
    unfv.gfv_universe_p50,
    unfv.gfv_universe_p66,
    unfv.gfv_universe_p75,
    unfv.gfv_universe_p100,

    CASE
        WHEN wtd.cust_order_wtd_avg_df <= rnk.cust_order_wtd_avg_df_p33 THEN 'Low DF WTP'
        WHEN wtd.cust_order_wtd_avg_df > rnk.cust_order_wtd_avg_df_p33 AND wtd.cust_order_wtd_avg_df <= rnk.cust_order_wtd_avg_df_p66 THEN 'Intermediate DF WTP'
        WHEN wtd.cust_order_wtd_avg_df > rnk.cust_order_wtd_avg_df_p66 THEN 'High DF WTP'
    END AS df_wtp,

    CASE
        WHEN wtd.cust_AFV <= rnk.cust_AFV_p33 THEN 'Low BV WTP'
        WHEN wtd.cust_AFV > rnk.cust_AFV_p33 AND wtd.cust_AFV <= rnk.cust_AFV_p66 THEN 'Intermediate BV WTP'
        WHEN wtd.cust_AFV > rnk.cust_AFV_p66 THEN 'High BV WTP'
    END AS gfv_wtp,

    CASE
        WHEN wtd.cust_order_wtd_avg_df <= undf.df_universe_p33 THEN 'Low DF WTP'
        WHEN wtd.cust_order_wtd_avg_df > undf.df_universe_p33 AND wtd.cust_order_wtd_avg_df <= undf.df_universe_p66 THEN 'Intermediate DF WTP'
        WHEN wtd.cust_order_wtd_avg_df > undf.df_universe_p66 THEN 'High DF WTP'
    END AS df_wtp_v2,

    CASE
        WHEN wtd.cust_AFV <= unfv.gfv_universe_p33 THEN 'Low BV WTP'
        WHEN wtd.cust_AFV > unfv.gfv_universe_p33 AND wtd.cust_AFV <= unfv.gfv_universe_p66 THEN 'Intermediate BV WTP'
        WHEN wtd.cust_AFV > unfv.gfv_universe_p66 THEN 'High BV WTP'
    END AS gfv_wtp_v2,

    wtd.cust_order_wtd_avg_df,
    wtd.cust_AFV,
    wtd.cust_order_count
FROM `dh-logistics-product-ops.pricing.bv_wtp_vs_df_wtp_raw_orders` ord
LEFT JOIN ntiles rnk USING (region, entity_id, vertical_type)
LEFT JOIN ntiles_df_universe undf USING (region, entity_id, vertical_type)
LEFT JOIN ntiles_gfv_universe unfv USING (region, entity_id, vertical_type)
INNER JOIN order_wtd_afv_and_avg_df_per_customer wtd USING (region, entity_id, vertical_type, perseus_client_id) -- INNER JOIN so that we eliminate customers with less than 5 orders over the last 60 days
ORDER BY region, entity_id, vertical_type, created_date;

##----------------------------------------------------------------------END OF CUSTOMER-BASED CLASSIFICATION SECTION----------------------------------------------------------------------##


