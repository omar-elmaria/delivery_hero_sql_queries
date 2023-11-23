-- Using the dps_sessions_mapped_to_orders_v2 table
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.surge_mov_my_mov_pct_afv` AS 
WITH temptbl AS (
    SELECT
        region,
        entity_id,
        dps_minimum_order_value_local / NULLIF(gfv_local, 0) AS MOV_Pct_of_AFV,
        ROUND(PERCENTILE_CONT(dps_minimum_order_value_local / NULLIF(gfv_local, 0), 0.5) OVER(PARTITION BY region, entity_id), 3) AS Median_MOV_Pct_of_AFV
    FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` o
    WHERE TRUE
        AND region IN ('Asia', 'Europe') -- Consider orders only in the entity of choice
        AND created_date BETWEEN DATE('2022-01-22') AND DATE('2022-02-20') -- Consider orders between certain start and end dates
        AND delivery_status = 'completed' -- Successful order
        AND is_commissionable -- Order qualifies for commission
        AND is_own_delivery -- OD orders
        AND vertical_type = 'restaurants' -- Restaurant order
)

SELECT
    region,
    entity_id,
    ROUND(AVG(MOV_Pct_of_AFV), 3) AS Avg_MOV_Pct_of_AFV,
    ROUND(AVG(Median_MOV_Pct_of_AFV), 3) AS Median_MOV_Pct_of_AFV,
FROM temptbl
GROUP BY 1,2
ORDER BY 1,3;

-----------------------------------------------------------------------END OF PART 1-----------------------------------------------------------------------

-- Using the central DWH orders table
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.surge_mov_my_mov_pct_afv` AS 
WITH entities AS (
    SELECT
        region,
        p.entity_id
    FROM `fulfillment-dwh-production.cl.entities`
    LEFT JOIN UNNEST(platforms) p
    WHERE TRUE 
        AND p.entity_id NOT LIKE 'ODR%' -- Eliminate entities starting with ODR (on-demand riders) as they are not part of DPS
        AND p.entity_id = 'FP_SG'
        AND region IN ('Asia', 'Europe')
    ORDER BY 1,2
),

temptbl AS (
    SELECT
        ent.region,
        o.global_entity_id AS entity_id,
        o.value.mov_local,
        o.value.gbv_local,
        o.value.mov_local / NULLIF(o.value.gbv_local, 0) AS MOV_Pct_of_AFV,
        ROUND(PERCENTILE_CONT(o.value.mov_local / NULLIF(o.value.gbv_local, 0), 0.5) OVER(PARTITION BY ent.region, o.global_entity_id), 3) AS Median_MOV_Pct_of_AFV
    FROM `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` o
    INNER JOIN entities ent ON o.global_entity_id = ent.entity_id -- Consider orders only in the regions of choice
    WHERE TRUE
        AND DATE(placed_at) BETWEEN DATE('2021-09-01') AND DATE('2021-10-31') -- Consider orders between certain start and end dates
        AND is_sent -- Successful order
        AND is_commissionable -- Order qualifies for commission
        AND is_own_delivery -- OD orders
        AND is_qcommerce = FALSE -- Restaurant order
)

SELECT
    region,
    entity_id,
    ROUND(AVG(MOV_Pct_of_AFV), 3) AS Avg_MOV_Pct_of_AFV,
    ROUND(AVG(Median_MOV_Pct_of_AFV), 3) AS Median_MOV_Pct_of_AFV,
FROM temptbl
GROUP BY 1,2
ORDER BY 1,2