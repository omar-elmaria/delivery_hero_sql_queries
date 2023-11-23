SELECT
    a.region,
    a.entity_id,
    a.country_code,
    a.* EXCEPT (region, entity_id, country_code),
    DENSE_RANK() OVER (PARTITION BY a.entity_id, a.country_code, a.scheme_id, a.delay_config_id ORDER BY a.tt_threshold) AS tt_tier,
    DENSE_RANK() OVER (PARTITION BY a.entity_id, a.country_code, a.scheme_id, a.delay_config_id, tt_threshold ORDER BY a.delay_threshold) AS delay_tier,
FROM (
    SELECT DISTINCT
        ps.region,
        ps.entity_id,
        ps.country_code,
        ps.scheme_id,
        h.scheme_name,
        TIMESTAMP_TRUNC(h.active_from, SECOND) AS scheme_active_from,
        TIMESTAMP_TRUNC(h.active_to, SECOND) AS scheme_active_to,
        h.delay_config_id,
        dc.name AS config_name,
        TIMESTAMP_TRUNC(dc.active_from, SECOND) AS tt_config_active_from,
        TIMESTAMP_TRUNC(dc.active_to, SECOND) AS tt_config_active_to,
        COALESCE(dd.travel_time_threshold, 9999999) AS tt_threshold,
        CASE
            WHEN dd.travel_time_threshold IS NULL THEN 9999999
            ELSE ROUND(FLOOR(dd.travel_time_threshold) + (dd.travel_time_threshold - FLOOR(dd.travel_time_threshold)) * 60 / 100, 2)
        END AS tt_threshold_in_min_and_sec,
        COALESCE(dd.delay_threshold, 9999999) AS delay_threshold,
        CASE
            WHEN dd.delay_threshold IS NULL THEN 9999999
            ELSE ROUND(FLOOR(dd.delay_threshold) + (dd.delay_threshold - FLOOR(dd.delay_threshold)) * 60 / 100, 2)
        END AS delay_threshold_in_min_and_sec,
        dd.delay_fee AS fee,
        TIMESTAMP_TRUNC(dd.active_from, SECOND) AS delay_detail_active_from,
        TIMESTAMP_TRUNC(dd.active_to, SECOND) AS delay_detail_active_to
    FROM `fulfillment-dwh-production.cl.dps_config_versions` AS ps
    LEFT JOIN UNNEST(price_scheme_history) AS h
    LEFT JOIN UNNEST(delay_history) AS dh
    LEFT JOIN UNNEST(delay_config) AS dc
    LEFT JOIN UNNEST(delay_detail) AS dd
    WHERE TRUE
        AND h.active_to IS NULL
        AND dc.active_to IS NULL
        AND dd.active_to IS NULL
        AND CONCAT(ps.entity_id, " | ", ps.scheme_id) IN ('FP_TH | 2997', 'FP_TH | 1443') 
    QUALIFY
        TIMESTAMP_TRUNC(h.active_from, SECOND) = MAX(TIMESTAMP_TRUNC(h.active_from, SECOND)) OVER (PARTITION BY ps.entity_id, ps.country_code, ps.scheme_id)
        AND TIMESTAMP_TRUNC(dc.active_from, SECOND) = MAX(TIMESTAMP_TRUNC(dc.active_from, SECOND)) OVER (PARTITION BY ps.entity_id, ps.country_code, ps.scheme_id)
-- Don't filter for the latest travel time detail record(s) via another QUALIFY statement because this occasionally removes relevant TT tiers
) AS a
ORDER BY region, entity_id, country_code, scheme_id, tt_tier, delay_tier
;