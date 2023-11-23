WITH exclude_dates_filtering_array AS ( -- Generate arrays that are by bounded by each "start_date_exclude" and "end_date_exclude" defined above in the inputs section
    SELECT 
        GENERATE_DATE_ARRAY(DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY), WEEK), INTERVAL 4 WEEK), DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY), WEEK), INTERVAL 1 DAY), INTERVAL 1 DAY) AS exclude_date_array
),

exclude_date_unnested_filtering_array AS ( -- Unnest the arrays to a single column that can be used in the WHERE clause of the "dps_sessions_mapped_to_orders" table
    SELECT
        date_col
    FROM exclude_dates_filtering_array
    LEFT JOIN UNNEST(exclude_date_array) date_col
)

SELECT
    date_col,
    DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY), WEEK), INTERVAL 4 WEEK) AS start_date,
    DATE_DIFF(date_col, DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY), WEEK), INTERVAL 4 WEEK), DAY) + 1 AS diff_in_days_from_start_date,
    CASE WHEN MOD(DATE_DIFF(date_col, DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY), WEEK), INTERVAL 4 WEEK), DAY) + 1, 2) = 0 THEN 'even' ELSE 'odd' END AS even_or_odd_day
FROM exclude_date_unnested_filtering_array
ORDER BY date_col