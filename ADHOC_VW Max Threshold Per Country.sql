-- Declaring Input Variables
DECLARE EntID, CtryCode, VertType STRING;
DECLARE StartDate, EndDate DATE;
DECLARE ZoneIDs ARRAY <INT64>;

SET (EntID, CtryCode, VertType) = ('FP_MY', 'my', 'restaurants');
SET (StartDate, EndDate) = (DATE('2021-08-06'), CURRENT_DATE());
SET ZoneIDs = [362, 363, 610, 253, 254];

-- Create the Numbeo table
DROP TABLE IF EXISTS `dh-logistics-product-ops.pricing.numbeo_data`;
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.numbeo_data`
(Country STRING, Numbeo FLOAT64);

INSERT INTO `dh-logistics-product-ops.pricing.numbeo_data`
VALUES('Kazakhstan', 5.01), 
('Paraguay', 3.71), 
('Portugal', 8), 
('Syria', 4.15), 
('Greece', 10), 
('Latvia', 7), 
('Iran', 3.42), 
('Morocco', 2.85), 
('Panama', 6.42), 
('Guatemala', 4.42), 
('Iraq', 4.28), 
('Chile', 6.53), 
('Nepal', 1.8), 
('Argentina', 5.93), 
('Ukraine', 4.82), 
('Ghana', 3.54), 
('Bahrain', 6.81), 
('India', 1.84), 
('Canada', 11.98), 
('Turkey', 3.51), 
('Belgium', 15), 
('Finland', 12), 
('Taiwan', 3.05), 
('North Macedonia', 3.25), 
('South Africa', 7.31), 
('Jamaica', 3.33), 
('Peru', 2.09), 
('Germany', 10), 
('Puerto Rico', 10.27), 
('Hong Kong', 6.59), 
('United States', 12.84), 
('Thailand', 1.8), 
('Libya', 6.08), 
('Costa Rica', 6.21), 
('Sweden', 9.72), 
('Vietnam', 1.87), 
('Poland', 5.46), 
('Bulgaria', 5.11), 
('Jordan', 4.83), 
('Kuwait', 5.68), 
('Nigeria', 1.45), 
('Tunisia', 1.84), 
('Croatia', 8), 
('Sri Lanka', 1.12), 
('Uruguay', 8.57), 
('United Kingdom', 14), 
('United Arab Emirates', 5.82), 
('Kenya', 3.52), 
('Switzerland', 23.34), 
('Palestine', 6.59), 
('Spain', 11), 
('Lebanon', 9.41), 
('Azerbaijan', 5.03), 
('Czech Republic', 5.48), 
('Israel', 15.8), 
('Australia', 12.22), 
('Estonia', 8.5), 
('Cyprus', 12), 
('Malaysia', 2.02), 
('Iceland', 16.77), 
('Oman', 4.45), 
('Bosnia And Herzegovina', 3.58), 
('Armenia', 4.36), 
('Austria', 12), 
('South Korea', 5.62), 
('El Salvador', 5.99), 
('Brazil', 3.95), 
('Algeria', 1.9), 
('Slovenia', 8.75), 
('Colombia', 2.66), 
('Ecuador', 2.57), 
('Kosovo', 3), 
('Hungary', 5.7), 
('Japan', 7.02), 
('Moldova', 4.87), 
('Belarus', 6.8), 
('Albania', 4.53), 
('Trinidad And Tobago', 6.3), 
('New Zealand', 11.7), 
('Honduras', 4.28), 
('Italy', 15), 
('Ethiopia', 2.57), 
('Singapore', 9.41), 
('Egypt', 3.27), 
('Bolivia', 2.48), 
('Malta', 15), 
('Russia', 6.93), 
('Saudi Arabia', 4.56), 
('Netherlands', 15), 
('Pakistan', 1.43), 
('China', 2.63), 
('Ireland', 15), 
('Qatar', 6.94), 
('Slovakia', 5.5), 
('France', 14), 
('Lithuania', 7), 
('Serbia', 5.11), 
('Romania', 5.07), 
('Philippines', 2.55), 
('Uzbekistan', 3.42), 
('Bangladesh', 1.51), 
('Norway', 17.03), 
('Denmark', 17.48), 
('Dominican Republic', 4.51), 
('Mexico', 5.09), 
('Zimbabwe', 5.13), 
('Montenegro', 5), 
('Indonesia', 1.48);

SELECT 
    *,
    ROUND(Max_VW_based_on_halfAvgGFV_Rounded * Inv_of_Avg_Fx_Rate, 2) AS Max_VW_based_on_halfAvgGFV_inEUR, 
    CURRENT_DATE() AS DateStamp
FROM(
    SELECT
        *,
        -- Max VW threshold allowed
        CASE 
            WHEN Avg_GFV_50pct < 10 THEN ROUND(Avg_GFV_50pct ,0) -- Round to the nearest whole number
            WHEN Avg_GFV_50pct < 100 THEN ROUND(Avg_GFV_50pct / 10, 0) * 10 -- Round to the nearest 10  
            WHEN Avg_GFV_50pct < 1000 THEN ROUND(Avg_GFV_50pct / 100, 0) * 100 -- Round to nearest 100
            WHEN Avg_GFV_50pct < 10000 THEN ROUND(Avg_GFV_50pct / 500, 0) * 500 -- Round to nearest 500
            ELSE ROUND(Avg_GFV_50pct / 1000, 0) * 1000 -- Round to nearest 1000 
        END AS Max_VW_based_on_halfAvgGFV_Rounded,  
    FROM (
        SELECT 
            od.global_entity_id,
            con.region,
            con.country_name,
            od.currency_code,
            ROUND(AVG(od.fx_rate_eur), 4) AS Avg_Fx_Rate,
            ROUND(1 / AVG(od.fx_rate_eur), 4) AS Inv_of_Avg_Fx_Rate, 
            ROUND(AVG(value.delivery_fee_local), 2) AS Avg_DF,
            ROUND(AVG(value.gbv_local), 2) AS Avg_GFV,
            COUNT(DISTINCT order_id) AS Count_DPS_Orders,
            ROUND(num.Numbeo * AVG(od.fx_rate_eur), 2) AS Numbeo_local,
            3 * ROUND(AVG(value.delivery_fee_local), 2) AS Avg_DF_3x,
            0.5 * ROUND(AVG(value.gbv_local), 2) AS Avg_GFV_50pct,
            ROUND(0.5 * (num.Numbeo * AVG(od.fx_rate_eur)), 2) AS Numbeo_local_50pct
        FROM `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` od
        INNER JOIN `fulfillment-dwh-production.cl.countries` con ON LOWER(RIGHT(od.global_entity_id, 2)) = LOWER(con.country_iso)
        LEFT JOIN `dh-logistics-product-ops.pricing.numbeo_data` num ON num.Country = con.country_name
        WHERE 1=1
            AND CAST(od.placed_at_local AS DATE) BETWEEN (CURRENT_DATE() - 30) AND CURRENT_DATE() --HERE (WORKS)
            AND is_sent -- Successfull delivery
            AND is_own_delivery -- Own delivery
            AND con.region = 'Asia'
            AND con.country_name != 'South Korea'
        GROUP BY 1,2,3,4, num.Numbeo 
    )
)
ORDER BY 2,3
