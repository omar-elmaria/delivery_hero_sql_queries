-- Share of vendors on each scheme ID per city
SELECT 
    *,
    ROUND(Num_Vendors/SUM(Num_Vendors) OVER (PARTITION BY entity_id, vertical_type, city_name, city_id, assignment_type), 4) AS PctShare
FROM (
    SELECT
        v.entity_id,
        v.vertical_type,
        dal.city_name,
        dal.city_id,
        dps.scheme_ID,
        dps.scheme_name,
        dps.assignment_type,
        COUNT(DISTINCT v.vendor_code) AS Num_Vendors
    FROM `fulfillment-dwh-production.cl.vendors_v2` v
    LEFT JOIN UNNEST(zones) zn
    LEFT JOIN UNNEST(dps) dps
    LEFT JOIN UNNEST(delivery_areas_location) dal
    WHERE 1=1
        AND v.entity_id = 'FP_TH' --HERE
        AND dal.city_name IN (
            'Bangkok',
            'Nonthaburi',
            'Samut Prakan',
            'Pathum Thani') --HERE
        AND v.vertical_type = 'restaurants'
        AND v.is_active -- Active vendor
    GROUP BY 1,2,3,4,5,6,7
)
GROUP BY 1,2,3,4,5,6,7,8
ORDER BY 7,8 DESC;

-- Share of vendors on each scheme ID per country and assignment type
SELECT 
    *,
    SUM(Num_Vendors) OVER (PARTITION BY entity_id, assignment_type) AS Total_Vendors,
    ROUND(Num_Vendors/SUM(Num_Vendors) OVER (PARTITION BY entity_id, assignment_type), 4) AS PctShare
FROM (
    SELECT
        v.entity_id,
        dps.scheme_ID,
        dps.scheme_name,
        dps.assignment_type,
        COUNT(DISTINCT v.vendor_code) AS Num_Vendors
    FROM `fulfillment-dwh-production.cl.vendors_v2` v
    LEFT JOIN UNNEST(dps) dps
    WHERE 1=1
        AND v.entity_id = 'FP_TH' --HERE
        AND v.vertical_type = 'restaurants'
        AND v.is_active -- Active vendor
    GROUP BY 1,2,3,4
    ORDER BY 4,5 DESC
)
GROUP BY 1,2,3,4,5
ORDER BY 4,5 DESC;

-- Share of vendors on each scheme ID per city
SELECT 
    *,
    SUM(Num_Vendors) OVER (PARTITION BY entity_id) AS Total_Vendors,
    ROUND(Num_Vendors/SUM(Num_Vendors) OVER (PARTITION BY entity_id), 4) AS PctShare
FROM (
    SELECT
        v.entity_id,
        dps.scheme_ID,
        dps.scheme_name,
        COUNT(DISTINCT v.vendor_code) AS Num_Vendors
    FROM `fulfillment-dwh-production.cl.vendors_v2` v
    LEFT JOIN UNNEST(dps) dps
    WHERE 1=1
        AND v.entity_id = 'FP_TH' --HERE
        AND v.vertical_type = 'restaurants'
        AND v.is_active -- Active vendor
    GROUP BY 1,2,3
    ORDER BY 4 DESC
)
GROUP BY 1,2,3,4
ORDER BY 3,4 DESC;

-- Share of vendors on each scheme ID per city
SELECT 
    *,
    SUM(Num_Vendors) OVER (PARTITION BY entity_id) AS Total_Vendors,
    ROUND(Num_Vendors/SUM(Num_Vendors) OVER (PARTITION BY entity_id), 4) AS PctShare
FROM (
    SELECT
        v.entity_id,
        dps.scheme_ID,
        dps.scheme_name,
        COUNT(DISTINCT v.vendor_code) AS Num_Vendors
    FROM `fulfillment-dwh-production.cl.vendors_v2` v
    LEFT JOIN UNNEST(dps) dps
    WHERE 1=1
        AND v.entity_id = 'FP_TH' --HERE
        AND v.vertical_type = 'restaurants'
        AND v.is_active -- Active vendor
    GROUP BY 1,2,3
    ORDER BY 4 DESC
)
GROUP BY 1,2,3,4
ORDER BY 3,4 DESC;

-- Share of vendors on each scheme ID per city
SELECT 
    *,
    ROUND(SUM(PctShare) OVER (ORDER BY Total_Vendors DESC), 4) AS PctShare_Cumulative
FROM (
    SELECT 
        entity_id,
        scheme_name,
        scheme_ID,
        SUM(1) AS Total_Vendors,
        ROUND(SUM(1)/SUM(SUM(1)) OVER (PARTITION BY entity_id), 4) AS PctShare,
    FROM (
        SELECT DISTINCT
            v.entity_id,
            vendor_code,
            name,
            dps.assignment_type,
            dps.scheme_ID,
            dps.scheme_name,
            ROW_NUMBER() OVER (PARTITION BY entity_id, vendor_code, name ORDER BY dps.assignment_type DESC) AS Row_Num_Vendors
        FROM `fulfillment-dwh-production.cl.vendors_v2` v
        LEFT JOIN UNNEST(dps) dps
        LEFT JOIN UNNEST(delivery_areas_location) dal
        WHERE 1=1
            AND v.entity_id = 'FP_TH' --HERE
            AND v.vertical_type = 'restaurants'
            AND v.is_active -- Active vendor
            AND dps.assignment_type != 'Country Fallback'
            AND dal.city_name IN ('Bangkok') --HERE
        ORDER BY 1,2,4 DESC
    )
    WHERE Row_Num_Vendors = 1
    GROUP BY 1,2,3
)
ORDER BY 4 DESC;

-- Laurent's query
SELECT 
    *,
    ROUND(SUM(Share) OVER (ORDER BY Vendor_Count DESC), 4) AS Share_Cumulative
FROM (
    SELECT 
        scheme, 
        COUNT(DISTINCT vendor_code) AS Vendor_Count, 
        ROUND(COUNT(DISTINCT vendor_code) / SUM(COUNT(DISTINCT vendor_code)) OVER (), 4) AS Share,
        SUM(COUNT(DISTINCT vendor_code)) OVER () AS Sum_Vendor 
    FROM `fulfillment-dwh-production.rl.dps_setup_tracker_scheme`
    WHERE entity_id = 'FP_MY' AND is_active AND if_used_assignment_type
    GROUP BY 1
)
ORDER BY 2 DESC
