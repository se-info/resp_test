WITH cases_tab AS
(SELECT 
        *

FROM dev_vnfdbi_opsndrivers.driver_ops_support_team_ticket_tab

WHERE
(ticket_category_v2 like '%Withdraw%' OR
ticket_category_v2 like '%Reject withdraw%' )
)
,s AS 
(SELECT  
        ct.*,
        r.city_name AS order_city,
        hp.city_name AS shipper_city,
        hub_locations,
        hub_type_original,
        r.drop_latitude,
        r.drop_longitude,
        t.points,
        ST_Distance(to_spherical_geography(ST_Point(CAST(r.drop_longitude AS DOUBLE),CAST(r.drop_latitude AS DOUBLE)))
                ,to_spherical_geography(ST_Point(cast(CAST(t.points AS ARRAY<JSON>)[2] as double),cast(CAST(t.points AS ARRAY<JSON>)[1] as double)))
                    ) AS "st_distance_meter"
        -- GREAT_CIRCLE_DISTANCE(r.drop_latitude,r.drop_longitude,
        --         CAST(CAST(t.points AS ARRAY<JSON>)[1] AS DOUBLE),CAST(CAST(t.points AS ARRAY<JSON>)[2] AS DOUBLE))*1000 AS "circle_distance_meter"                    

FROM cases_tab ct 

LEFT JOIN driver_ops_hub_driver_performance_tab hp 
        ON hp.uid = ct.driver_id 
        AND ct.created BETWEEN hp.start_shift_time AND hp.end_shift_time

LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live hi ON hi.hub_name = hp.hub_locations

LEFT JOIN driver_ops_raw_order_tab r ON r.order_code = ct.order_code

CROSS JOIN UNNEST (CAST(JSON_EXTRACT(hi.extra_data,'$.geo_data.points') AS ARRAY<JSON>)) AS t(points)


WHERE ct.grass_date BETWEEN CURRENT_DATE - INTERVAL '60' DAY AND CURRENT_DATE - INTERVAL '1' DAY
AND ct.driver_id IS NOT NULL 
AND r.city_name = hp.city_name
AND hp.registered_ = 1
)
SELECT 
        created_date,
        order_city,
        ticket_category_v2,
        CASE 
        WHEN MIN_st_distance_meter_agg <= 1 THEN '1. 0 - 1km'
        WHEN MIN_st_distance_meter_agg <= 1.5 THEN '2. 1 - 1.5km'
        WHEN MIN_st_distance_meter_agg <= 2.5 THEN '3. 1.5 - 2.5km'
        WHEN MIN_st_distance_meter_agg <= 3.5 THEN '4. 2.5 - 3.5km'
        WHEN MIN_st_distance_meter_agg <= 5 THEN '5. 3.5 - 5km'
        WHEN MIN_st_distance_meter_agg > 5 THEN '6. ++5km' END AS "drop_ltlg to border ltlg range",
        COUNT(DISTINCT (case_id,driver_id,created_ts)) AS total_cases

FROM
(SELECT 
        DATE(created) AS created_date,
        created AS created_ts,
        system_type,
        id AS case_id,
        order_city,
        ticket_category_v1,
        ticket_category_v2,
        order_code,
        driver_id,
        hub_locations,
        hub_type_original,
        ARRAY_MIN(ARRAY_AGG(st_distance_meter))/CAST(1000 AS DOUBLE) MIN_st_distance_meter_agg

FROM s 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11)
GROUP BY 1,2,3,4 


