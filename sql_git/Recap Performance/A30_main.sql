with raw_date as 
(SELECT

      DATE(report_date) AS report_date
FROM
    (
(
SELECT sequence(date'2022-12-01',date'2023-05-31') bar)

CROSS JOIN
    unnest (bar) as t(report_date)
)
)
,params_date(period_grp,period,start_date,end_date,days) as 
(
SELECT 
        '1. Daily'
        ,CAST(report_date as varchar)
        ,CAST(report_date as date)
        ,CAST(report_date as date)
        ,CAST(1 as double)

from raw_date

UNION 

SELECT 
        '2. Weekly'
        ,'W- ' || CAST(YEAR(date_trunc('week',report_date))*100 + WEEK(date_trunc('week',report_date)) as varchar)
        ,CAST(date_trunc('week',report_date) as date)
        ,max(report_date)
        ,date_diff('day',cast(date_trunc('week',report_date) as date),max(report_date)) + 1

from raw_date

group by 1,2,3

UNION 

SELECT 
        '3. Monthly'
        ,'M- ' || CAST(YEAR(date_trunc('month',report_date))*100 + MONTH(date_trunc('month',report_date)) as varchar)
        ,date_trunc('month',report_date)
        ,max(report_date)
        ,date_diff('day',cast(date_trunc('month',report_date) as date),max(report_date)) + 1

from raw_date

group by 1,2,3
)

SELECT 
    p.period,
    p.period_grp,
    p.days as days,
    shipper_type,
    CASE WHEN city_name in ('HCM City','Ha Noi City','Da Nang City') THEN city_name ELSE 'Other' END AS city_group,
    SUM(a7_shipper_arr)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) a7_shipper_arr,
    SUM(a30_shipper_arr)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) a30_shipper_arr,
    SUM(a1_shipper_arr)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) a1_shipper_arr

FROM (
    SELECT 
        dates.report_date, shipper_type,city_name,
        COUNT(DISTINCT CASE WHEN created_date = dates.report_date THEN uid ELSE NULL END) a1_shipper_arr,
        COUNT(DISTINCT CASE WHEN created_date BETWEEN dates.report_date - INTERVAL '6' DAY AND dates.report_date THEN uid ELSE NULL END) a7_shipper_arr,
        COUNT(DISTINCT CASE WHEN created_date BETWEEN dates.report_date - INTERVAL '29' DAY AND dates.report_date THEN uid ELSE NULL END) a30_shipper_arr

    FROM (

        SELECT  
            DISTINCT DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) created_date, uid, 
            CASE WHEN shipper_type_id = 12 THEN 'hub' ELSE 'non-hub' END as shipper_type,city_name

        FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

        LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm 
            ON dot.uid = sm.shipper_id and DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) = try_cast(sm.grass_date as date)

        WHERE dot.order_status IN (400)       
        AND date(dt) = current_date  - interval '1' day
        AND DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) >= DATE'2022-12-01'
                                                                                
        AND dot.pick_city_id NOT IN (238,469)
        ) 

    CROSS JOIN (SELECT report_date FROM (SELECT SEQUENCE(DATE'2022-12-01',DATE'2023-05-31') seq) CROSS JOIN UNNEST(seq) as t (report_date)) dates
    GROUP BY 1, 2, 3 ) base
    
INNER JOIN params_date p ON report_date BETWEEN p.start_date and p.end_date

WHERE p.period_grp = '3. Monthly'

GROUP BY 1,2,3,4,5