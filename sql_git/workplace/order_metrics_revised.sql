with raw_date as 
(SELECT

      DATE(report_date) AS report_date
FROM
    (
(
SELECT sequence(date_trunc('month',current_date) - interval '2' month,current_date - interval '1' day) bar
)

CROSS JOIN
    unnest (bar) as t(report_date)
)
)
,params_date(period_group,period_,start_date,end_date,days) as 
(
SELECT 
        '1. Daily'
        ,CAST(report_date as varchar)
        ,report_date
        ,report_date
        ,1

from raw_date

UNION ALL 
SELECT 
        '2. Weekly'
        ,'W- ' || CAST(YEAR(date_trunc('week',report_date))*100 + WEEK(date_trunc('week',report_date)) as varchar)
        ,date_trunc('week',report_date) 
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
,raw AS 
(SELECT 
        raw.*,
        CASE 
        WHEN raw.source = 'now_ship_shopee' 
             THEN 
             (CASE 
             WHEN raw.order_status = 'Assigning Timeout' THEN 1 ELSE 0 END)
             WHEN raw.source in ('now_ship_user','now_ship_merchant') THEN 
                (CASE WHEN raw.last_incharge_timestamp is null and sa.assigning_count > 0 
                     and raw.cancel_reason in ('No Reason','Wait for assigning driver too long','Unable to contact driver','Shipper denied Auto Accept order','Mistaken accept') 
                     THEN 1 ELSE 0 END)
            WHEN raw.source in ('now_ship_same_day') THEN 
                (CASE WHEN raw.last_incharge_timestamp is null 
                and raw.cancel_reason in ('No Reason','Wait for assigning driver too long','Unable to contact driver','Shipper denied Auto Accept order','Mistaken accept') THEN 1 ELSE 0 END)
            WHEN raw.source in ('order_food','order_fresh','order_market') THEN 
                (CASE WHEN raw.cancel_reason = 'No driver' THEN 1 ELSE 0 END )
        ELSE 0 END AS is_no_driver,
        IF(order_status = 'Delivered',1,0) AS is_del,
        IF(order_status = 'Cancelled',1,0) AS is_cancel,
        IF(order_status = 'Quit',1,0) AS is_quit,
        CASE 
        WHEN created_timestamp <= last_incharge_timestamp AND is_asap = 1 THEN 1 ELSE 0 END AS is_valid_incharged,
        CASE 
        WHEN created_timestamp <= delivered_timestamp AND is_asap = 1 THEN 1 ELSE 0 END AS is_valid_completed,

        DATE_DIFF('second',created_timestamp,last_incharge_timestamp)*1.00/60 AS lt_incharged,
        DATE_DIFF('second',created_timestamp,delivered_timestamp)*1.00/60 AS lt_completion,
        CASE 
        WHEN group_id > 0 AND order_assign_type = 'Group' THEN 'group'
        WHEN group_id > 0 AND order_assign_type != 'Group' THEN 'stack'
        WHEN assign_type = '4. Free Pick' THEN 'freepick'
        WHEN assign_type = '5. Manual' THEN 'manual'
        ELSE 'single' END AS assign_type,
        ca.is_auto_accepted,
        ca.is_ca,
        CASE 
        WHEN peak_mode_name = 'Peak 3 Mode' AND order_type = 0 THEN 1 ELSE 0 END AS is_peak3,
        if(raw.order_type != 0,1,coalesce(is_foody_delivery,0)) as filter_delivery,
        CASE 
        WHEN order_type = 0 THEN 'delivery' ELSE 'spxi' END AS sub_source,
        CASE 
        WHEN city_name IN 
        ('HCM City',
        'Ha Noi City',
        'Da Nang City',
        'Dong Nai',
        'Can Tho City',
        'Binh Duong',
        'Hai Phong City',
        'Hue City',
        'Vung Tau',
        'Khanh Hoa') THEN city_name 
        WHEN city_name IN 
        ('Bac Ninh',
        'Nghe An',
        'Thai Nguyen',
        'Quang Ninh',
        'Lam Dong',
        'Quang Nam') THEN 'T3'
        ELSE 'new_cities' END AS cities

FROM driver_ops_raw_order_tab raw 

LEFT JOIN 
(select 
        id,
        is_foody_delivery 
FROM shopeefood.shopeefood_mart_dwd_vn_order_completed_da 
WHERE DATE(dt) = CURRENT_DATE - INTERVAL '1' DAY
) oct ON raw.id = oct.id

LEFT JOIN 
(SELECT 
         ref_order_id
        ,order_category
        ,COUNT(ref_order_id) AS assigning_count
FROM dev_vnfdbi_opsndrivers.driver_ops_order_assign_log_tab
GROUP BY 1,2
) sa ON sa.ref_order_id = raw.id AND sa.order_category = raw.order_type

LEFT JOIN 
(SELECT 
        ref_order_id,
        order_category,
        MAX_BY(is_auto_accepted,create_time) AS is_auto_accepted,
        MAX_BY(is_ca,create_time) AS is_ca

FROM (SELECT 
            ref_order_id,
            order_category,
            case when sa.experiment_group in (3,4,7,8) then 1 else 0 end as is_auto_accepted,
            case when sa.experiment_group in (5,6,7,8) then 1 else 0 end as is_ca,
            create_time
FROM dev_vnfdbi_opsndrivers.driver_ops_order_assign_log_tab sa 
)
GROUP BY 1,2
) ca ON ca.ref_order_id = raw.id AND ca.order_category = raw.order_type

WHERE 1 = 1 
)
SELECT 
        p.period_,
        COALESCE(raw.cities,'VN') AS cities,
        -- raw.sub_source,
        COALESCE(TRY(COUNT(DISTINCT order_code)),0)*1.0000/COUNT(DISTINCT created_date) AS gross_order,
        COALESCE(TRY(COUNT(DISTINCT CASE WHEN is_del = 1 THEN order_code ELSE NULL END)),0)*1.0000/COUNT(DISTINCT created_date) AS net_order,
        COALESCE(TRY(COUNT(DISTINCT CASE WHEN is_cancel = 1 AND is_no_driver = 1 THEN order_code ELSE NULL END)),0)*1.0000/COUNT(DISTINCT created_date) AS cnd_order,
        COALESCE(TRY(COUNT(DISTINCT CASE WHEN is_peak3 = 1 AND is_del = 1 THEN order_code ELSE NULL END)*1.0000
        /COUNT(DISTINCT CASE WHEN is_del = 1 THEN order_code ELSE NULL END)),0) AS pp_peak3,
        COALESCE(TRY(SUM(CASE WHEN sub_source = 'delivery' AND is_valid_incharged = 1 AND is_del = 1 THEN lt_incharged ELSE NULL END)*1.0000/
            COUNT(DISTINCT CASE WHEN sub_source = 'delivery' AND is_valid_incharged = 1 AND is_del = 1 THEN order_code ELSE NULL END)),0) AS avg_incharged,
        COALESCE(TRY(SUM(CASE WHEN sub_source = 'delivery' AND is_valid_completed = 1 AND is_del = 1 THEN lt_completion ELSE NULL END)*1.0000/
            COUNT(DISTINCT CASE WHEN sub_source = 'delivery' AND is_valid_completed = 1 AND is_del = 1 THEN order_code ELSE NULL END)),0) AS avg_e2e,
        COALESCE(TRY(APPROX_PERCENTILE(CASE WHEN sub_source = 'delivery' AND is_valid_incharged = 1 AND is_del = 1 THEN lt_incharged ELSE NULL END,0.95)),0) AS pct95th_incharged, 
        COALESCE(TRY(APPROX_PERCENTILE(CASE WHEN sub_source = 'delivery' AND is_valid_completed = 1 AND is_del = 1 THEN lt_completion ELSE NULL END,0.95)),0) AS pct95th_completion, 
        COUNT(DISTINCT CASE WHEN is_del = 1 THEN (shipper_id,created_date) ELSE NULL END)*1.0000/COUNT(DISTINCT created_date) AS a1,
        COUNT(DISTINCT CASE WHEN is_del = 1 AND order_type = 0 THEN (shipper_id,created_date) ELSE NULL END)*1.0000/COUNT(DISTINCT created_date) AS a1_delivery,
        COUNT(DISTINCT CASE WHEN is_del = 1 AND order_type = 6 THEN (shipper_id,created_date) ELSE NULL END)*1.0000/COUNT(DISTINCT created_date) AS a1_e2c,
        COUNT(DISTINCT CASE WHEN is_del = 1 AND order_type NOT IN (0,6) THEN (shipper_id,created_date) ELSE NULL END)*1.0000/COUNT(DISTINCT created_date) AS a1_c2c



FROM raw 

INNER JOIN params_date p ON raw.created_date BETWEEN p.start_date AND p.end_date

WHERE filter_delivery = 1
AND REGEXP_LIKE(COALESCE(city_name,'n/a'),'n/a|Dien Bien|Test') = False
GROUP BY 1,GROUPING SETS(raw.cities,())