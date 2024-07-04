with raw_date as 
(SELECT

      DATE(report_date) AS report_date
FROM
    (
(
SELECT sequence(date'2021-12-01',current_date - interval '1' day) bar)

CROSS JOIN
    unnest (bar) as t(report_date)
)
        
)
,params(period_group,period,start_date,end_date,days) as 
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
,driver_order as 
(SELECT
        shipper_id
        , report_date
        , MIN(report_date) OVER (PARTITION BY shipper_id) AS first_date
        , order_uid
        , order_status
        , order_status_mapping
        , is_hub_driver
        , driver_payment_policy
        , city_id
    FROM
        (SELECT dot.uid as shipper_id
              ,dot.ref_order_id as order_id
              ,dot.ref_order_code as order_code
              ,CAST(dot.ref_order_id AS VARCHAR) || '-' || CAST(dot.ref_order_category AS VARCHAR) AS order_uid
              ,dot.ref_order_category
              ,case when dot.ref_order_category = 0 then 'order_delivery'
                    when dot.ref_order_category = 3 then 'now_moto'
                    when dot.ref_order_category = 4 then 'now_ship'
                    when dot.ref_order_category = 5 then 'now_ship'
                    when dot.ref_order_category = 6 then 'now_ship_shopee'
                    when dot.ref_order_category = 7 then 'now_ship_sameday'
                    else null end source
              ,dot.ref_order_status
              ,dot.order_status
              ,case when dot.order_status = 1 then 'Pending'
                    when dot.order_status in (100,101,102) then 'Assigning'
                    when dot.order_status in (200,201,202,203,204) then 'Processing'
                    when dot.order_status in (300,301) then 'Error'
                    when dot.order_status in (400,401,402,403,404,405,406,407) then 'Completed'
                    else null end as order_status_group
             ,case when dot.order_status = 400 then 'Delivered'
                when dot.order_status = 401 then 'Quit'
                when dot.order_status in (402,403,404) then 'Cancelled'
                when dot.order_status in (405,406,407) then 'Others'
                else 'Others' end as order_status_mapping
              ,dot.is_asap
              ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60))
                    when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
                    else date(from_unixtime(dot.submitted_time- 60*60)) end as report_date
              ,date(from_unixtime(dot.submitted_time- 60*60)) created_date
              ,case when dot.real_drop_time = 0 then null else from_unixtime(dot.real_drop_time - 60*60) end as last_delivered_timestamp
            --   ,case when dot.pick_city_id = 238 THEN 'Dien Bien' else city.city_name end as city_name
              ,case when dot.pick_city_id = 217 then 'HCM'
                    when dot.pick_city_id = 218 then 'HN'
                    when dot.pick_city_id = 219 then 'DN'
                    ELSE 'OTH' end as city_group
              ,dot.pick_city_id as city_id
              ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy
              ,case when driver_hub.shipper_type_id = 12 then 1 else 0 end as is_hub_driver
        FROM (select * from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) 
        )dot
        LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id
        LEFT JOIN
            (SELECT  
                sm.shipper_id
                ,sm.shipper_type_id
                ,try_cast(sm.grass_date as date) as report_date
            from shopeefood.foody_mart__profile_shipper_master sm
            where 1=1
            and sm.grass_date != 'current'
            and shipper_type_id <> 3
            and shipper_status_code = 1
            ) driver_hub on driver_hub.shipper_id = dot.uid and driver_hub.report_date = case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60))
                                                                                                 when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
                                                                                                 else date(from_unixtime(dot.submitted_time- 60*60)) end 
        )
)        
, driver_time AS (
SELECT d.*, IF(sm.shipper_type_id = 12, 'Hub', 'Non-hub') AS shipper_type
FROM
    (SELECT
        shipper_id
        , MIN(IF(total_online_time > 0, grass_date, NULL)) OVER (PARTITION BY shipper_id) first_date
        , total_online_time
        , total_working_time
        , grass_date
    FROM
        (SELECT
            shipper_id
            , create_date AS grass_date
            , CAST(DATE_DIFF('second', actual_start_time_online, actual_end_time_online) AS DOUBLE) / 3600 AS total_online_time
            , CAST(DATE_DIFF('second', actual_start_time_work, actual_end_time_work) AS DOUBLE) / 3600 AS total_working_time
        FROM
            (SELECT
                uid AS shipper_id
                ,DATE(from_unixtime(create_time - 3600)) AS create_date
                ,FROM_UNIXTIME(check_in_time - 3600) AS actual_start_time_online
                ,GREATEST(from_unixtime(check_out_time - 3600),from_unixtime(order_end_time - 3600)) AS actual_end_time_online
                ,IF(order_start_time = 0, FROM_UNIXTIME(check_in_time - 3600), FROM_UNIXTIME(order_start_time - 3600)) AS actual_start_time_work
                ,IF(order_end_time = 0, FROM_UNIXTIME(check_in_time - 3600), FROM_UNIXTIME(order_end_time - 3600)) AS actual_end_time_work
                FROM shopeefood.foody_internal_db__shipper_time_sheet_tab__reg_daily_s0_live
                WHERE 1=1
                AND check_in_time > 0
                AND check_out_time > 0
                AND check_out_time >= check_in_time
                AND order_end_time >= order_start_time
                AND ((order_start_time = 0 AND order_end_time = 0)
                    OR (order_start_time > 0 AND order_end_time > 0 AND order_start_time >= check_in_time AND order_start_time <= check_out_time)
                    )
            )
        )
    ) d
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm ON d.shipper_id = sm.shipper_id AND d.grass_date = TRY_CAST(sm.grass_date AS DATE)

WHERE 1 = 1 
AND sm.grass_date != 'current'
)
SELECT
      p.period
    , p.days AS days
    , SUM(sp.total_online_time) / p.days AS supply_time_active_drivers
    , SUM(IF(sp.shipper_type = 'Hub' AND d.shipper_id IS NOT NULL, sp.total_online_time, 0)) / p.days AS supply_time_transacting_hub_drivers
    , SUM(IF(sp.shipper_type = 'Non-hub' AND d.shipper_id IS NOT NULL, sp.total_online_time, 0)) / p.days AS supply_time_transacting_nonhub_drivers
    , SUM(IF(d.shipper_id IS NOT NULL, sp.total_online_time, 0)) / p.days AS supply_time_transacting_drivers
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id = 12 AND sm.city_name IN ('HCM City', 'Ha Noi City'), sp.total_online_time, 0)) / p.days AS supply_time_transacting_drivers_hub_hcm_hn
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (1,6,11), sp.total_online_time, 0)) / p.days AS supply_time_transacting_drivers_t1_hcm_hn
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (2,7,12), sp.total_online_time, 0)) / p.days AS supply_time_transacting_drivers_t2_hcm_hn
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (3,8,13), sp.total_online_time, 0)) / p.days AS supply_time_transacting_drivers_t3_hcm_hn
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (4,9,14), sp.total_online_time, 0)) / p.days AS supply_time_transacting_drivers_t4_hcm_hn
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (5,10,15), sp.total_online_time, 0)) / p.days AS supply_time_transacting_drivers_t5_hcm_hn
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id != 12 AND sm.city_name NOT IN ('HCM City', 'Ha Noi City'), sp.total_online_time, 0)) / p.days AS supply_time_transacting_drivers_dn_oth
    , SUM(IF(d.shipper_id IS NOT NULL, sp.total_working_time, 0)) / p.days AS _onjob_time_transacting_drivers
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id = 12 AND sm.city_name IN ('HCM City', 'Ha Noi City'), sp.total_working_time, 0)) / p.days AS _onjob_time_transacting_drivers_hub_hcm_hn
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (1,6,11), sp.total_working_time, 0)) / p.days AS _onjob_time_transacting_drivers_t1_hcm_hn
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (2,7,12), sp.total_working_time, 0)) / p.days AS _onjob_time_transacting_drivers_t2_hcm_hn
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (3,8,13), sp.total_working_time, 0)) / p.days AS _onjob_time_transacting_drivers_t3_hcm_hn
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (4,9,14), sp.total_working_time, 0)) / p.days AS _onjob_time_transacting_drivers_t4_hcm_hn
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (5,10,15), sp.total_working_time, 0)) / p.days AS _onjob_time_transacting_drivers_t5_hcm_hn
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id != 12 AND sm.city_name NOT IN ('HCM City', 'Ha Noi City'), sp.total_working_time, 0)) / p.days AS _onjob_time_transacting_drivers_dn_oth
FROM driver_time sp
LEFT JOIN ( --transacting drivers
    SELECT
        shipper_id
        , report_date AS grass_date
    FROM driver_order
    where 1=1
    GROUP BY 1,2
        ) d ON sp.shipper_id = d.shipper_id AND sp.grass_date = d.grass_date

INNER JOIN params p ON sp.grass_date BETWEEN p.start_date AND p.end_date

LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm ON sp.shipper_id = sm.shipper_id AND sp.grass_date = TRY_CAST(sm.grass_date AS DATE)

LEFT JOIN shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus ON sp.shipper_id = bonus.uid AND sp.grass_date = DATE(from_unixtime(bonus.report_date - 3600))

WHERE sm.grass_date != 'current'
and p.period_group = '3. Monthly'
GROUP BY 1,2
