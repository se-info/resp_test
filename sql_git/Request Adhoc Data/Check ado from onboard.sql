with base as 
(SELECT  
       sp.uid as driver_id
      ,DATE(FROM_UNIXTIME(sp.create_time - 3600)) as onboard_date         

FROM shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live sp 

WHERE 1 = 1 
)
,summary as 
(SELECT 
         ado.report_date
        ,ado.uid as driver_id
        ,sm.city_name
        ,CASE WHEN sm.shipper_type_id = 12 then 'Hub' else 'Non Hub' end as driver_type
        ,base.onboard_date 
        ,ado.total_order
        ,SUM(CASE WHEN ado_v2.report_date between base.onboard_date and ado.report_date then ado_v2.total_order else null end) as ado_onboard_to_report

FROM (SELECT 
         date(from_unixtime(dot.real_drop_time - 3600)) as report_date
        ,year(date(from_unixtime(dot.real_drop_time - 3600)))*100 + week(date(from_unixtime(dot.real_drop_time - 3600))) as create_year_week
        ,dot.uid
        ,dl.total_online_seconds/cast(3600 as double) as total_online_time
        ,sum(dot.delivery_distance/cast(1000 as double)) as total_distance
        , count(dot.ref_order_code) as total_order
FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id
        left join shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live dl on dl.uid = dot.uid and date(from_unixtime(dl.report_date - 3600)) = date(from_unixtime(dot.real_drop_time - 3600))
        where dot.order_status = 400
        group by 1,2,3,4
)ado 

LEFT JOIN (SELECT 
         date(from_unixtime(dot.real_drop_time - 3600)) as report_date
        ,dot.uid
        ,sum(dot.delivery_distance/cast(1000 as double)) as total_distance
        , count(dot.ref_order_code) as total_order
FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
        where dot.order_status = 400
        group by 1,2
    )ado_v2 on ado_v2.uid = ado.uid

LEFT JOIN base on ado.uid = base.driver_id

LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = ado.uid and try_cast(sm.grass_date as date) = ado.report_date

GROUP BY 1,2,3,4,5,6
ORDER BY base.onboard_date DESC)

SELECT
        s.report_date
       ,city_name
       ,driver_type  
       ,CASE 
            WHEN ado_onboard_to_report <= 10 then '1. 0 - 10'
            WHEN ado_onboard_to_report <= 15 then '2. 10 - 15'
            WHEN ado_onboard_to_report <= 20 then '3. 15 - 20'
            WHEN ado_onboard_to_report <= 25 then '4. 20 - 25'
            WHEN ado_onboard_to_report <= 30 then '5. 25 - 30'
            WHEN ado_onboard_to_report > 30 then '6. 30 +'
            END AS ado_from_obdate_range
       ,SUM(total_order) AS ado_on_day
       ,SUM(ado_onboard_to_report) AS total_ado_onboard_to_report_date
       ,COUNT(DISTINCT driver_id) as total_driver


FROM summary s 

WHERE 1 = 1 
AND s.report_date BETWEEN current_date - interval '7' day and current_date - interval '1' day

GROUP BY 1,2,3,4
                    