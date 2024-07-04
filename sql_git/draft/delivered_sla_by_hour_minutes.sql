WITH  base AS (
SELECT dot.uid as shipper_id
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

      ,dot.is_asap

      ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 3600))
            when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 3600))
            else date(from_unixtime(dot.submitted_time- 3600)) end as report_date
      ,date(from_unixtime(dot.submitted_time- 3600)) created_date
      ,if(dot.is_asap = 0, fa.last_auto_assign_timestamp, from_unixtime(dot.submitted_time- 3600)) as inflow_timestamp

      ,case when dot.real_drop_time = 0 then null else from_unixtime(dot.real_drop_time - 3600) end as last_delivered_timestamp

      ,case when dot.pick_city_id = 217 then 'HCM'
            when dot.pick_city_id = 218 then 'HN'
            when dot.pick_city_id = 219 then 'DN'
            when dot.pick_city_id = 220 then 'HP'
            ELSE 'OTH' end as city_group
      ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy
FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id
LEFT JOIN
        (
        SELECT   order_id , 0 as order_type
                ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
                ,max(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_auto_assign_timestamp
                ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
                ,max(case when status = 6 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp
                from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
                where 1=1
                and grass_schema = 'foody_order_db'
                group by 1,2

        UNION ALL

        SELECT   ns.order_id, ns.order_type
                ,min(from_unixtime(create_time - 3600)) first_auto_assign_timestamp
                ,max(from_unixtime(create_time - 3600)) last_auto_assign_timestamp
                ,max(case when status in (3,4) then cast(from_unixtime(update_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
                ,max(case when status in (3,4) then cast(from_unixtime(update_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp
        FROM
                ( SELECT order_id, order_type , create_time , update_time, status

                 from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                 where order_type in (4,5,6,7)
                 and grass_schema = 'foody_partner_archive_db'
                 UNION

                 SELECT order_id, order_type, create_time , update_time, status

                 from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                 where order_type in (4,5,6,7)
                 and schema = 'foody_partner_db'
                 ) ns
        GROUP BY 1,2
        ) fa on dot.ref_order_id = fa.order_id and dot.ref_order_category = fa.order_type
WHERE dot.order_status = 400
)
SELECT date_ 
      ,shipper_id 
      ,shipper_name
      ,city_name 
      ,is_hub
      ,nowfood_orders_1030am_1230pm
      ,nowfood_orders_1730pm_19pm
      ,case when sla_1030_1230 >= 99 and date_ > date'2022-02-08' then coalesce(try(5000 * nowfood_orders_1030am_1230pm),0) else 0 end as bonus_1030_1230
      ,case when sla_1730_1900 >= 99 then coalesce(try(5000 * nowfood_orders_1730pm_19pm),0) else 0 end as bonus_1730_1900
      
FROM       
(SELECT
     date(base.last_delivered_timestamp) as date_     
    ,base.shipper_id
    , sm.shipper_name
    , base.city_group AS city_name
    , IF(sm.shipper_type_id = 11, 'Non-hub', 'Hub') AS is_hub
    ,coalesce(try((cnt_total_incharge_order_1030_1230*1.000/cnt_total_assign_order_1030_1230)*100),0) as sla_1030_1230
    ,coalesce(try((cnt_total_incharge_order_1730_1900*1.000/cnt_total_assign_order_1730_1900)*100),0) as sla_1730_1900
    , COUNT(DISTINCT IF(HOUR(base.last_delivered_timestamp)*100+ MINUTE(base.last_delivered_timestamp) >= 1030 AND HOUR(base.last_delivered_timestamp)*100+ MINUTE(base.last_delivered_timestamp) <= 1230 AND base.source = 'order_delivery', base.order_uid, NULL)) AS nowfood_orders_1030am_1230pm
    , COUNT(DISTINCT IF(HOUR(base.last_delivered_timestamp)*100+ MINUTE(base.last_delivered_timestamp) >= 1730 AND HOUR(base.last_delivered_timestamp)*100+ MINUTE(base.last_delivered_timestamp) <= 1900 AND base.source = 'order_delivery', base.order_uid, NULL)) AS nowfood_orders_1730pm_19pm

    


FROM base
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm ON base.shipper_id = sm.shipper_id AND TRY_CAST(sm.grass_date AS DATE) = date(base.last_delivered_timestamp)

---sla 
LEFT JOIN (SELECT
  assign.date_ AS report_date
, assign.shipper_id
, sm.city_name
---1030 - 1230 
, COALESCE(assign.cnt_total_assign_order_1030_1230,0) + COALESCE(deny.cnt_total_deny_1030_1230,0) AS cnt_total_assign_order_1030_1230
, COALESCE(assign.cnt_total_incharge_order_1030_1230,0) AS cnt_total_incharge_order_1030_1230
, COALESCE(assign.cnt_total_ignore_order_1030_1230,0) AS cnt_total_ignore_order_1030_1230
, COALESCE(deny.cnt_total_deny_1030_1230,0) AS cnt_total_deny_1030_1230
---1730 - 1900
, COALESCE(assign.cnt_total_assign_order_1730_1900,0) + COALESCE(deny.cnt_total_deny_1730_1900,0) AS cnt_total_assign_order_1730_1900
, COALESCE(assign.cnt_total_incharge_order_1730_1900,0) AS cnt_total_incharge_order_1730_1900
, COALESCE(assign.cnt_total_ignore_order_1730_1900,0) AS cnt_total_ignore_order_1730_1900
, COALESCE(deny.cnt_total_deny_1730_1900,0) AS cnt_total_deny_1730_1900


FROM
(SELECT
date_
, shipper_id
---10h30 - 12h30
, COUNT(DISTINCT IF(HOUR(a.create_time)*100+ MINUTE(a.create_time) >= 1030 AND HOUR(a.create_time)*100+ MINUTE(a.create_time) <= 1230 , a.order_uid, NULL)) AS cnt_total_assign_order_1030_1230
, COUNT(DISTINCT IF(HOUR(a.create_time)*100+ MINUTE(a.create_time) >= 1030 AND HOUR(a.create_time)*100+ MINUTE(a.create_time) <= 1230 and status IN (3,4), a.order_uid, NULL)) AS cnt_total_incharge_order_1030_1230
, COUNT(DISTINCT IF(HOUR(a.create_time)*100+ MINUTE(a.create_time) >= 1030 AND HOUR(a.create_time)*100+ MINUTE(a.create_time) <= 1230 and status IN (8,9), a.order_uid, NULL)) AS cnt_total_ignore_order_1030_1230
---17h30 - 19h
, COUNT(DISTINCT IF(HOUR(a.create_time)*100+ MINUTE(a.create_time) >= 1730 AND HOUR(a.create_time)*100+ MINUTE(a.create_time) <= 1900 , a.order_uid, NULL)) AS cnt_total_assign_order_1730_1900
, COUNT(DISTINCT IF(HOUR(a.create_time)*100+ MINUTE(a.create_time) >= 1730 AND HOUR(a.create_time)*100+ MINUTE(a.create_time) <= 1900 and status IN (3,4), a.order_uid, NULL)) AS cnt_total_incharge_order_1730_1900
, COUNT(DISTINCT IF(HOUR(a.create_time)*100+ MINUTE(a.create_time) >= 1730 AND HOUR(a.create_time)*100+ MINUTE(a.create_time) <= 1900 and status IN (8,9), a.order_uid, NULL)) AS cnt_total_ignore_order_1730_1900

FROM
(SELECT
a.shipper_id
, a.order_uid
, a.order_id
, CASE
WHEN a.order_type = 0 THEN '1. Food/Market'
WHEN a.order_type in (4,5) THEN '2. NS'
WHEN a.order_type = 6 THEN '3. NSS'
WHEN a.order_type = 7 THEN '4. NS Same Day'
ELSE 'Others' END AS order_type
, a.order_type AS order_code
,CASE
WHEN a.assign_type = 1 THEN '1. Single Assign'
WHEN a.assign_type in (2,4) THEN '2. Multi Assign'
WHEN a.assign_type = 3 THEN '3. Well-Stack Assign'
WHEN a.assign_type = 5 THEN '4. Free Pick'
WHEN a.assign_type = 6 THEN '5. Manual'
WHEN a.assign_type in (7,8) THEN '6. New Stack Assign'
ELSE NULL END AS assign_type
, DATE(FROM_UNIXTIME(a.create_time - 3600)) AS date_
, FROM_UNIXTIME(a.create_time - 3600) as create_time 
, a.status
, IF(a.experiment_group IN (3,4,7,8), 1, 0) AS is_auto_accepted
FROM
(SELECT
CONCAT(CAST(order_id AS VARCHAR), '-', CAST(order_type AS VARCHAR)) AS order_uid
, order_id, city_id, assign_type, update_time, create_time, status, order_type
, experiment_group, shipper_uid AS shipper_id

FROM shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
WHERE status IN (3,4,8,9,17,18) -- shipper incharge + deny + ignore
AND DATE(FROM_UNIXTIME(create_time - 3600)) BETWEEN DATE'2022-02-08' AND DATE'2022-02-10'
UNION ALL

SELECT
CONCAT(CAST(order_id as VARCHAR), '-', CAST(order_type AS VARCHAR)) AS order_uid
, order_id, city_id, assign_type, update_time, create_time, status, order_type
, experiment_group, shipper_uid AS shipper_id

FROM shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
WHERE status IN (3,4,8,9,17,18) -- shipper incharge + deny + ignore
AND DATE(FROM_UNIXTIME(create_time - 3600)) BETWEEN DATE'2022-02-08' AND DATE'2022-02-10'
) a
) a
GROUP BY 1,2
) assign

LEFT JOIN

(SELECT
deny_date
, shipper_id
, COUNT(ref_order_code) AS cnt_deny_total
, COUNT(DISTINCT IF(HOUR(a.create_time)*100+ MINUTE(a.create_time) >= 1030 AND HOUR(a.create_time)*100+ MINUTE(a.create_time) <= 1230 and deny_type = 'Driver_Fault' , a.order_uid, NULL)) AS cnt_total_deny_1030_1230
---17h30 - 19h
, COUNT(DISTINCT IF(HOUR(a.create_time)*100+ MINUTE(a.create_time) >= 1730 AND HOUR(a.create_time)*100+ MINUTE(a.create_time) <= 1900 and deny_type = 'Driver_Fault' , a.order_uid, NULL)) AS cnt_total_deny_1730_1900
FROM
(SELECT
dod.uid AS shipper_id
, DATE(FROM_UNIXTIME(dod.create_time - 3600)) AS deny_date
,FROM_UNIXTIME(dod.create_time - 3600) create_time
, concat(cast(dot.ref_order_id as VARCHAR),'-',cast(dot.ref_order_category as VARCHAR)) as order_uid
, dot.ref_order_id
, dot.ref_order_code
, dot.ref_order_category
, CASE
WHEN dot.ref_order_category = 0 THEN 'Food/Market'
WHEN dot.ref_order_category = 4 THEN 'NS Instant'
WHEN dot.ref_order_category = 5 THEN 'NS Food Mex'
WHEN dot.ref_order_category = 6 THEN 'NS Shopee'
WHEN dot.ref_order_category = 7 THEN 'NS Same Day'
WHEN dot.ref_order_category = 8 THEN 'NS Multi Drop'
ELSE NULL END AS order_source
, CASE
WHEN dod.deny_type = 0 THEN 'NA'
WHEN dod.deny_type = 1 THEN 'Driver_Fault'
WHEN dod.deny_type = 10 THEN 'Order_Fault'
WHEN dod.deny_type = 11 THEN 'Order_Pending'
WHEN dod.deny_type = 20 THEN 'System_Fault'
END AS deny_type
, reason_text

FROM shopeefood.foody_partner_db__driver_order_deny_log_tab__reg_daily_s0_live dod
LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on dod.order_id = dot.id
WHERE DATE(FROM_UNIXTIME(dod.create_time - 3600)) BETWEEN DATE'2022-02-08' AND DATE'2022-02-10'
) a
group by 1,2
) deny on assign.date_ = deny.deny_date AND assign.shipper_id = deny.shipper_id
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = assign.shipper_id and try_cast(grass_date as date) = assign.date_  
where 1 = 1
and sm.city_id = 218
and sm.shipper_type_id = 11) sla on sla.shipper_id = base.shipper_id and sla.report_date = date(base.last_delivered_timestamp)


WHERE sm.shipper_type_id IN (11)

AND DATE(base.last_delivered_timestamp) between DATE'2022-02-08' and DATE'2022-02-10'
AND base.city_group IN ('HN')

GROUP BY 1,2,3,4,5,6,7 )
