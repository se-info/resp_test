with raw as 
        (
        SELECT   distinct a.id 
                , a.uid as driver_id 
                , a.city_name
                , a.service_type
                , a.grass_date
                , a.is_asap
                , a.total_compensation_loss
                , a.is_cancelled
                , if(cancel_reason = 'No driver', 1, 0) is_cancel_no_driver
                , if(b.incharge_timestamp > 0 , 1, 0) is_found_driver

        FROM dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_ops_order_detail_ext a
        left join shopeefood.foody_mart__fact_gross_order_join_detail b on a.id = b.id
        where 1=1 
      --  and cancel_reason = 'No driver'
        and a.grass_date between date'2022-07-01' and date'2022-08-09'
        )

-- select * from raw where id = 332997117
, assign_raw as 

(
select distinct order_id
      ,driver_id 
      ,status 
      ,incharge_unixtime
      ,1 as is_driver_incharged
from 
        (
        SELECT
                    a.order_uid
                    , a.order_id
                    , CASE
                        WHEN a.order_type = 0 THEN '1. Food/Market'
                        WHEN a.order_type in (4,5) THEN '2. NS'
                        WHEN a.order_type = 6 THEN '3. NSS'
                        WHEN a.order_type = 7 THEN '4. NS Same Day'
                    ELSE 'Others' END AS order_source
                    , a.order_type
                    ,CASE
                        WHEN a.assign_type = 1 THEN '1. Single Assign'
                        WHEN a.assign_type in (2,4) THEN '2. Multi Assign'
                        WHEN a.assign_type = 3 THEN '3. Well-Stack Assign'
                        WHEN a.assign_type = 5 THEN '4. Free Pick'
                        WHEN a.assign_type = 6 THEN '5. Manual'
                        WHEN a.assign_type in (7,8) THEN '6. New Stack Assign'
                        ELSE NULL END AS assign_type
                    , DATE(FROM_UNIXTIME(a.create_time - 3600)) AS date_
                    , a.create_time as incharge_unixtime
                    , a.status
                    , IF(a.experiment_group IN (3,4,7,8), 1, 0) AS is_auto_accepted
                    , a.shipper_id as driver_id

                FROM
                    (SELECT
                        CONCAT(CAST(order_id AS VARCHAR), '-', CAST(order_type AS VARCHAR)) AS order_uid
                        , order_id, city_id, assign_type, update_time, create_time, status, order_type
                        , experiment_group, shipper_uid AS shipper_id

                    FROM shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                    WHERE status IN (3,4) -- shipper incharge

                    UNION ALL

                    SELECT
                        CONCAT(CAST(order_id as VARCHAR), '-', CAST(order_type AS VARCHAR)) AS order_uid
                        , order_id, city_id, assign_type, update_time, create_time, status, order_type
                        , experiment_group, shipper_uid AS shipper_id

                    FROM shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                    WHERE status IN (3,4) -- shipper incharge 
                    ) a

                LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city ON city.id = a.city_id AND city.country_id = 86

                WHERE 1=1
                -- AND DATE(FROM_UNIXTIME(a.create_time - 3600)) >= date(current_date) - interval '90' day
                -- AND DATE(FROM_UNIXTIME(a.create_time - 3600)) < date(current_date)
                and order_type = 0

        )
)

,cc_auto_assign as 

(
SELECT --*, FROM_UNIXTIME(create_time) - interval '1' hour as ts 
      order_id
    , create_uid
    , create_time as auto_assign_unixtime
    , FROM_UNIXTIME(create_time - 3600) auto_assign_timestamp

from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
where 1=1 and status = 21 and create_uid > 0 --and order_id = 332997117

)

,withdraw_raw as 
(

SELECT a.*, b.driver_id as driver_id_b , b.incharge_unixtime, from_unixtime(b.incharge_unixtime - 3600) as incharge_time, b.is_driver_incharged, b.status
         , c.auto_assign_timestamp as cc_change_auto_assign_time
         , c.auto_assign_unixtime
         , case when c.create_uid > 0 then 1 else 0 end as is_cc_change_auto_assign
         , row_number() over(partition by a.id,b.driver_id order by c.auto_assign_timestamp asc) rk
from raw a 

left join assign_raw b on a.id = b.order_id and a.is_found_driver = 1 and a.driver_id != b.driver_id 
left join cc_auto_assign c on a.id = c.order_id and c.auto_assign_unixtime between b.incharge_unixtime and b.incharge_unixtime + 1800 
--where id = 332997117

)

-- select * from withdraw_raw where id = 344969771



select  
*

from 
(
select    a.id 
        , a.driver_id 
        , a.city_name
        , a.service_type
        , a.grass_date
        , a.is_found_driver
        , a.is_asap 
        , a.is_cancelled
        , a.total_compensation_loss
        , a.incharge_time
        , cc_change_auto_assign_time
        , if(is_cc_change_auto_assign = 1, 1, 0) is_driver_withdraw_order
        , auto_assign_unixtime - incharge_unixtime as lt_incharge_to_withdraw
from withdraw_raw a

where 1=1 
and rk = 1
--group by 1,2,3,4,5,6,7,8,9,10,
)

where 1=1 AND grass_date >= date'2022-08-09' --and is_driver_withdraw_order = 0 AND  is_found_driver = 1
--group by 1,2,3
