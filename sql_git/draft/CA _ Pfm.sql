with raw as 
(SELECT case when dot.is_asap = 1 then date(from_unixtime(dot.submitted_time - 3600)) 
             else date(from_unixtime(dot.real_drop_time - 3600)) end as report_date
--date(from_unixtime(dot.real_drop_time - 3600))  as report_date
,year(date(from_unixtime(dot.submitted_time - 3600)))*100 + week(date(from_unixtime(dot.submitted_time - 3600))) as create_year_week
,dot.uid
,city.city_name
--,count(dot.ref_order_code) as total_order
,dot.ref_order_code
,dot.ref_order_id
,case when dot.ref_order_category = 0 then '1. Food'
      else '2. Ship' end as service
,dot.group_id
,case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 then 1 else 0 end as is_group_order
,case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 0 then 1 else 0 end as is_stack_order

,case when a.experiment_group in (3,4) then 1 ELSE 0 end as is_auto_accepted

,case when a.experiment_group in (7,8) then 1 ELSE 0 end as is_auto_accepted_continuous_assign

,case when group_order.order_type = 200 and dot.ref_order_category <> 0 then if(ns.is_valid_lt_incharge = 1, ns.lt_incharge, 0)
      else date_diff('second',fa.first_auto_assign_timestamp,fa.last_incharge_timestamp)*1.0000/60 end as assign_time 

,case when group_order.order_type = 200 and dot.ref_order_category <> 0 then if(ns.is_valid_lt_pickup = 1, ns.lt_pickup, 0)
      else  date_diff('second',fa.last_incharge_timestamp,fa.last_picked_timestamp)*1.0000/60 end as picked_time 

,date_diff('second',from_unixtime(dot.submitted_time - 3600),from_unixtime(dot.real_drop_time - 3600))*1.0000/60 as completion_time 

FROM shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot

---City Name 
LEFT JOIN ( SELECT city_id
                  ,city_name

            from shopeefood.foody_mart__fact_gross_order_join_detail
            
            where from_unixtime(create_timestamp) between date(cast(now() - interval '30' day as TIMESTAMP)) and date(cast(now() - interval '1' hour as TIMESTAMP))
            
            GROUP BY city_id,city_name
            )city on city.city_id = dot.pick_city_id 
----Assignment
        LEFT JOIN 
    (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

        from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
        where status in (3,4) -- shipper incharge

        UNION

        SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

        from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
        where status in (3,4) -- shipper incharge
        )a on a.order_id = dot.ref_order_id and a.order_type = dot.ref_order_category

    -- take last incharge
         LEFT JOIN
            (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status

            from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
            where status in (3,4) -- shipper incharge

            UNION

            SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status

            from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
            where status in (3,4) -- shipper incharge
        )a_filter on a.order_uid = a_filter.order_uid and a.create_time < a_filter.create_time

--- Lead time 
        LEFT JOIN
                            (
                            SELECT   order_id 
                                    ,0 as order_type
                                    ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
                                    ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
                                    ,max(case when status = 6 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp
                                    from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
                                    where 1=1
                                    and grass_schema = 'foody_order_db'
                                    group by 1,2

                            UNION

                            SELECT   ns.order_id
                                    ,ns.order_type      
                                    ,min(from_unixtime(ns.create_time - 60*60)) first_auto_assign_timestamp
                                    ,max(case when status in (3,4) then cast(from_unixtime(ns.update_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
                                    ,max(case when status in (3,4) then cast(from_unixtime(ns.update_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp
                            FROM
                                    ( SELECT order_id, order_type , create_time , update_time, status

                                     from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                                     where order_type in (4,5,6,7)  -- now ship/ns shopee/ ns same day
                                     and grass_schema = 'foody_partner_archive_db'
                                     UNION

                                     SELECT order_id, order_type, create_time , update_time, status

                                     from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                                     where order_type in (4,5,6,7) -- now ship/ns shopee/ ns same day
                                     and schema = 'foody_partner_db'
                                     )ns
                            
                            GROUP BY 1,2
                            
                            
                            )fa on fa.order_id = dot.ref_order_id 
                                and fa.order_type = dot.ref_order_category
left join vnfdbi_opsndrivers.snp_foody_nowship_performance_tab ns on ns.group_id >0 and ns.group_id = dot.group_id and ns.id = dot.ref_order_id
---GROUP ORDER 
LEFT JOIN
(SELECT
a.order_id
,a.order_type
,case when a.order_type <> 200 then order_type else ogi.ref_order_category end as order_category
,case when a.assign_type = 1 then '1. Single Assign'
when a.assign_type in (2,4) then '2. Multi Assign'
when a.assign_type = 3 then '3. Well-Stack Assign'
when a.assign_type = 5 then '4. Free Pick'
when a.assign_type = 6 then '5. Manual'
when a.assign_type in (7,8) then '6. New Stack Assign'
else null end as assign_type
,from_unixtime(a.create_time - 60*60) as create_time
,from_unixtime(a.update_time - 60*60) as update_time
,date(from_unixtime(a.create_time - 60*60)) as date_
,case when cast(FROM_UNIXTIME(a.create_time - 60*60) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
when cast(FROM_UNIXTIME(a.create_time - 60*60) as date) between DATE('2021-01-01') and DATE('2021-01-03') then 202053
else YEAR(cast(FROM_UNIXTIME(a.create_time - 60*60) as date))*100 + WEEK(cast(FROM_UNIXTIME(a.create_time - 60*60) as date)) end as year_week

from (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
where status in (3,4) -- shipper incharge

UNION

SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
where status in (3,4) -- shipper incharge
)a

-- take last incharge
LEFT JOIN
(SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status

from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
where status in (3,4) -- shipper incharge

UNION

SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status

from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
where status in (3,4) -- shipper incharge
)a_filter on a.order_uid = a_filter.order_uid and a.create_time < a_filter.create_time

-- auto accept

LEFT JOIN shopeefood.foody_partner_db__order_group_info_tab__reg_daily_s0_live ogi on ogi.id > 0 and ogi.id = case when a.order_type = 200 then a.order_id else 0 end

where 1=1
and a_filter.order_id is null -- take last incharge
-- and a.order_id = 9490679
and a.order_type = 200

GROUP BY 1,2,3,4,5,6,7,8

)group_order on group_order.order_id = dot.group_id and dot.group_id > 0 and group_order.order_category = dot.ref_order_category


where dot.order_status = 400
and a_filter.order_uid is null
) 
,final as
(SELECT 
      a.report_date
     ,a.service
     ,a.city_name
     ,'1. All' as metrics
     ,count(a.ref_order_code) as total_order 
     ,sum(a.assign_time) as assign_time 
     ,sum(a.picked_time) as pickup_time 
     ,sum(a.completion_time) as completion_time 
     
from raw a
where 1 = 1 

GROUP BY 1, 2, 3, 4


UNION ALL 


SELECT 
      b.report_date
     ,b.service
     ,b.city_name
     ,'2. Auto Accept' as metrics
     ,count(b.ref_order_code) as total_order 
     ,sum(b.assign_time) as assign_time 
     ,sum(b.picked_time) as pickup_time 
     ,sum(b.completion_time)as completion_time 
     
from raw b
where 1 = 1 
and b.is_auto_accepted = 1 
GROUP BY 1, 2, 3, 4

UNION ALL 


SELECT 
      c.report_date
     ,c.service
     ,c.city_name
     ,'3. Auto Accept Continuous Assign' as metrics
     ,count(c.ref_order_code) as total_order 
     ,sum(c.assign_time)as assign_time 
     ,sum(c.picked_time)as pickup_time 
     ,sum(c.completion_time)  as completion_time 
     
from raw c
where 1 = 1 
and c.is_auto_accepted_continuous_assign = 1 
GROUP BY 1, 2, 3, 4
)

SELECT 
    a.report_date
    ,a.service 
    ,'1. All' as city_name
    ,a.metrics
    ,sum(a.total_order) as total_order 
     ,sum(a.assign_time)*1.0000/sum(a.total_order) as assign_time 
     ,sum(a.pickup_time)*1.0000/sum(a.total_order) as pickup_time 
     ,sum(a.completion_time)*1.0000/sum(a.total_order) as completion_time 

FROM final a 
where 1 = 1 
and a.report_date between current_date - interval '15' day and current_date - interval '1' day 
GROUP BY 1, 2, 3, 4

UNION ALL 

SELECT 
    b.report_date
    ,b.service 
    ,b.city_name
    ,b.metrics
    ,sum(b.total_order) as total_order 
     ,sum(b.assign_time)*1.0000/sum(b.total_order) as assign_time 
     ,sum(b.pickup_time)*1.0000/sum(b.total_order) as pickup_time 
     ,sum(b.completion_time)*1.0000/sum(b.total_order) as completion_time 

FROM final b 
where 1 = 1 
and b.report_date between current_date - interval '15' day and current_date - interval '1' day 
GROUP BY 1, 2, 3, 4