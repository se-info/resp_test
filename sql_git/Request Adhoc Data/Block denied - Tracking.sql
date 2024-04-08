with report_date_time as 
(
SELECT
     DATE(report_date) AS report_date
    ,sequence(cast(t.report_date as timestamp ) ,cast(t.report_date as timestamp) + interval '86400' second, interval '3600' second  ) dt_array 
    ,1 as mapping
FROM
    (
(
SELECT sequence(current_date - interval '7' day, current_date - interval '1' day) bar)
CROSS JOIN

    unnest (bar) as t(report_date)
)
)
,list_time_range as 
(select 
       t1.mapping
      ,t2.dt_array_unnest as start_time 
      ,t2.dt_array_unnest + interval '3599.99' second as end_time 



from report_date_time t1 

cross join unnest (dt_array) as t2(dt_array_unnest) 

order by 2 asc)

, base_time as
(SELECT sts.uid as shipper_id
        ,1 as mapping
        ,date(from_unixtime(sts.create_time - 60*60)) as create_date
        ,sm.city_name
        , CASE 
            WHEN sm.city_name = 'HCM City' then 'HCM'
            WHEN sm.city_name = 'Ha Noi City' then 'HN'
            WHEN sm.city_name = 'Da Nang City' then 'DN'
        else 'OTH' end as city_group
        ,case when sm.shipper_type_id = 12 and slot.uid is not null then 1 else 0 end as is_hub
                              
        ,from_unixtime(check_in_time - 60*60) as check_in_time
        ,from_unixtime(check_out_time - 60*60) as check_out_time
        ,from_unixtime(order_start_time - 60*60) as order_start_time
        ,from_unixtime(order_end_time - 60*60) as order_end_time
        
                       
        ,check_in_time as check_in_time_original
        ,check_out_time as check_out_time_original
        ,order_start_time as order_start_time_original
        ,order_end_time as order_end_time_original
                          
                     
        ,from_unixtime(check_in_time - 60*60) as actual_start_time_online
        ,greatest(from_unixtime(check_out_time - 60*60),from_unixtime(order_end_time - 60*60)) as actual_end_time_online
        ,case when order_start_time = 0 then from_unixtime(check_in_time - 60*60) else from_unixtime(order_start_time - 60*60) end as actual_start_time_work
        ,case when order_end_time = 0 then from_unixtime(check_in_time - 60*60) else from_unixtime(order_end_time - 60*60) end as actual_end_time_work
        ,date(from_unixtime(check_in_time - 60*60)) as actual_start_date_online
        ,date(greatest(from_unixtime(check_out_time - 60*60),from_unixtime(order_end_time - 60*60))) as actual_end_date_online
        
        from shopeefood.foody_internal_db__shipper_time_sheet_tab__reg_daily_s0_live sts
        left join shopeefood.foody_mart__profile_shipper_master sm
            on sts.uid = sm.shipper_id and date(from_unixtime(sts.create_time - 60*60)) = try_cast(sm.grass_date as date)

        left join shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live slot on slot.uid = sts.uid and date(from_unixtime(slot.date_ts - 3600)) = date(from_unixtime(sts.create_time - 60*60)) and slot.registration_status != 2             

            
        where 1=1
        and date(from_unixtime(sts.create_time - 60*60)) between current_date - interval '14' day and current_date + interval '1' day
        and check_in_time > 0
        and check_out_time > 0
        and check_out_time >= check_in_time
        and order_end_time >= order_start_time
        and ((order_start_time = 0 and order_end_time = 0)
            OR (order_start_time > 0 and order_end_time > 0 and order_start_time >= check_in_time and order_start_time <= check_out_time)
            )
)
,raw as
(select 
    --  cast(hour(range.start_time) as varchar) || cast(minute(range.start_time) as varchar) as lookup_value
     shipper_id
    ,date(range.start_time) as date_
    ,hour(range.start_time) as hour_
	,range.start_time
	,range.end_time
    ,base.city_group
    ,base.is_hub
    ,case 
        when range.start_time <= base.actual_start_time_online and base.actual_end_time_online <= range.end_time then 1
        when base.actual_start_time_online <= range.start_time and range.start_time <= base.actual_end_time_online and base.actual_end_time_online <= range.end_time then 1
        when range.start_time <= base.actual_start_time_online and base.actual_start_time_online <= range.end_time and range.end_time <= base.actual_end_time_online then 1
        when base.actual_start_time_online <= range.start_time and range.end_time <= base.actual_end_time_online then 1
        else 0 end as is_valid
	--     ,count(distinct case 
    --     when range.start_time <= base.actual_start_time_work and base.actual_end_time_work <= range.end_time then base.shipper_id
    --     when base.actual_start_time_work <= range.start_time and range.start_time <= base.actual_end_time_work and base.actual_end_time_work <= range.end_time then base.shipper_id
    --     when range.start_time <= base.actual_start_time_work and base.actual_start_time_work <= range.end_time and range.end_time <= base.actual_end_time_work then base.shipper_id
    --     when base.actual_start_time_work <= range.start_time and range.end_time <= base.actual_end_time_work then base.shipper_id
    --     else null end) as total_driver_work
    -- ,count(distinct case 
    --     when range.start_time <= base.actual_start_time_online and base.actual_end_time_online <= range.end_time then base.shipper_id
    --     when base.actual_start_time_online <= range.start_time and range.start_time <= base.actual_end_time_online and base.actual_end_time_online <= range.end_time then base.shipper_id
    --     when range.start_time <= base.actual_start_time_online and base.actual_start_time_online <= range.end_time and range.end_time <= base.actual_end_time_online then base.shipper_id
    --     when base.actual_start_time_online <= range.start_time and range.end_time <= base.actual_end_time_online then base.shipper_id
    --     else null end) as total_driver_online

from list_time_range range 
left join base_time base
	on range.mapping = base.mapping
-- group by 1,2,3,4,5,6,7
)
,final_metrics as 
(select 
        a.date_ 
       ,a.start_time
       ,a.end_time
       ,a.shipper_id 
       ,a.city_group
       ,a.is_hub
       ,a.is_valid 
       ,hub.extra_ship
       ,array_agg(distinct order_id) as denied_order
       ,cardinality(filter(array_agg(distinct order_id),x -> x is not null)) as total_denied 
       ,cardinality(filter(array_agg(distinct order_uid),x -> x is not null)) as total_assign
       ,try( 1 - (cardinality(filter(array_agg(distinct order_id),x -> x is not null))/cast(cardinality(filter(array_agg(distinct order_uid),x -> x is not null))as double))) as rate_denied
       ,dense_rank()over(partition by a.shipper_id,a.date_ order by start_time asc) as rank_ 
    --    ,case when cardinality(filter(array_agg(order_id),x -> x is not null)) > 0 then dense_rank()over(partition by shipper_id,date_,array_agg(order_id) order by start_time asc) else 0 end as rank_ 



    --    ,array_join(cast(filter(map_values(array_agg(deny_order)),x -> x is not null) as array<json>),',') as data_denied
from 
(select 
     raw.*
     ,ass.order_id
     ,sa.order_uid 
    -- ,map(array['order_id','deny_type'],array[cast(ass.order_id as varchar ),ass.issue_category]) as deny_order

from raw

left join dev_vnfdbi_opsndrivers.phong_raw_assignment_test ass  on ass.shipper_id = raw.shipper_id 
                                                                and raw.date_ = date(ass.timestamp) 
                                                                and ass.timestamp between raw.start_time and raw.end_time 
                                                                and ass.issue_category not in ('Ignore', 'Sytem_Fault')

left join 
        (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                        ,shipper_uid
                from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                where assign_type != 5
                UNION
            
                SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                        ,shipper_uid
                from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                where assign_type != 5
            )sa on sa.shipper_uid = raw.shipper_id 
                and raw.date_ = date(from_unixtime(sa.create_time - 3600))
                and from_unixtime(sa.create_time - 3600) between raw.start_time and raw.end_time   



where raw.is_valid = 1 

and raw.is_hub = 1 

and raw.date_ = current_date - interval '2' day
group by 1,2,3,4,5,6,7,8,9,10

) a 
left join dev_vnfdbi_opsndrivers.phong_hub_driver_metrics hub on hub.uid = a.shipper_id and hub.date_ = a.date_ 



where city_group = 'HCM'
-- and a.shipper_id = 10113838
and hub.extra_ship > 0   

group by 1,2,3,4,5,6,7,8

order by 4,11)


select  a.date_ 
       ,a.start_time 
       ,a.end_time 
       ,a.shipper_id,a.city_group,a.is_hub,a.rank_,total_assign,extra_ship
       ,sum(a.total_denied)over(partition by a.shipper_id order by a.start_time asc) as denied_cum
       ,sum(a.total_assign)over(partition by a.shipper_id order by a.start_time asc) as assign_cum 
    --    ,b.start_time as start_time_v2 
    --    ,b.total_assign as assign_v2 





from final_metrics a 

where 1 = 1 
