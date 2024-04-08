with dt_array_tab as
(select 1 mapping, sequence(cast('${date} 00:00:00' as timestamp ), cast('${date} 19:59:59' as timestamp ), interval '300' second  ) dt_array
) 
,list_time_range as
(select 
    t1.mapping
    ,t2.dt_array as start_time
    ,t2.dt_array + interval '299.99' second as end_time
from dt_array_tab t1
cross join unnest (dt_array) as t2(dt_array)
)
, base_time as
(SELECT uid as shipper_id
        ,1 as mapping
        ,date(from_unixtime(create_time - 60*60)) as create_date
        ,sm.city_name
        , CASE 
            WHEN sm.city_name = 'HCM City' then 'HCM'
            WHEN sm.city_name = 'Ha Noi City' then 'HN'
            WHEN sm.city_name = 'Da Nang City' then 'DN'
        else 'OTH' end as city_group
                              
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
            on sts.uid = sm.shipper_id and date(from_unixtime(create_time - 60*60)) = try_cast(sm.grass_date as date)
            
        where 1=1
        and date(from_unixtime(create_time - 60*60)) BETWEEN date('${date}') - interval '1' day and date('${date}') + interval '1' day
        and check_in_time > 0
        and check_out_time > 0
        and check_out_time >= check_in_time
        and order_end_time >= order_start_time
        and ((order_start_time = 0 and order_end_time = 0)
            OR (order_start_time > 0 and order_end_time > 0 and order_start_time >= check_in_time and order_start_time <= check_out_time)
            )
)
,overall as
(
    select 
        slot.report_date
        ,slot.hour_
        , CASE 
        WHEN sm.city_name = 'HCM City' then 'HCM'
        WHEN sm.city_name = 'Ha Noi City' then 'HN'
        WHEN sm.city_name = 'Da Nang City' then 'DN'
        else 'OTH' end as city_group
        ,count(distinct case when slot.work_time > 0 then slot.shipper_id else null end) total_work_driver_overall
        ,count(distinct case when slot.online_time > 0 then slot.shipper_id else null end) total_online_driver_overall
from dev_vnfdbi_opsndrivers.shopeefood_vn_driver_supply_hour_by_time_slot slot
left join shopeefood.foody_mart__profile_shipper_master sm
    on slot.shipper_id = sm.shipper_id and slot.report_date = try_cast(sm.grass_date as date)
where report_date = date('${date}')
group by 1,2,3
)
,raw as
(select 
    cast(hour(range.start_time) as varchar) || cast(minute(range.start_time) as varchar) as lookup_value
    ,date(range.start_time) as date_
    ,hour(range.start_time) as hour_
	,range.start_time
	,range.end_time
    ,base.city_group
	    ,count(distinct case 
        when range.start_time <= base.actual_start_time_work and base.actual_end_time_work <= range.end_time and order_start_time_original > 0 then base.shipper_id 
        when base.actual_start_time_work <= range.start_time and range.start_time <= base.actual_end_time_work and base.actual_end_time_work <= range.end_time and order_start_time_original > 0 then base.shipper_id
        when range.start_time <= base.actual_start_time_work and base.actual_start_time_work <= range.end_time and range.end_time <= base.actual_end_time_work and order_start_time_original > 0 then base.shipper_id
        when base.actual_start_time_work <= range.start_time and range.end_time <= base.actual_end_time_work and order_start_time_original > 0 then base.shipper_id
        else null end) as total_driver_work
    ,count(distinct case 
        when range.start_time <= base.actual_start_time_online and base.actual_end_time_online <= range.end_time then base.shipper_id
        when base.actual_start_time_online <= range.start_time and range.start_time <= base.actual_end_time_online and base.actual_end_time_online <= range.end_time then base.shipper_id
        when range.start_time <= base.actual_start_time_online and base.actual_start_time_online <= range.end_time and range.end_time <= base.actual_end_time_online then base.shipper_id
        when base.actual_start_time_online <= range.start_time and range.end_time <= base.actual_end_time_online then base.shipper_id
        else null end) as total_driver_online

from list_time_range range 
left join base_time base
	on range.mapping = base.mapping
group by 1,2,3,4,5,6
)
select 
    raw.*
    ,overall.total_online_driver_overall
    ,overall.total_work_driver_overall
from raw
left join overall
    on raw.date_ = overall.report_date and raw.city_group = overall.city_group and raw.hour_ = overall.hour_
