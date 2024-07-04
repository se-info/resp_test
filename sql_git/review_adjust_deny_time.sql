with report_date_time as 
(
SELECT
     DATE(report_date) AS report_date
    ,sequence(cast(t.report_date as timestamp ) ,cast(t.report_date as timestamp) + interval '86400' second, interval '1799.99' second  ) dt_array 
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
      ,t2.dt_array_unnest + interval '1798.99' second as end_time 



from report_date_time t1 

cross join unnest (dt_array) as t2(dt_array_unnest) 

order by 2 asc)

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
        ,case when sm.shipper_type_id = 12 then 1 else 0 end as is_hub
                              
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
        and date(from_unixtime(create_time - 60*60)) between current_date - interval '14' day and current_date + interval '1' day
        and check_in_time > 0
        and check_out_time > 0
        and check_out_time >= check_in_time
        and order_end_time >= order_start_time
        and ((order_start_time = 0 and order_end_time = 0)
            OR (order_start_time > 0 and order_end_time > 0 and order_start_time >= check_in_time and order_start_time <= check_out_time)
            )
)
,f as 
(select 
     cast(hour(range.start_time) as varchar) || cast(minute(range.start_time) as varchar) as lookup_value
    ,date(range.start_time) as date_
    ,hour(range.start_time) as hour_
	,range.start_time
	,range.end_time
    ,base.city_group
    ,base.is_hub
    ,base.shipper_id
    ,base.actual_start_time_work
    ,base.actual_end_time_work
    ,case 
        when range.start_time <= base.actual_start_time_work and base.actual_end_time_work <= range.end_time then 0.5
        when base.actual_start_time_work <= range.start_time and range.start_time <= base.actual_end_time_work and base.actual_end_time_work <= range.end_time then 0.5
        when range.start_time <= base.actual_start_time_work and base.actual_start_time_work <= range.end_time and range.end_time <= base.actual_end_time_work then 0.5
        when base.actual_start_time_work <= range.start_time and range.end_time <= base.actual_end_time_work then 0.5
        else 0 end as qualified
    ,case 
    when base.actual_end_time_work >= range.end_time then date_diff('second',range.start_time,range.end_time)*1.00/3600
    when base.actual_end_time_work < range.end_time then date_diff('second',base.actual_end_time_work,range.end_time)*1.00/3600
    else 0
    end as working_time
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
)
,waiting_time_tab as 
(select
        created_date,
        shipper_id,
        sum(lt_arrive_to_pick) as waiting_time

from     
(select         
        id,
        created_date,
        shipper_id,
        date_diff('second',max_arrived_at_merchant_timestamp,picked_timestamp)*1.00/3600 as lt_arrive_to_pick

from driver_ops_raw_order_tab

where created_date >= current_date - interval '7' day
and (HOUR(created_timestamp)*100 + MINUTE(created_timestamp)) between 1630 and 1733
and order_status = 'Delivered'
and order_type = 0 
)
where lt_arrive_to_pick > 0 
group by 1,2
)
,deny_log as 
(select shipper_id,created,count(delivery_id) from driver_ops_deny_log_tab
where created >= current_date - interval '7' day
and (HOUR(created_ts)*100 + MINUTE(created_ts)) between 1630 and 1733
and reason_name_en = 'Merchant long preparation time'
group by 1,2
)
,m as 
(select 
        f.date_,
        f.shipper_id,
        f.city_group,
        coalesce(w.waiting_time,0) as waiting_time,
        if(d.shipper_id is not null,1,0) as is_deny,
        sum(working_time) as working_time_by_hour


from f 

left join waiting_time_tab w on w.shipper_id = f.shipper_id and w.created_date = f.date_

left join deny_log d on d.shipper_id = f.shipper_id and d.created = f.date_ 

where qualified > 0 
and date_ >= current_date - interval '3' day
and (f.start_time between cast('2024-02-29 16:00:00' as timestamp) and cast('2024-02-29 17:33:00' as timestamp)
or f.start_time between cast('2024-02-28 16:00:00' as timestamp) and cast('2024-02-28 17:33:00' as timestamp)
)
group by 1,2,3,4,5
)
-- select * from m where city_group = 'HN' and is_deny = 1 
select
        date_,
        city_group,
        -- is_d eny,
        avg(waiting_time) as avg_waiting_time,
        avg(working_time_by_hour) as avg_working_time_by_hour,
        sum(waiting_time)*1.0000/sum(working_time_by_hour) as pp_waiting_time

from m 
where 1 = 1 
group by 1,2

