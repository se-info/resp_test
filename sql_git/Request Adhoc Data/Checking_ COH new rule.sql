with report_date_time as 
(
SELECT
     DATE(report_date) AS report_date
    ,sequence(cast(t.report_date as timestamp ) ,cast(t.report_date as timestamp) + interval '86400' second, interval '3600' second  ) dt_array 
    ,1 as mapping
FROM
    (
(
SELECT sequence(current_date - interval '14' day, current_date - interval '1' day) bar)
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
        and date(from_unixtime(create_time - 60*60)) BETWEEN current_date - interval '14' day and current_date - interval '1' day
        -- and sts.uid = 14073602
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
         date(range.start_time) as date_
        ,hour(range.start_time) as hour_
	    ,range.start_time
        ,range.end_time
        ,base.shipper_id
        ,base.city_group
        ,case 
            when range.start_time <= base.actual_start_time_work and base.actual_end_time_work <= range.end_time and order_start_time_original > 0 then 1
            when base.actual_start_time_work <= range.start_time and range.start_time <= base.actual_end_time_work and base.actual_end_time_work <= range.end_time and order_start_time_original > 0 then 1
            when range.start_time <= base.actual_start_time_work and base.actual_start_time_work <= range.end_time and range.end_time <= base.actual_end_time_work and order_start_time_original > 0 then 1
            when base.actual_start_time_work <= range.start_time and range.end_time <= base.actual_end_time_work and order_start_time_original > 0 then 1
            else null end as is_valid_work
        ,case 
            when range.start_time <= base.actual_start_time_online and base.actual_end_time_online <= range.end_time then 1
            when base.actual_start_time_online <= range.start_time and range.start_time <= base.actual_end_time_online and base.actual_end_time_online <= range.end_time then 1
            when range.start_time <= base.actual_start_time_online and base.actual_start_time_online <= range.end_time and range.end_time <= base.actual_end_time_online then 1
            when base.actual_start_time_online <= range.start_time and range.end_time <= base.actual_end_time_online then 1
            else null end as is_valid_online 
            

from list_time_range range 
left join base_time base on range.mapping = base.mapping
-- group by 1,2,3,4,5,6
)
-- select 
--     raw.*
-- from raw

-- where is_valid_work = 1 
-- and shipper_id = 14073602
-- and date_ = current_date - interval '1' day
,final_driver as
(select 
    raw.*
from raw

where (is_valid_work = 1 or is_valid_online = 1) 
-- and shipper_id = 14073602
-- and date_ = current_date - interval '1' day
)
, base as
(select
         date(from_unixtime(create_time-3600)) created_date
        ,hour(from_unixtime(create_time-3600)) created_hour
        ,user_id
        ,id
        ,txn_type
        ,sum(balance/cast(100 as double)) over (partition by user_id order by id asc rows between unbounded preceding and current row) as main_wallet
        ,sum(deposit/cast(100 as double)) over (partition by user_id order by id asc rows between unbounded preceding and current row) as deposit
        ,row_number () over(partition by user_id,date(from_unixtime(create_time-3600)),hour(from_unixtime(create_time-3600)) order by id desc) as rank        
        --,sum(balance/100) over (partition by user_id order by id asc rows between unbounded preceding and current row) + sum(deposit/100) over (partition by user_id order by id asc rows between unbounded preceding and current row) as total_balance

from shopeefood.foody_accountant_db__partner_transaction_tab__reg_daily_s0_live

where 1 = 1 
-- and user_id = 2996387
-- group by 1,2,3,4,5
)
,final_coh as 
(select 
          base.*
        ,(main_wallet + deposit) as coh_current


from base 
-- where created_date = current_date - interval '1' day 
)
,final as 
(select fd.date_
       ,fd.hour_ 
       ,fd.shipper_id
    --    ,fc.coh_current
    --    ,fc2.coh_current as coh_2
       ,coalesce(fc.coh_current,fc2.coh_current) as coh_current 
       ,coalesce(fd.city_group,null) city_group 
       ,coalesce(fd.is_valid_online,0) is_valid_online
       ,coalesce(fd.is_valid_work,0) is_valid_work 


from final_driver fd  

left join  final_coh fc on fc.user_id = fd.shipper_id
                        and fc.created_date = fd.date_ 
                        and fc.created_hour = fd.hour_
                        and fc.rank = 1

left join  final_coh fc2 on fc2.user_id = fd.shipper_id
                        and fc2.created_date = fd.date_ 
                        and fc2.created_hour = fd.hour_ - 1
                        and fc2.rank = 1                                                          
where 1 = 1 
and fd.date_ >= current_date - interval '7' day
)


select --* from final where shipper_id = 15913869
        date_
       ,hour_
       ,city_group
       ,case when coh_current < 0 then '1. Under 0'
             when coh_current <= 100000 then '2. 0 - 100k'   
             when coh_current <= 200000 then '3. 100 - 200k'   
             when coh_current <= 300000 then '4. 200 - 300k'   
             when coh_current <= 400000 then '5. 300 - 400k'   
             when coh_current <= 600000 then '6. 400 - 600k'   
             when coh_current <= 800000 then '7. 600 - 800k'   
             when coh_current <= 1000000 then '8. 800 - 1000k'   
             when coh_current <= 2000000  then '9. 1000 - 2000k'
             when coh_current <= 5000000  then '10. 2000 - 5000k'
             when coh_current > 5000000  then '11. > 5000k'
             else 'No transaction' end as coh_range
        -- ,coh_current             
        -- ,shipper_id
        ,count(distinct case when is_valid_online = 1 then shipper_id else null end) as total_online_drivers              
        ,count(distinct case when is_valid_work = 1 then shipper_id else null end) as total_working_drivers              



from final
-- where city_group = 'HCM' and hour_ = 10
-- where shipper_id = 15913869
group by 1,2,3,4
