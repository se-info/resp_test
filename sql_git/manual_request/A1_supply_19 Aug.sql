SELECT drv.date_ 
,drv.h_ 
,drv.start_time_slot
,drv.end_time_slot
,drv.city_group
,drv.total_driver_online
,drv.total_driver_work




from
(SELECT base1.date_ 
,base1.h_
,base1.start_time_slot
,base1.end_time_slot
,driver.city_group
,sum(base1.total_driver_online) as total_driver_online
,sum(base1.total_driver_work) as total_driver_work

from
(SELECT base.date_
,base.h_
,base.start_time_slot
,base.end_time_slot
-- ,base.city_group
,base.shipper_id
,count(distinct case when base.online_time > 0 then base.shipper_id else null end) as total_driver_online
,count(distinct case when base.work_time > 0 then base.shipper_id else null end) as total_driver_work

from
(SELECT time_.mapping
,time_.date_
,time_.h_
,time_.start_time_slot
,time_.end_time_slot
,online.shipper_id
,online.actual_start_time_online
,online.actual_end_time_online
,online.actual_start_time_work
,online.actual_end_time_work
--,driver.city_group

,case when online.actual_end_time_online < time_.start_time_slot then 0
    when online.actual_start_time_online > time_.end_time_slot then 0
    else date_diff('second',   greatest(time_.start_time_slot,online.actual_start_time_online)   ,   least(time_.end_time_slot,online.actual_end_time_online)   )*1.0000
    end as online_time

,case when online.actual_end_time_work < time_.start_time_slot then 0
    when online.actual_start_time_work > time_.end_time_slot then 0
    else date_diff('second',   greatest(time_.start_time_slot,online.actual_start_time_work)   ,   least(time_.end_time_slot,online.actual_end_time_work)   )*1.0000
    end as work_time


from
(SELECT d.mapping
		,d.date_ 
		,h.h_
        ,date_add('hour',h.h_,cast(d.date_ as TIMESTAMP)) as start_time_slot
		,date_add('second',0,date_add('minute',60,date_add('hour',h.h_,cast(d.date_ as TIMESTAMP)))) as end_time_slot		


		from
		(SELECT cast(psm.grass_date as DATE ) as date_
		,1 as mapping

		from shopeefood.foody_mart__profile_shipper_master psm

		where 1=1
		and psm.grass_date <> 'current'
		and cast(psm.grass_date as DATE) >= date(current_date) - interval '120' day 
        --and cast(psm.grass_date as DATE) between  date(now() - interval '3' day) and date(now() - interval '1' day)    -- date('2020-11-11')

		GROUP BY 1
		)d
		 
			LEFT JOIN
					(SELECT temp.h_
						,1 as mapping
						
						from
						(SELECT 0 as h_ UNION
    						SELECT 1 as h_ UNION
    						SELECT 2 as h_ UNION
    						SELECT 3 as h_ UNION
    						SELECT 4 as h_ UNION
    						SELECT 5 as h_ UNION
    						SELECT 6 as h_ UNION
    						SELECT 7 as h_ UNION
    						SELECT 8 as h_ UNION
    						SELECT 9 as h_ UNION
    						SELECT 10 as h_ UNION
    						SELECT 11 as h_ UNION
    						SELECT 12 as h_ UNION
    						SELECT 13 as h_ UNION
    						SELECT 14 as h_ UNION
    						SELECT 15 as h_ UNION
    						SELECT 16 as h_ UNION
    						SELECT 17 as h_ UNION
    						SELECT 18 as h_ UNION
    						SELECT 19 as h_ UNION
    						SELECT 20 as h_ UNION
    						SELECT 21 as h_ UNION
    						SELECT 22 as h_ UNION
    						SELECT 23 as h_

					)temp
				)h on h.mapping = d.mapping
				
	
				
)time_				
				
    LEFT JOIN (SELECT uid as shipper_id
                ,1 as mapping
                ,date(from_unixtime(create_time - 60*60)) as create_date
                
                -- important timestamp
                ,from_unixtime(check_in_time - 60*60) as check_in_time
                ,from_unixtime(check_out_time - 60*60) as check_out_time
                ,from_unixtime(order_start_time - 60*60) as order_start_time
                ,from_unixtime(order_end_time - 60*60) as order_end_time
                
                -- for checking
                ,check_in_time as check_in_time_original
                ,check_out_time as check_out_time_original
                ,order_start_time as order_start_time_original
                ,order_end_time as order_end_time_original
                ------------------
                -- actual use
                ,from_unixtime(check_in_time - 60*60) as actual_start_time_online
                ,greatest(from_unixtime(check_out_time - 60*60),from_unixtime(order_end_time - 60*60)) as actual_end_time_online
                ,case when order_start_time = 0 then from_unixtime(check_in_time - 60*60) else from_unixtime(order_start_time - 60*60) end as actual_start_time_work
                ,case when order_end_time = 0 then from_unixtime(check_in_time - 60*60) else from_unixtime(order_end_time - 60*60) end as actual_end_time_work
                ,date(from_unixtime(check_in_time - 60*60)) as actual_start_date_online
                ,date(greatest(from_unixtime(check_out_time - 60*60),from_unixtime(order_end_time - 60*60))) as actual_end_date_online
                
                from shopeefood.foody_internal_db__shipper_time_sheet_tab__reg_daily_s0_live sts
                    
                where 1=1
                --and sts.uid = 2576025
                --and date(from_unixtime(create_time - 60*60)) between  date(now() - interval '3' day) and  date(now() - interval '1' day)  -- date('2020-11-10') and date('2020-11-12')   -- date(now() - interval '1' day) -- date('2020-06-30')
                and 
                    (date(from_unixtime(create_time - 60*60)) >= current_date - interval '120' day
                     )
                
                and check_in_time > 0
                and check_out_time > 0
                and check_out_time >= check_in_time
                and order_end_time >= order_start_time
                and ((order_start_time = 0 and order_end_time = 0)
                    OR (order_start_time > 0 and order_end_time > 0 and order_start_time >= check_in_time and order_start_time <= check_out_time)
                    )
            )online on time_.mapping = online.mapping and (time_.date_ = online.actual_start_date_online OR time_.date_ = online.actual_end_date_online) 
    
           
            
  
 --  limit 100
  
)base  
-- where test.online_time > 0 
  
   GROUP BY 1,2,3,4,5
   
)base1   
    LEFT JOIN (
select  
         try_cast(sm.grass_date as date) as report_date 
        ,sm.shipper_id 
        ,coalesce(loc.city_name,sm.city_name) as city_group

    from shopeefood.foody_mart__profile_shipper_master sm 

    LEFT JOIN (SELECT 
                 oct.uid as shipper_id
                ,cast(from_unixtime(oct.submitted_time - 60*60) as date) as created_date
                ,from_unixtime(oct.submitted_time - 60*60) as created_timestamp
                ,case when oct.pick_city_id = 238 THEN 'Dien Bien' else city.name_en end as city_name
                -- ,city.name_en as city_name
                ,ROW_NUMBER()OVER(partition by uid,cast(from_unixtime(oct.submitted_time - 60*60) as date) order by from_unixtime(oct.submitted_time - 60*60) desc) as rank 
                
                from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) oct

                -- city name
                left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = oct.pick_city_id and city.country_id = 86

                            
                
                where 1=1
                and oct.order_status = 400
                
                -- GROUP BY 1,2,3,4,5
            )loc on loc.shipper_id = sm.shipper_id and loc.created_date = try_cast(sm.grass_date as date) and loc.rank = 1 
        

where 1 = 1 
and sm.grass_date != 'current'
                )driver on driver.shipper_id = base1.shipper_id and driver.report_date = base1.date_ 


GROUP BY 1,2,3,4,5

--   limit 100
   
 )drv
        

where drv.date_ between current_date - interval '1' day and current_date - interval '1' day 