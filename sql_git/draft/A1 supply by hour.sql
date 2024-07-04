SELECT drv.date_ 
,drv.h_ 
,drv.start_time_slot
,drv.end_time_slot
,drv.city_group
,drv.total_driver_online
,drv.total_driver_work
--,peak.percent_time_running_normal
--,peak.percent_time_running_peak_1
--,peak.percent_time_running_peak_2
--,peak.percent_time_running_peak_3
,coalesce(vol.total_order_inflow_overall,0) as total_order_inflow_overall
,coalesce(vol.total_order_cancelled_overall,0) as total_order_cancelled_overall

,coalesce(vol.total_order_inflow_food,0) as total_order_inflow_food
,coalesce(vol.total_order_cancelled_food,0) as total_order_cancelled_food

,coalesce(vol.total_order_inflow_ns,0) as total_order_inflow_ns
,coalesce(vol.total_order_cancelled_ns,0) as total_order_cancelled_ns
,coalesce(vol.total_order_freepick_not_cancelled,0) as total_order_freepick_not_cancelled_overall
,coalesce(vol.total_order_freepick_not_cancelled_food,0) as total_order_freepick_not_cancelled_food
,coalesce(vol.total_order_freepick_not_cancelled_ns,0) as total_order_freepick_not_cancelled_ns

,coalesce(vol.total_order_freepick_cancelled,0) total_order_freepick_cancelled_overall
,coalesce(vol.total_order_freepick_cancelled_food,0) total_order_freepick_cancelled_food
,coalesce(vol.total_order_freepick_cancelled_ns,0) total_order_freepick_cancelled_ns
--,coalesce(vol.total_order_stack_not_cancelled,0) as total_order_stack_not_cancelled
,coalesce(vol.total_unique_user_food,0) total_unique_user_food
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
    LEFT JOIN (SELECT shipper_id
                    ,city_name
                    ,case when grass_date = 'current' then date(current_date)
                        else cast(grass_date as date) end as report_date
                    ,case when city_name = 'HCM City' then 'HCM'
                        when city_name = 'Ha Noi City' then 'HN'
                        when city_name = 'Da Nang City' then 'DN'
                        else city_name end as city_group
                
                    from shopeefood.foody_mart__profile_shipper_master
                    
                    where 1=1
                    and (grass_date = 'current' 
                        OR cast(grass_date as date) >= current_date - interval '120' day
                        )        -- cast(grass_date as date) >= date(current_date) - interval '10' day))
                    
                    --and shipper_type_id <> 3
                    --and shipper_status_code = 1
    
                )driver on driver.shipper_id = base1.shipper_id and driver.report_date = base1.date_ 


GROUP BY 1,2,3,4,5

--   limit 100
   
 )drv
        LEFT JOIN (SELECT base2.city_group
					,base2.date_
					,base2.h_
					,base2.start_time_slot
					,base2.end_time_slot
					,sum(base2.time_in_normal_mode)*1.0000/sum(base2.time_base_per_slot) as percent_time_running_normal
					,sum(base2.time_in_peak_1)*1.0000/sum(base2.time_base_per_slot) as percent_time_running_peak_1
					,sum(base2.time_in_peak_2)*1.0000/sum(base2.time_base_per_slot) as percent_time_running_peak_2
					,sum(base2.time_in_peak_3)*1.0000/sum(base2.time_base_per_slot) as percent_time_running_peak_3
					
					
					from
					(SELECT base1.city_id
					,case when base1.city_id = 217 then 'HCM'
						when base1.city_id = 218 then 'HN'
						when base1.city_id = 219 then 'DN'
						else 'OTH' end as city_group
					,city.city_name    
					,base1.district_id
					,case when base1.district_id in (1,5,17,20,21,22,25,29,31,12) then 'Center'
						  when base1.district_id in (2,4,6,7,19,23,24,26,27,28,9,11,15,16,35,693,945) then 'Outer Center'
						  when base1.district_id in (8,18,30,32,10,13,14,33,34,677,678,679,688,690,694,695,696,699) then  'Suburb'
						  else 'Others' end as area
					,district.name_en as district_name
					,base1.date_
					,base1.h_
					,base1.start_time_slot
					,base1.end_time_slot
					
					,3600 as time_base_per_slot
					,sum(case when base1.peak_mode_name not in ('Peak 1 Mode', 'Peak 2 Mode', 'Peak 3 Mode') then base1.mode_time_in_slot_second else null end) as time_in_normal_mode
					,sum(case when base1.peak_mode_name in ('Peak 1 Mode') then base1.mode_time_in_slot_second else null end) as time_in_peak_1
					,sum(case when base1.peak_mode_name in ('Peak 2 Mode') then base1.mode_time_in_slot_second else null end) as time_in_peak_2
					,sum(case when base1.peak_mode_name in ('Peak 3 Mode') then base1.mode_time_in_slot_second else null end) as time_in_peak_3
					
					from
					(SELECT base.mapping
					,base.city_id
					,base.district_id
					,base.date_
					,case when base.date_ between DATE('2018-12-31') and DATE('2018-12-31') then 201901
						when base.date_ between DATE('2019-12-30') and DATE('2019-12-31') then 202001
						else YEAR(base.date_)*100 + WEEK(base.date_) end as created_year_week
					
					,base.h_
					,base.start_time_slot
					,base.end_time_slot
					,mode.mode_id
					,mode.peak_mode_name
					,mode.start_time
					,mode.end_time
					,mode.available_driver
					,mode.assigning_order
					,mode.driver_availability
					,case when mode.end_time < base.start_time_slot then 0
						when mode.start_time > base.end_time_slot then 0
						else date_diff('second',   greatest(base.start_time_slot,mode.start_time)   ,   least(base.end_time_slot,mode.end_time)   ) --*1.0000/(60*60)
						end as mode_time_in_slot_second
					
					
					from
					(SELECT loc.mapping
					,loc.city_id
					,loc.district_id
					,t.date_
					,t.h_
					,t.start_time_slot
					,t.end_time_slot
					
					
					from
					(SELECT pm.city_id
					,pm.district_id
					,1 as mapping
					
					from shopeefood.foody_delivery_admin_db__peak_mode_export_activity_tab__reg_daily_s0_live pm
					GROUP BY 1,2
					)loc
						LEFT JOIN 
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
									-- and cast(psm.grass_date as DATE) between  date(now() - interval '3' day) and date(now() - interval '1' day)    -- date('2020-11-11')
							               
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
								
								
								)t on loc.mapping = t.mapping
								
					)base 
						LEFT JOIN
									(SELECT pm.city_id
										,pm.district_id
										,pm.mode_id
										,from_unixtime(pm.start_time - 60*60) as start_time
										,from_unixtime(pm.start_time + pm.running_time - 60*60) as end_time
										,pm.available_driver
										,pm.assigning_order
										,pm.driver_availability
										,pm_name.name as peak_mode_name
										
										from shopeefood.foody_delivery_admin_db__peak_mode_export_activity_tab__reg_daily_s0_live pm 
											left join shopeefood.foody_delivery_admin_db__peak_mode_tab__reg_daily_s0_live pm_name on pm_name.id = pm.mode_id
										where pm.mode_id in (7,8,9,10,11)
									)mode on mode.city_id = base.city_id and mode.district_id = base.district_id
					
					)base1
					
							LEFT JOIN shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live district on base1.district_id = district.id
								LEFT JOIN 
										(SELECT city_id
											,city_name
											
											from shopeefood.foody_mart__profile_shipper_master psm
											GROUP BY 1,2
										)city on city.city_id = base1.city_id
								
					WHERE base1.created_year_week > YEAR(date(current_date) - interval '120' day)*100 + WEEK(date(current_date) - interval '90' day)
					and base1.date_ >= date('2022-03-01')
					
							
					GROUP BY 1,2,3,4,5,6,7,8,9,10,11
					
					)base2
					
					GROUP BY 1,2,3,4,5       
         )peak on peak.city_group = drv.city_group and peak.start_time_slot = drv.start_time_slot and peak.end_time_slot = drv.end_time_slot 
         
         
    LEFT JOIN (SELECT base1.inflow_date
				,base1.city_group
				,base1.start_time_slot
				,base1.end_time_slot

				,count(distinct base1.uid) as total_order_inflow_overall
				,count(distinct case when source = 'order_delivery' then base1.uid else null end) as total_order_inflow_food
				,count(distinct case when source <> 'order_delivery' then base1.uid else null end) as total_order_inflow_ns
				,count(distinct case when source = 'order_delivery' then base1.user_id else null end) as total_unique_user_food
				,count(distinct case when base1.is_cancelled = 1 then base1.uid else null end) as total_order_cancelled_overall
				,count(distinct case when base1.is_cancelled = 1 and source = 'order_delivery' then base1.uid else null end) as total_order_cancelled_food
				,count(distinct case when base1.is_cancelled = 1 and source <> 'order_delivery' then base1.uid else null end) as total_order_cancelled_ns
				,count(distinct case when base1.is_cancelled = 0 and base1.assign_type = '4. Free Pick' then base1.uid else null end) as total_order_freepick_not_cancelled
				,count(distinct case when base1.is_cancelled = 0 and base1.assign_type = '4. Free Pick' and source = 'order_delivery' then base1.uid else null end) as total_order_freepick_not_cancelled_food
				,count(distinct case when base1.is_cancelled = 0 and base1.assign_type = '4. Free Pick' and source <> 'order_delivery' then base1.uid else null end) as total_order_freepick_not_cancelled_ns
				,count(distinct case when base1.is_cancelled = 1 and base1.assign_type = '4. Free Pick' then base1.uid else null end) as total_order_freepick_cancelled
				,count(distinct case when base1.is_cancelled = 1 and base1.assign_type = '4. Free Pick' and source = 'order_delivery' then base1.uid else null end) as total_order_freepick_cancelled_food
				,count(distinct case when base1.is_cancelled = 1 and base1.assign_type = '4. Free Pick' and source <> 'order_delivery' then base1.uid else null end) as total_order_freepick_cancelled_ns
				,count(distinct case when base1.is_cancelled = 0 and base1.assign_type = '6. New Stack Assign' then base1.uid else null end) as total_order_stack_not_cancelled
				
				FROM
						(
						SELECT base.shipper_id
							  ,base.city_name
							  ,base.city_group
							  ,base.report_date
							  ,base.created_date
							  ,date(base.inflow_timestamp) inflow_date
				
							  ,base.order_id
							  ,base.order_code
							  ,concat(base.source,'_',cast(base.order_id as varchar)) as uid
							  ,base.ref_order_category order_type
							  ,base.source
							  ,base.ref_order_status order_status
							  ,base.is_stack
							  ,base.group_code
							  ,base.group_id 
				
							  ,base.first_auto_assign_timestamp
							  ,base.last_delivered_timestamp
							  ,base.inflow_timestamp
							  ,EXTRACT(HOUR from base.inflow_timestamp) inflow_hour
							  ,date_add('hour',EXTRACT(HOUR from base.inflow_timestamp) ,cast(date(base.inflow_timestamp) as TIMESTAMP)) as start_time_slot
							  ,date_add('second',0,date_add('minute',60,date_add('hour',EXTRACT(HOUR from base.inflow_timestamp),cast(date(base.inflow_timestamp) as TIMESTAMP)))) as end_time_slot 
							  ,base.is_asap 
							  ,base.is_cancelled
							  ,base.assign_type
							  ,base.is_stack
							  ,base.is_delivered
							  ,base.user_id
							  
						FROM
									(
									SELECT dot.uid as shipper_id
										  ,dot.ref_order_id as order_id
										  ,dot.ref_order_code as order_code
										  ,dot.ref_order_category
										  ,case when dot.ref_order_category = 0 then 'order_delivery'
												when dot.ref_order_category = 3 then 'now_moto'
												when dot.ref_order_category = 4 then 'now_ship'
												when dot.ref_order_category = 5 then 'now_ship'
												when dot.ref_order_category = 6 then 'now_ship_shoppee'
												when dot.ref_order_category = 7 then 'now_ship_sameday'
												else null end source
										  ,dot.ref_order_status
										  ,case when dot.ref_order_category = 0 then 
										                case when dot.ref_order_status = 8 then 1 
										                else 0 
										                end
										        when dot.ref_order_status in (3,6,12) then 1
										        else 0 end as is_cancelled
										  
										  ,case when dot.ref_order_category = 0 then 
										            case when dot.ref_order_status = 7 then 1
										            else 0
										            end
										        when dot.ref_order_status = 11 then 1
										        else 0 end as is_delivered
										  
										  ,case when dot.is_asap = 0 and dot.ref_order_status in (7,11) then date(from_unixtime(dot.real_drop_time - 60*60)) else date(from_unixtime(dot.submitted_time- 60*60)) end as report_date
										  ,date(from_unixtime(dot.submitted_time- 60*60)) created_date
										  
										  ,case when dot.group_id > 0 then 1 else 0 end as is_stack
										  ,ogi.group_code
										  ,ogm.group_id 
										  ,ogi.distance*1.0000/(100*1000) overall_distance
										  ,dot.delivery_distance*1.0000/1000 delivery_distance
										  
										  ,from_unixtime(dot.real_drop_time - 60*60) last_delivered_timestamp
										  ,fa.first_auto_assign_timestamp 
										  ,dot.is_asap 
				
										  ,case when dot.is_asap = 0 then fa.first_auto_assign_timestamp else from_unixtime(dot.submitted_time- 60*60) end as inflow_timestamp
										  
										  ,case when dot.pick_city_id = 238 THEN 'Dien Bien' else city.city_name end as city_name
											,case when dot.pick_city_id = 217 then 'HCM'
												when dot.pick_city_id = 218 then 'HN'
												when dot.pick_city_id = 219 then 'DN'
												ELSE 'OTH' end as city_group
											,last_incharge.assign_type as assign_type
											,go.user_id
									
									FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 
									
									LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm on dot.group_id = ogm.group_id and dot.ref_order_id = ogm.ref_order_id and ogm.mapping_status = 11 and ogm.ref_order_category = dot.ref_order_category
												LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm_filter on dot.group_id = ogm.group_id and dot.ref_order_id = ogm_filter.ref_order_id and ogm_filter.mapping_status = 11 
																																						and ogm_filter.ref_order_category = dot.ref_order_category
																																						and ogm_filter.create_time >  ogm.create_time
											LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id = ogm.group_id
									
									left join (SELECT city_id
												,city_name
												
												from shopeefood.foody_mart__fact_gross_order_join_detail
												where from_unixtime(create_timestamp) between date(cast(now() - interval '120' day as TIMESTAMP)) and date(cast(now() - interval '1' hour as TIMESTAMP))
												
												GROUP BY city_id
														,city_name
												)city on city.city_id = dot.pick_city_id
									
									
									LEFT JOIN shopeefood.foody_mart__fact_gross_order_join_detail go on go.id = dot.ref_order_id and dot.ref_order_category = 0
									
									LEFT JOIN
									(
										SELECT   order_id , 0 as order_type
												,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
														
												from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
										
												group by 1,2
										
										UNION
										
										SELECT   ns.order_id, ns.order_type ,min(from_unixtime(create_time - 60*60)) first_auto_assign_timestamp
										
										FROM 
												( SELECT order_id, order_type , create_time
										
												 from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
												 where order_type in (4,5,6,7) -- now ship/ns shopee/ ns same day
													
												 UNION
											
												 SELECT order_id, order_type, create_time
											
												 from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
												 where order_type in (4,5,6,7) -- now ship/ns shopee/ ns same day
												 )ns
										GROUP BY 1,2
									)fa on dot.ref_order_id = fa.order_id and dot.ref_order_category = fa.order_type
									
									
									LEFT JOIN
									            (SELECT a.order_uid
                                                    ,a.order_id
                                                    ,a.order_type  
                                                    ,case when a.assign_type = 1 then '1. Single Assign'
                                                          when a.assign_type in (2,4) then '2. Multi Assign'
                                                          when a.assign_type = 3 then '3. Well-Stack Assign'
                                                          when a.assign_type = 5 then '4. Free Pick'
                                                          when a.assign_type = 6 then '5. Manual'
                                                          when a.assign_type in (7,8) then '6. New Stack Assign'
                                                          else null end as assign_type
									            
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
									            
									            
									                where 1=1
                                                    and a_filter.order_id is null -- take last incharge
                                                    -- and a.order_id = 109630183
                                                    
                                                    GROUP BY 1,2,3,4
									            
									            )last_incharge on dot.ref_order_id = last_incharge.order_id and dot.ref_order_category = last_incharge.order_type
									
									
									WHERE 1=1
									and ogm_filter.create_time is null
									
									--and dot.ref_order_status in (7,11)
									
									)base
						
						WHERE 1=1
						--and base.created_date = date('2020-11-11')
						)base1
				
				Where 1=1 
				and base1.inflow_date >= current_date - interval '120' day
			 -- >= date('2020-12-01')
				
				GROUP BY 1,2,3,4
    
            )vol on vol.city_group = drv.city_group and vol.start_time_slot = drv.start_time_slot and vol.end_time_slot = drv.end_time_slot

where drv.date_ = date'2022-04-27'