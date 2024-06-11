with report_date_time as 
(
SELECT
     DATE(report_date) AS report_date
    ,sequence(cast(t.report_date as timestamp ) ,cast(t.report_date as timestamp) + interval '86400' second, interval '1800' second  ) dt_array 
    ,1 as mapping
FROM
    (
(
SELECT sequence(current_date - interval '7' day, current_date - interval '1' day) bar)
CROSS JOIN

    unnest (bar) as t(report_date)
)
)
,date_time as 
(select 
       t1.mapping
      ,t2.dt_array_unnest as start_time 
      ,t2.dt_array_unnest + interval '1799.99' second as end_time 



from report_date_time t1 

cross join unnest (dt_array) as t2(dt_array_unnest) 

order by 2 asc)

-- select 
--     *
-- from date_time

, raw as 
        (
                select 
                         1 mapping
                        ,base.created_date
                        ,date(base.inflow_timestamp) inflow_date				
                        ,base.order_id
                        ,base.order_code
                        -- ,base.order_status
                        ,case when base.order_status = 400 then 'Delivered'
                              when base.order_status = 401 then 'Quit'
                              when base.order_status in (402,403,404) then 'Cancelled'
                              when base.order_status in (405) then 'Returned'
                              else 'Others' end as order_status
                        ,base.created_timestamp
                        ,base.first_auto_assign_timestamp
                        ,base.last_incharge_timestamp
                        ,base.last_delivered_timestamp
                        ,base.inflow_timestamp	
                        ,from_unixtime(base.final_status_unixtime - 60*60) final_status_timestamp	   
                        ,base.is_asap 

                        ,base.created_unixtime
                        ,base.first_auto_assign_unixtime
                        ,base.last_incharge_unixtime
                        ,base.canceled_unixtime
                        ,base.quit_unixtime
                        ,base.delivered_unixtime
                        ,base.final_status_unixtime      
                        ,base.order_category
                        ,base.city_name     
                        ,base.final_order_status
                        ,case when base.distance <= 3 then '1. 0 - 3km'
                              when base.distance >3 then '2. > 3km'
                              end as distance_range             


                from
                        (
                        select   dot.uid as shipper_id
                                ,dot.ref_order_id as order_id
                                ,dot.ref_order_code as order_code
                                ,dot.ref_order_category
                                ,dot.ref_order_status	
                                ,dot.order_status
                                ,(dot.delivery_distance)/cast(1000 as double) as distance									  
                                ,case when dot.is_asap = 0 and dot.ref_order_status in (7,11) then date(from_unixtime(dot.real_drop_time - 60*60)) else date(from_unixtime(dot.submitted_time- 60*60)) end as report_date
                                ,date(from_unixtime(dot.submitted_time- 60*60)) created_date	
                                ,from_unixtime(dot.submitted_time- 60*60) created_timestamp																		  
                                ,from_unixtime(case when dot.real_drop_time > 0 then dot.real_drop_time - 60*60
                                                    when dot.real_drop_time = 0 and order_status = 400 then fa.delivered_unixtime - 60*60
                                                    else null end) last_delivered_timestamp
                                ,fa.first_auto_assign_timestamp 
                                ,fa.last_incharge_timestamp
                                ,dot.is_asap 		
                                ,coalesce(fa.first_auto_assign_timestamp,from_unixtime(dot.submitted_time- 60*60)) as inflow_timestamp										  
                                ,case when dot.pick_city_id = 238 then 'Dien Bien' else city.city_name end as city_name
                                ,case when dot.pick_city_id = 217 then 'HCM'
                                      when dot.pick_city_id = 218 then 'HN'
                                      when dot.pick_city_id = 219 then 'DN'
                                      else 'OTH' end as city_group
                                ,dot.submitted_time as created_unixtime
                                ,coalesce(fa.first_auto_assign_unixtime,9999999999) as first_auto_assign_unixtime
                                ,coalesce(fa.last_incharge_unixtime,9999999999) as last_incharge_unixtime
                                -- ,coalesce(fa.delivered_unixtime,9999999999) as delivered_unixtime
                                ,coalesce(fa.canceled_unixtime,9999999999) as canceled_unixtime
                                ,coalesce(fa.quit_unixtime,9999999999) as quit_unixtime
                                ,case when dot.real_drop_time > 0 then dot.real_drop_time 
                                      when dot.real_drop_time = 0 and order_status = 400 then coalesce(fa.delivered_unixtime,9999999999)
                                      else 9999999999 end as delivered_unixtime
                                ,case when order_status = 400 and dot.real_drop_time > 0 then dot.real_drop_time 
                                      when order_status = 400 and dot.real_drop_time = 0 then coalesce(fa.delivered_unixtime,9999999999)
                                      when order_status in (402,403,404) then coalesce(fa.canceled_unixtime,9999999999) 
                                      when order_status = 401 then coalesce(fa.quit_unixtime,9999999999) else null end as final_status_unixtime
                                ,case when dot.ref_order_category = 0 then '1. Food/Market' else '2. SPXI' end as order_category
                                ,case when order_status = 400 then 'Delivered' else 'Cancelled' end as final_order_status

                        from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 
                        
                        left join shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct ON dot.ref_order_category = 0 AND oct.id = dot.ref_order_id
                        left join (select city_id
                                    ,city_name

                                    from shopeefood.foody_mart__fact_gross_order_join_detail
                                    where from_unixtime(create_timestamp) between date(cast(now() - interval '30' day as timestamp)) and date(cast(now() - interval '1' hour as timestamp))

                                    group by city_id
                                            ,city_name
                                    )city on city.city_id = dot.pick_city_id


                        left join
                        (
                            select   order_id , 0 as order_type
                                    ,min(case when status = 21 then cast(from_unixtime(create_time) as timestamp) - interval '1' hour else null end) as first_auto_assign_timestamp
                                    ,max(case when status = 11 then cast(from_unixtime(create_time) as timestamp) - interval '1' hour else null end) as last_incharge_timestamp	
                                    ,coalesce(min(case when status = 21 then create_time else null end),9999999999) as first_auto_assign_unixtime
                                    ,coalesce(max(case when status = 11 then create_time else null end),9999999999) as last_incharge_unixtime
                                    ,coalesce(max(case when status = 7 then create_time else null end),9999999999) as delivered_unixtime
                                    ,coalesce(max(case when status = 8 then create_time else null end),9999999999) as canceled_unixtime
                                    ,coalesce(max(case when status = 9 then create_time else null end),9999999999) as quit_unixtime

                            from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live

                            group by 1,2

                        )fa on dot.ref_order_id = fa.order_id and dot.ref_order_category = fa.order_type

                        where 1=1
                        -- and dot.ref_order_category = 0
                        and dot.pick_city_id not in (0,238,468,469,470,471,472)
                        and dot.drop_city_id not in (0,238,468,469,470,471,472)
                        )base

                where date(base.inflow_timestamp) between current_date - interval '7' day and current_date - interval '1' day 
                -- and base.order_id in (5085914,5085942)
                -- limit 10
)
select 
         date_ 
        ,start_time
        ,end_time
        ,city_name
        ,order_category
        -- ,distance_range
        ,count(distinct order_code) as total_gross
        ,count(distinct case when order_status = 'Delivered' then order_code else null end) as total_net
        ,count(distinct case when order_status = 'Delivered' and distance_range = '1. 0 - 3km' then order_code else null end)/cast(count(distinct case when order_status = 'Delivered' then order_code else null end) as double) as percent_under_3km
        ,count(distinct case when order_status = 'Delivered' and distance_range = '2. > 3km' then order_code else null end)/cast(count(distinct case when order_status = 'Delivered' then order_code else null end) as double) as percent_over_3km

from 
(select 
         date(dt.start_time) as date_ 
        ,dt.start_time
        ,dt.end_time
        ,raw.* 




from raw 

inner join date_time dt on raw.inflow_timestamp between dt.start_time and dt.end_time
)

where (hour(start_time) between 10 and 12 or hour(start_time) between 17 and 19)
-- and city_name not in ('HCM City','Ha Noi City')

group by 1,2,3,4,5
