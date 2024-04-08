-- DROP TABLE IF EXISTS dev_vnfdbi_opsndrivers.phong_on_going_orders_by_hour;
-- CREATE TABLE IF NOT EXISTS dev_vnfdbi_opsndrivers.phong_on_going_orders_by_hour AS 
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
      ,t2.dt_array_unnest as current_vn_datetime_from 
      ,t2.dt_array_unnest + interval '1798.99' second as current_vn_datetime_to 



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
                        ,base.order_status
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
                        ,base.city_group     
                        ,base.final_order_status           


                from
                        (
                        select   dot.uid as shipper_id
                                ,dot.ref_order_id as order_id
                                ,dot.ref_order_code as order_code
                                ,dot.ref_order_category
                                ,dot.ref_order_status	
                                ,dot.order_status									  
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
                                ,case when dot.pick_city_id = 238 then 'dien bien' else city.city_name end as city_name
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
,raw2 as
(select 
    cast(hour(t1.current_vn_datetime_from) as varchar ) || cast(minute(t1.current_vn_datetime_from) as varchar ) as lookup_value
    ,date(t1.current_vn_datetime_from) as date_
    ,hour(t1.current_vn_datetime_from) as hour_
    ,t1.current_vn_datetime_from
    ,t1.current_vn_datetime_to
    ,t2.order_category
    -- , t1.current_sg_time, t1.current_sg_unixtime
    ,t2.city_group
    ,count(case when t2.inflow_timestamp >= t1.current_vn_datetime_from and t2.inflow_timestamp <= t1.current_vn_datetime_to then order_id else null end) as total_gross_order
    ,count(case when final_order_status = 'Cancelled' and t2.final_status_timestamp >= t1.current_vn_datetime_from and t2.final_status_timestamp <= t1.current_vn_datetime_to then order_id else null end) as total_cancel_order

    ,count(distinct case 
        when t1.current_vn_datetime_from <= t2.inflow_timestamp and t2.final_status_timestamp <= t1.current_vn_datetime_to then order_id
        when t2.inflow_timestamp <= t1.current_vn_datetime_from and t1.current_vn_datetime_from <= t2.final_status_timestamp and t2.final_status_timestamp <= t1.current_vn_datetime_to then order_id
        when t1.current_vn_datetime_from <= t2.inflow_timestamp and t2.inflow_timestamp <= t1.current_vn_datetime_to and t1.current_vn_datetime_to <= t2.final_status_timestamp then order_id
        when t2.inflow_timestamp <= t1.current_vn_datetime_from and t1.current_vn_datetime_to <= t2.final_status_timestamp then order_id
        else null end) as total_ongoing_order

    ,count(distinct case 
        when final_order_status = 'Cancelled' and t1.current_vn_datetime_from <= t2.inflow_timestamp and t2.final_status_timestamp <= t1.current_vn_datetime_to then order_id
        when final_order_status = 'Cancelled' and t2.inflow_timestamp <= t1.current_vn_datetime_from and t1.current_vn_datetime_from <= t2.final_status_timestamp and t2.final_status_timestamp <= t1.current_vn_datetime_to then order_id
        when final_order_status = 'Cancelled' and t1.current_vn_datetime_from <= t2.inflow_timestamp and t2.inflow_timestamp <= t1.current_vn_datetime_to and t1.current_vn_datetime_to <= t2.final_status_timestamp then order_id
        when final_order_status = 'Cancelled' and t2.inflow_timestamp <= t1.current_vn_datetime_from and t1.current_vn_datetime_to <= t2.final_status_timestamp then order_id
        else null end) as total_cancel_ongoing_order
        
    
    --   ,count(distinct order_code) as total_ongoing_order 
    --   ,count(distinct case when t1.current_sg_unixtime >= t2.first_auto_assign_unixtime and t1.current_sg_unixtime < t2.last_incharge_unixtime then order_code else null end) total_driver_assigning_order

from raw t2 
left join date_time t1 
    on t2.mapping = t1.mapping
group by 1,2,3,4,5,6,7
)
select 
    --  raw2.lookup_value
     raw2.date_
    ,raw2.hour_
    ,raw2.current_vn_datetime_from
    ,raw2.current_vn_datetime_to
    ,raw2.city_group
    ,raw2.order_category
    ,raw2.total_ongoing_order
    -- ,overall.total_inflow_orders
    ,raw2.total_cancel_ongoing_order
    ,raw2.total_gross_order
    ,raw2.total_cancel_order
from raw2
