with slr_tier_config_tab(slr_from,slr_to,slr_tier) as 
(VALUES 
(0,0.5,1),
(0.5,0.65,2),
(0.65,0.85,3),
(0.85,0.95,4),
(0.95,1,5)
)
,points_tier_config_tab(points_from,points_to,point_tier) as 
(VALUES 
(0,300,1),
(300,800,2),
(800,1500,3),
(1500,2400,4),
(2400,999999,5)
)
,rate as 
(select 
        created_date,
        shipper_id as uid,
        sum(rating_star)/cast(count(distinct id) as double) as avg_rating
    
from    
(select  
        raw.id,
        raw.created_date,
        rate.rating_star,
        raw.city_name,
        hub.slot_id,
        raw.shipper_id


from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab raw 

left join 
(select 
        *,
        case when cfo.shipper_rate = 0 then null
        when cfo.shipper_rate = 1 or cfo.shipper_rate = 101 then 1
        when cfo.shipper_rate = 2 or cfo.shipper_rate = 102 then 2
        when cfo.shipper_rate = 3 or cfo.shipper_rate = 103 then 3
        when cfo.shipper_rate = 104 then 4
        when cfo.shipper_rate = 105 then 5
        else null end as rating_star 

from shopeefood.foody_user_activity_db__customer_feedback_order_tab__reg_daily_s0_live cfo
    ) rate
    on raw.id = rate.order_id 
    and raw.order_type = 0
left join shopeefood.foody_partner_archive_db__shipper_hub_order_tab__reg_continuous_s0_live hub 
    on hub.ref_order_id = raw.id
    and hub.ref_order_category = raw.order_type

left join shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hi 
    on hi.id = hub.autopay_report_id

where driver_policy = 2 
and raw.order_status = 'Delivered'
and raw.shipper_id > 0 
and raw.city_id in (217,218,220)
and hub.slot_id is not null
and rating_star is not null
)
group by 1,2
)
,hub_performance as 
(select 
        date_,
        uid,
        sum(case when total_order > 0 then in_shift_online_time else null end) as in_shift_online_time,
        sum(coalesce(total_order,0)) as total_order,
        count(distinct slot_id) as total_registered,
        count(distinct case when total_order > 0 then slot_id else null end) as active_slot,
        count(distinct case when total_order > 0 and kpi = 1 then slot_id else null end) as pass_kpi,
        sum(late_rate*total_order) as late_order

from dev_vnfdbi_opsndrivers.driver_ops_hub_driver_performance_tab
            
where registered_ = 1
group by 1,2
)
,online_performance as
(SELECT 
        base.report_date
                                                
        ,case 
        when day_of_week(base.report_date) IN (6,7) then 1 else 0 end as is_weekend
        ,base.shipper_id
        ,base.hub_type
        ,base.slot_id
                
                                                                                                                             
                                                                                                                          
                 
        ,case when base.actual_end_time_online < base.start_shift_time then 0
            when base.actual_start_time_online > base.end_shift_time then 0
            else date_diff('second',   greatest(base.start_shift_time,base.actual_start_time_online)   ,   least(base.end_shift_time,base.actual_end_time_online)   )*1.0000/(60*60)
            end as in_shift_online_time

        ,case when base.actual_end_time_work < base.start_shift_time then 0
            when base.actual_start_time_work > base.end_shift_time then 0
            else date_diff('second',   greatest(base.start_shift_time,base.actual_start_time_work)   ,   least(base.end_shift_time,base.actual_end_time_work)   )*1.0000/(60*60)
            end as in_shift_work_time
            
        ,case when base.actual_end_time_online < base.h11_start then 0
            when base.actual_start_time_online > base.h11_end then 0
            else date_diff('second',   greatest(base.h11_start,base.actual_start_time_online)   ,   least(base.h11_end,base.actual_end_time_online)   )*1.0000/(60*60)
            end as h11_online_time
        ,case when base.actual_end_time_online < base.h18_start then 0
            when base.actual_start_time_online > base.h18_end then 0
            else date_diff('second',   greatest(base.h18_start,base.actual_start_time_online)   ,   least(base.h18_end,base.actual_end_time_online)   )*1.0000/(60*60)
            end as h18_online_time


FROM
(SELECT 
         sts.uid as shipper_id
        ,shift.slot_id 
        ,date(from_unixtime(create_time - 60*60)) as report_date
        ,CASE WHEN shift.shift_hour = 10 then '10 hour shift'
        WHEN shift.shift_hour = 8 then '8 hour shift'
        WHEN shift.shift_hour = 5 then '5 hour shift'
        WHEN shift.shift_hour = 3 then '3 hour shift'
        ELSE null END as hub_type
                              
        ,from_unixtime(check_in_time - 60*60) as check_in_time
        ,from_unixtime(check_out_time - 60*60) as check_out_time
        ,from_unixtime(order_start_time - 60*60) as order_start_time
        ,from_unixtime(order_end_time - 60*60) as order_end_time

        ,from_unixtime(check_in_time - 60*60) as actual_start_time_online
        ,greatest(from_unixtime(check_out_time - 60*60),from_unixtime(order_end_time - 60*60)) as actual_end_time_online

        ,case when order_start_time = 0 then from_unixtime(check_in_time - 60*60) else from_unixtime(order_start_time - 60*60) end as actual_start_time_work
        ,case when order_end_time = 0 then from_unixtime(check_in_time - 60*60) else from_unixtime(order_end_time - 60*60) end as actual_end_time_work
        ,date_add('hour',shift.start_shift,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP)) as start_shift_time
        ,date_add('hour',shift.end_shift,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP)) as end_shift_time
        ,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '11' hour as h11_start
        ,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '12' hour as h11_end
        ,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '18' hour as h18_start
        ,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP) + interval '19' hour as h18_end
from shopeefood.foody_internal_db__shipper_time_sheet_tab__reg_daily_s0_live sts

left join 
(
SELECT 
         date(from_unixtime(date_ts - 3600)) as date_
        ,uid
        ,slot_id
        ,case when registration_status = 1 then 'Registered'
            when registration_status = 2 then 'OFF'
            when registration_status = 3 then 'Worked'
            end as registration_status
        ,(end_time - start_time)/3600 as shift_hour
        ,start_time/3600 as start_shift
        ,end_time/3600 as end_shift
        ,date_add('hour',(start_time/3600),cast(date(from_unixtime(date_ts - 60*60)) as TIMESTAMP)) as start_time
        ,date_add('hour',(end_time/3600),cast(date(from_unixtime(date_ts - 60*60)) as TIMESTAMP)) as end_time

from shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live
where registration_status != 2 
)shift on shift.uid = sts.uid 
        and shift.date_ = date(from_unixtime(create_time - 60*60))
        and from_unixtime(create_time - 3600) between shift.start_time and shift.end_time

where 1 = 1 

)base
where hub_type is not null
and report_date >= current_date - interval '90' day

group by 1,2,3,4,5,6,7,8,9

)
,online_final as 
(select 
    report_date,
    shipper_id,
    sum(in_shift_online_time) as total_online,
    sum(case when is_weekend = 1 then h11_online_time else 0 end) as online_lunch_weekend,
    sum(case when is_weekend = 1 then h18_online_time else 0 end) as online_dinner_weekend,
    sum(case when is_weekend = 1 then in_shift_online_time else 0 end) 
        - sum(case when is_weekend = 1 then h11_online_time else 0 end) - sum(case when is_weekend = 1 then h18_online_time else 0 end) as online_off_peak_weekend,

    sum(case when is_weekend = 0 then h11_online_time else 0 end) as online_lunch_weekday,
    sum(case when is_weekend = 0 then h18_online_time else 0 end) as online_dinner_weekday,
    sum(case when is_weekend = 0 then in_shift_online_time else 0 end) 
        - sum(case when is_weekend = 0 then h11_online_time else 0 end) - sum(case when is_weekend = 0 then h18_online_time else 0 end) as online_off_peak_weekday

from online_performance 

group by 1,2)
,value_tab as 
(select 
        m.*,
        SUM(op.total_online) AS total_online,
        SUM(op.online_lunch_weekend) AS online_lunch_weekend,
        SUM(op.online_dinner_weekend) AS online_dinner_weekend,
        SUM(op.online_off_peak_weekend) AS online_off_peak_weekend,
        SUM(op.online_lunch_weekday) AS online_lunch_weekday,
        SUM(op.online_dinner_weekday) AS online_dinner_weekday,
        SUM(op.online_off_peak_weekday) AS online_off_peak_weekday

from
(select 
        m.*,                             
        avg(rate.avg_rating) as pp_rating

from
(select 
        m.*,
        sum(coalesce(hp.active_slot,0)) as pp_active,
        sum(coalesce(hp.pass_kpi,0)) as pp_pass_kpi,
        sum(coalesce(hp.total_order,0)) as total_order,
        sum(coalesce(hp.total_registered,0)) as total_registered,
                                                                                                                                                                                                            
                                                                                                                                                                                                      
                                                                             
        sum(coalesce(late_order,0)) as pp_late,
        count(distinct case when total_order > 0 then hp.date_ else null end) as working_days, 
        sum(case when hp.date_ = m.end_date then coalesce(hp.total_order,0) else null end) as daily_order

        
from
(select 
        report_date - interval '29' day as start_date,
        report_date as end_date,
        shipper_id as uid,
        city_name,
        total_order as total_order_end_date

from dev_vnfdbi_opsndrivers.driver_ops_driver_performance_tab
where 1 = 1 
and city_name in ('HCM City','Ha Noi City','Hai Phong City')
and shipper_type = 12     
                 
) m 

left join hub_performance hp 
        on hp.uid = m.uid 
        and hp.date_ between m.start_date and m.end_date


group by 1,2,3,4,5
) m 

left join rate 
        on rate.uid = m.uid
        and rate.created_date between m.start_date and m.end_date
group by 1,2,3,4,5,6,7,8,9,10,11,12

) m 
left join online_final op 
    on op.shipper_id = m.uid 
    and op.report_date between m.start_date and m.end_date

where end_date = ${cut_of_date}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)
select 
        base.*,
        sc.slr_tier,
        pc.point_tier,
        least(sc.slr_tier,pc.point_tier) as final_tier

from 
(select 
        *,
        (try(pp_pass_kpi*1.0000/pp_active)*2 + -- kpi,
        try(pp_active*1.0000/total_registered)*2 + -- active 
        IF(total_order=0,1,1 - try(pp_late*try(1.0000/total_order)))/2 + --  ontime,
        IF(pp_rating IS NULL,1,try(pp_rating*1.0000/5))/2 -- rating
        )/5 as slr,
        coalesce(total_online,0)*10 as total_points
        
from value_tab
) base

left join slr_tier_config_tab sc on base.slr > sc.slr_from
                                and base.slr <= sc.slr_to

left join points_tier_config_tab pc on base.total_points > pc.points_from
                                and base.total_points <= pc.points_to


