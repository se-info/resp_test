with online_check as 
(select
        created,
        uid,
        sum(h11_online_time) as h11
from 
(select 
         uid
        ,date(from_unixtime(create_time - 3600)) as created
        ,from_unixtime(checkin_time - 3600) as checkin
        ,from_unixtime(checkout_time - 3600) as checkout  
        ,cast(date(from_unixtime(checkin_time - 60*60)) as TIMESTAMP) + interval '11' hour as h11_start
        ,cast(date(from_unixtime(checkin_time - 60*60)) as TIMESTAMP) + interval '12' hour as h11_end
        ,case 
        when from_unixtime(checkout_time - 3600) < cast(date(from_unixtime(checkin_time - 60*60)) as TIMESTAMP) + interval '11' hour then 0
        when from_unixtime(checkin_time - 3600) > cast(date(from_unixtime(checkin_time - 60*60)) as TIMESTAMP) + interval '12' hour then 0
        else date_diff('second',
            greatest(cast(date(from_unixtime(checkin_time - 60*60)) as TIMESTAMP) + interval '11' hour,from_unixtime(checkin_time - 3600)),   
            least(cast(date(from_unixtime(checkin_time - 60*60)) as TIMESTAMP) + interval '12' hour,from_unixtime(checkout_time - 3600))   
            )*1.0000/(60*60)
        end as h11_online_time


from shopeefood.foody_partner_db__shipper_checkin_checkout_log_tab__reg_daily_s0_live 
where 1 = 1 
and date(from_unixtime(create_time - 3600)) = date'2024-01-28'
)
group by 1,2
)
select 
        raw.uid,
        raw.date_,
        kpi_failed,
        oc.h11,
        raw.hub_type_original,
        raw.total_income,
        case 
        when raw.hub_type_original = '8 hour shift' and oc.h11 = 1 and raw.in_shift_online_time >= 4 and raw.total_order < 25 and ac.no_ignored = 0 and ac.no_deny = 0 
        then (25*13500) - (raw.total_order * 13500)
        when raw.hub_type_original = '10 hour shift' and oc.h11 = 1 and raw.in_shift_online_time >= 5 and raw.total_order < 30 and ac.no_ignored = 0 and ac.no_deny = 0 
        then (30*13500) - (raw.total_order * 13500) 
        else 0 end as compensation_manual,
        ac.no_ignored,
        ac.no_deny,
        ac.order_info

from driver_ops_hub_driver_performance_tab  raw 

left join online_check oc 
    on oc.uid = raw.uid 
    and oc.created = raw.date_  

left join 
(select 
         date(sa.create_time) as created
        ,sa.driver_id
        ,h.hub_type_x_start_time
        ,array_agg(distinct case when status in (8,9,17,18,2,14,15) then order_code else null end) as order_info
        ,count(distinct case when status in (3,4,2,14,15,8,9,17,18) then (driver_id,order_code,create_time) else null end) as no_assign
        ,count(distinct case when status in (3,4) then (driver_id,order_code,create_time) else null end) as no_incharged
        ,count(distinct case when status in (8,9,17,18) then (driver_id,order_code,create_time) else null end) as no_ignored
        ,count(distinct case when status in (2,14,15) then (driver_id,order_code,create_time) else null end) as no_deny

from driver_ops_order_assign_log_tab sa  

left join (select * from driver_ops_hub_driver_performance_tab where registered_ = 1) h 
       on sa.driver_id = h.uid 
       and date(sa.create_time) = h.date_
       and sa.create_time between h.start_shift_time and h.end_shift_time

where date(create_time) = date'2024-01-28' 
and hour(create_time) >= 10 
and hour(create_time) <= 15
and status in (3,4,2,14,15,8,9,17,18) 
and h.hub_type_original is not null
group by 1,2,3
) ac on ac.driver_id = raw.uid and ac.created = raw.date_ and ac.hub_type_x_start_time = raw.hub_type_x_start_time

where 1 = 1 
and raw.date_ = date'2024-01-28'
                                   
and raw.in_shift_online_time > 0
and regexp_like(raw.hub_type_original,'10 hour|8 hour') = true
and regexp_like(raw.kpi_failed,'Online peak hour|Online in shift') = true
and regexp_like(raw.kpi_failed,'Auto Accept') = false
and raw.extra_ship = 0