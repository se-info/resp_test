with rate as 
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

left join (select * from shopeefood.shopeefood_mart_cdm_dwd_vn_rating_rating_driver_da where date(dt) = current_date - interval '1' day) rate
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
and rating_star is not null)
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
,f as 
(select 
        m.*,
        pp_online/cast(working_days as double) as ave_pp_online,
        case 
        when pp_online/cast(working_days as double) > 5.733626 then 3 
        when pp_online/cast(working_days as double) between 4.502711 and 5.733626 then 2 
        else 1 end as working_type,
        avg(rate.avg_rating) as pp_rating

from
(select 
        m.*,
        sum(coalesce(hp.active_slot,0)) as pp_active,
        sum(coalesce(hp.pass_kpi,0)) as pp_pass_kpi,
        sum(coalesce(hp.total_order,0)) as total_order,
        sum(coalesce(hp.total_registered,0)) as total_registered,
        -- coalesce(count(distinct case when hp.total_order > 0  then hp.slot_id else null end)/cast(count(distinct case when hp.registered_ > 0  then hp.slot_id else null end) as double),0) as pp_active,
        -- coalesce(count(distinct case when hp.kpi > 0  then hp.slot_id else null end)/cast(count(distinct case when hp.total_order > 0  then hp.slot_id else null end) as double),0) as pp_pass_kpi,
        coalesce(sum(coalesce(hp.in_shift_online_time,0)),0) as pp_online,
        sum(coalesce(late_order,0)) as pp_late,
        count(distinct case when total_order > 0 then hp.date_ else null end) as working_days, 
        sum(case when hp.date_ = m.end_date then coalesce(hp.total_order,0) else null end) as daily_order
        
from
(select 
        date_ - interval '29' day as start_date,
        date_ as end_date,
        uid


from dev_vnfdbi_opsndrivers.driver_ops_hub_driver_performance_tab
where total_order > 0             
and city_name in ('HCM City','Ha Noi City')     
group by 1,2,3
) m 

left join hub_performance hp 
        on hp.uid = m.uid 
        and hp.date_ between m.start_date and m.end_date
group by 1,2,3
) m 

left join rate 
        on rate.uid = m.uid
        and rate.created_date between m.start_date and m.end_date
where end_date between date'2023-11-20' and date'2023-11-26'
group by 1,2,3,4,5,6,7,8,9,10,11
)
,m as 
(select 
        *,
        case 
        when slr >= 0 and slr < 40 then 1 
        when slr >= 40 and slr < 60 then 2 
        when slr >= 60 and slr < 80 then 3
        when slr >= 80 and slr <= 95 then 4
        when slr > 95 then 5 end as tier


from 
(select 
        f.*,
        ( (pp_active/cast(total_registered as double)) *1 
                + (pp_pass_kpi/cast(pp_active as double)) * 1 
                + (pp_online/cast(8*30 as double)) * 2 
                + (coalesce(pp_rating/cast(5 as double),1)) * 0.5 
                + ((total_order - pp_late)/cast(total_order as double)) * 0.5)
                /cast(5 as double)*100 as slr,      
        dense_rank()over(partition by end_date order by (working_type,ave_pp_online) desc) as ranking
from f 
)
)
,summary as 
(select 
        *,
        case 
        when slr_v2 >= 0 and slr_v2 < 40 then 1 
        when slr_v2 >= 40 and slr_v2 < 60 then 2 
        when slr_v2 >= 60 and slr_v2 < 80 then 3
        when slr_v2 >= 80 and slr_v2 <= 95 then 4
        when slr_v2 > 95 then 5 end as tier_v2

from 
(select
        *,
                -- assume 30pp driver/tier being improved 
        case when pp_re_rank <= 0.45 then 
                -- buffer 10% for active slot 
        (((least(pp_active *1.1,total_registered))/cast(total_registered as double)) *1
                -- buffer 10% for pass kpi  
                + (least(pp_pass_kpi*1.1,pp_active)/cast(pp_active as double)) * 1 
                + (pp_online/cast(8*30 as double)) * 2 
                + (coalesce(pp_rating,1)/cast(5 as double)) * 0.5 
                + ((total_order - pp_late)/cast(total_order as double)) * 0.5)
                /cast(5 as double)*100 
        else slr end
        as slr_v2

from
(select 
        m.*,
        dense_rank()over(partition by end_date,tier order by ranking) as re_rank,
        count(uid)over(partition by tier,end_date) as a1,
        dense_rank()over(partition by end_date,tier order by ranking)/
        cast(count(uid)over(partition by tier,end_date) as double) as pp_re_rank

from m )
)
order by slr_v2 desc )
select * from summary




