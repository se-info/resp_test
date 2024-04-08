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
        sum(in_shift_online_time) as in_shift_online_time,
        sum(coalesce(total_order,0)) as total_order,
        count(distinct slot_id) as total_registered,
        count(distinct case when total_order > 0 then slot_id else 0 end) as active_slot,
        count(distinct case when total_order > 0 and kpi = 1 then slot_id else 0 end) as pass_kpi,
        sum(late_rate*total_order) as late_order

from dev_vnfdbi_opsndrivers.driver_ops_hub_driver_performance_tab
            
where registered_ = 1
group by 1,2
)
,metrics as 
(select 
        m.*,
        avg(rate.avg_rating)/cast(5 as double) as pp_rating

from
(select 
        m.*,
        sum(coalesce(hp.active_slot,0))/cast(sum(coalesce(hp.total_registered,0)) as double) as pp_active,
        sum(coalesce(hp.pass_kpi,0))/cast(sum(coalesce(hp.active_slot,0)) as double) as pp_pass_kpi,
        -- coalesce(count(distinct case when hp.total_order > 0  then hp.slot_id else null end)/cast(count(distinct case when hp.registered_ > 0  then hp.slot_id else null end) as double),0) as pp_active,
        -- coalesce(count(distinct case when hp.kpi > 0  then hp.slot_id else null end)/cast(count(distinct case when hp.total_order > 0  then hp.slot_id else null end) as double),0) as pp_pass_kpi,
        coalesce(sum(coalesce(hp.in_shift_online_time,0))/cast(8*30 as double),0) as pp_online,
        1 - sum(coalesce(late_order,0))/cast(sum(coalesce(hp.total_order,0)) as double) as pp_late
from
(select 
        date_ - interval '29' day as start_date,
        date_ as end_date,
        uid


from dev_vnfdbi_opsndrivers.driver_ops_hub_driver_performance_tab
where total_order > 0                  
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

group by 1,2,3,4,5,6,7
)
select
        *,
        (pp_active*1 + pp_pass_kpi * 2 + least(pp_online,1) * 1 + pp_rating * 0.5 + pp_late * 0.5)/cast(5 as double) as slr,
        (pp_active*1 + pp_pass_kpi * 2 + pp_online * 1 + pp_rating * 0.5 + pp_late * 0.5)/cast(5 as double) as slr_v2

from metrics 
where end_date between current_date - interval '7' day and current_date - interval '1' day