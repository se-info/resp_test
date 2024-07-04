-- select 
--     from_unixtime(min(create_time-3600))
--     ,from_unixtime(max(create_time-3600))
-- from shopeefood.foody_partner_db__order_point_log_tab__reg_daily_s0_live
-- limit 10

-- select 
--     *
-- from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da
-- limit 10

with base as
(select 
    o.shipper_id
    ,o.created_date
    ,o.ref_order_id
    ,p.point
    ,o.source
    ,sm.city_name
    ,case 
        when sm.shipper_type_id = 12 then 'hub' else 'non-hub' end as shipper_type
    ,o.source
    ,from_unixtime(p.create_time-3600) as create_timestamp
    ,restaurant_id
    ,case 
        when ${check_merchant_level} = 1 then 
            case 
                when source in (${source}) and hour(from_unixtime(p.create_time-3600)) between ${hour_from} and ${hour_to} 
                and restaurant_id in (${list_restaurant_id}) then 1 else 0 end 
        else 
            case 
                when source in (${source}) and hour(from_unixtime(p.create_time-3600)) between ${hour_from} and ${hour_to} then 1 else 0 end 
        end as order_flag
from shopeefood_vn_bnp_order_performance_dev o
left join shopeefood.foody_partner_db__order_point_log_tab__reg_daily_s0_live p
    on o.ref_order_id = p.order_id and date(o.created_date) = date(from_unixtime(p.create_time-3600))
left join shopeefood.foody_mart__profile_shipper_master sm
    on o.shipper_id = sm.shipper_id and o.created_date = try_cast(sm.grass_date as date)
left join shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
    on o.ref_order_id = oct.id and o.source = 'NowFood'
where o.created_date = date '2022-10-10'
and city_group in ('HCM','HN')
and sm.city_name in ('HCM City','Ha Noi City')
and o.order_status = 'Delivered'
)
,added_point_tab as
(select 
    created_date
    ,shipper_id
    ,shipper_type
    ,sum(point)
    ,sum(case when order_flag = 1 and point < ${threshold} then ${threshold} else point end) as point_added
    ,round((sum(case when order_flag = 1 and point < ${threshold} then ${threshold} else point end) - sum(point))*${order_buffer_rate},0) as additional_point
from base
group by 1,2,3
)
,raw as
(select 
   date(from_unixtime(bonus.report_date-3600)) as report_date
   ,bonus.uid as shipper_id
   ,case
        when bonus.tier in (1,6,11) then 'T1' -- as current_driver_tier
        when bonus.tier in (2,7,12) then 'T2'
        when bonus.tier in (3,8,13) then 'T3'
        when bonus.tier in (4,9,14) then 'T4'
        when bonus.tier in (5,10,15) then 'T5'
        else null end as current_driver_tier
    ,shipper_type
    ,bonus.total_point
    ,bonus.daily_point
    ,bonus.completed_rate/100.00 as sla
    ,bonus.bonus_value/100.00 as bonus_value
    ,a.additional_point
    ,case 
        when bonus.total_point - coalesce((bonus2.daily_point),0) + bonus.daily_point between 0 and 1800 then 'T1' 
        when bonus.total_point - coalesce((bonus2.daily_point),0) + bonus.daily_point between 1801 and 3600 then 'T2'
        when bonus.total_point - coalesce((bonus2.daily_point),0) + bonus.daily_point between 3601 and 5400 then 'T3'
        when bonus.total_point - coalesce((bonus2.daily_point),0) + bonus.daily_point between 5401 and 8400 then 'T4'
        when bonus.total_point - coalesce((bonus2.daily_point),0) + bonus.daily_point > 8401 then 'T5'
        end as tmr_driver_tier
    ,case 
        when (bonus.total_point + additional_point) - coalesce((bonus2.daily_point),0) + bonus.daily_point between 0 and 1800 then 'T1' 
        when (bonus.total_point + additional_point) - coalesce((bonus2.daily_point),0) + bonus.daily_point between 1801 and 3600 then 'T2'
        when (bonus.total_point + additional_point) - coalesce((bonus2.daily_point),0) + bonus.daily_point between 3601 and 5400 then 'T3'
        when (bonus.total_point + additional_point) - coalesce((bonus2.daily_point),0) + bonus.daily_point between 5401 and 8400 then 'T4'
        when (bonus.total_point + additional_point) - coalesce((bonus2.daily_point),0) + bonus.daily_point > 8401 then 'T5'
        end as new_driver_tier
    ,ceiling(bonus.total_point/100.00) as range_point
    ,(bonus.total_point + additional_point) as new_total_point
    ,(bonus.daily_point + additional_point) as new_daily_point
    ,coalesce((bonus2.daily_point),0) as daily_point_l30d
from shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus
left join added_point_tab a
    on bonus.uid  = a.shipper_id and date(from_unixtime(bonus.report_date-3600)) = created_date
left join shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus2
    on bonus.uid = bonus2.uid and date(from_unixtime(bonus.report_date-3600)) = date(from_unixtime(bonus2.report_date-3600)) + interval '30' day
-- left join dev_vnfdbi_opsndrivers.ingest_non_hub_bonus_point tier
--     on (total_point + additional_point) between 

where a.shipper_type = 'non-hub'
)
select 
    raw.*
    ,case 
        when raw.current_driver_tier in ('T1') and tmr_driver_tier in ('T2','T3','T4','T5') then 'up'
        when raw.current_driver_tier in ('T2') and tmr_driver_tier in ('T3','T4','T5') then 'up'
        when raw.current_driver_tier in ('T3') and tmr_driver_tier in ('T4','T5') then 'up'
        when raw.current_driver_tier in ('T4') and tmr_driver_tier in ('T5') then 'up'
        when raw.current_driver_tier  = tmr_driver_tier then 'keep'
        when raw.current_driver_tier in ('T2','T3','T4','T5') and tmr_driver_tier in ('T1') then 'down'
        when raw.current_driver_tier in ('T3','T4','T5') and tmr_driver_tier in ('T2') then 'down'
        when raw.current_driver_tier in ('T4','T5') and tmr_driver_tier in ('T3') then 'down'
        when raw.current_driver_tier in ('T5') and tmr_driver_tier in ('T4') then 'down'
        end as original_type_change

    ,case 
        when raw.current_driver_tier in ('T1') and new_driver_tier in ('T2','T3','T4','T5') then 'up'
        when raw.current_driver_tier in ('T2') and new_driver_tier in ('T3','T4','T5') then 'up'
        when raw.current_driver_tier in ('T3') and new_driver_tier in ('T4','T5') then 'up'
        when raw.current_driver_tier in ('T4') and new_driver_tier in ('T5') then 'up'
        when raw.current_driver_tier  = new_driver_tier then 'keep'
        when raw.current_driver_tier in ('T2','T3','T4','T5') and new_driver_tier in ('T1') then 'down'
        when raw.current_driver_tier in ('T3','T4','T5') and new_driver_tier in ('T2') then 'down'
        when raw.current_driver_tier in ('T4','T5') and new_driver_tier in ('T3') then 'down'
        when raw.current_driver_tier in ('T5') and new_driver_tier in ('T4') then 'down'
        end as new_type_change
    ,case when sla >= 90 then cast(p.bonus as double) else 0 end as new_bonus
    ,case when sla >= 90 then cast(p.bonus as double) else 0 end - coalesce(bonus_value,0) as gap_bonus

from raw
left join vnfdbi_opsndrivers.ingest_non_hub_bonus_point p
    on raw.new_daily_point between cast(p.from_ as bigint) and cast(p.to_ as bigint) and raw.current_driver_tier = p.current_driver_tier

-- where current_driver_tier <> new_driver_tier

