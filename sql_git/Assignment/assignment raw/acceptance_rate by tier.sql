with base_assignment as
(
select
date(create_timestamp) created_date
        ,ref_order_category
        ,order_uid
        ,shipper_uid
        ,create_timestamp
        ,status
        ,case 
when sm.shipper_type_id = 12 then 'Hub'
when bonus.tier in (1,6,11) then 'T1' -- as current_driver_tier
when bonus.tier in (2,7,12) then 'T2'
when bonus.tier in (3,8,13) then 'T3'
when bonus.tier in (4,9,14) then 'T4'
when bonus.tier in (5,10,15) then 'T5'
else 'part_time' end as current_driver_tier
from dev_vnfdbi_opsndrivers.shopeefood_bnp_assignment_order_tab raw
left join shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus
on raw.shipper_uid = bonus.uid and date(from_unixtime(raw.create_time-3600)) = cast(from_unixtime(bonus.report_date - 3600) as date)
left join shopeefood.foody_mart__profile_shipper_master sm
on raw.shipper_uid = sm.shipper_id and date(from_unixtime(raw.create_time-3600)) = try_cast(sm.grass_date as date)

where status in (3,4,2,14,15,8,9,17,18) 
and date(create_timestamp) = date '2022-12-05'
)



select 
    created_date
    ,case 
when ref_order_category = 0 then 'Food'
when ref_order_category != 0 then 'Ship'
end as service
    ,current_driver_tier
    ,count(distinct case when status in (3,4,2,14,15,8,9,17,18) then (shipper_uid,order_uid,create_timestamp) else null end) as no_assign
    ,count(distinct case when status in (3,4) then (shipper_uid,order_uid,create_timestamp) else null end) as no_incharged
    ,count(distinct case when status in (8,9,17,18) then (shipper_uid,order_uid,create_timestamp) else null end) as no_ignored
    ,count(distinct case when status in (2,14,15) then (shipper_uid,order_uid,create_timestamp) else null end) as no_deny
from base_assignment
group by 1,2,3