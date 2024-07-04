with list_shipper_test as
(select 
    uid as shipper_id
from shopeefood.foody_internal_db__shipper_info_contact_tab__reg_daily_s2_live
where work_email like '%@foody%'

union all 

select 
    uid as shipper_id
from shopeefood.foody_internal_db__shipper_info_personal_tab__reg_continuous_s0_live sip
-- where uid = 10274319
where regexp_like(lower(CONCAT(sip.last_name, ' ', sip.first_name)),'test|now|admin')
or regexp_like(lower(sip.full_name),'test')

union all

select 
    shipper_id
from shopeefood.foody_mart__profile_shipper_master
where shipper_type_id = 3
or shipper_id in (21647116,8162429,104967)
-- 
/*
list driver test GF update
21647116: xuchao.wang@shopee.com (name: xuchao Wang)
8162429: testerQc@gmail.com (name: Đơn Rủi Ro HN)
104967: phuongphung@foody.vn  (Name: Phương Phùng HCM Tester)
Em thấy tên tài khoản lạ nên check lại thì email test vs shopee và foody)
*/
)
,pool_shipper as
(select 
    case when sm.grass_date = 'current' then date(current_date)
                        else cast(sm.grass_date as date) end as report_date
    ,sm.shipper_id
    ,case when sm.shipper_type_id = 12 then 'hub' else 'non-hub' end as shipper_type
    ,case 
        when shipper_status_code = 1 and  sm.shipper_type_id = 12 then 'hub'
        when shipper_status_code = 1 and  sm.shipper_type_id != 12 and bonus.tier in (1,6,11) then 'T1' -- as current_driver_tier
        when shipper_status_code = 1 and  sm.shipper_type_id != 12 and bonus.tier in (2,7,12) then 'T2'
        when shipper_status_code = 1 and  sm.shipper_type_id != 12 and bonus.tier in (3,8,13) then 'T3'
        when shipper_status_code = 1 and  sm.shipper_type_id != 12 and bonus.tier in (4,9,14) then 'T4'
        when shipper_status_code = 1 and  sm.shipper_type_id != 12 and bonus.tier in (5,10,15) then 'T5'
        when shipper_status_code != 1 then 'inactive'
        else 'part_time' end as current_driver_tier
        -- when shipper_status_code = 1 and  sm.shipper_type_id != 12 and bonus.tier not in (5,10,15) then 'T5'

    ,shipper_status_code
    ,shipper_type_id
    ,sm.city_name
    ,case when t.shipper_id is not null then 1 else 0 end as is_test
    ,case when shipper_status_code = 1 then 'active' else 'inactive' end as shipper_status
    ,bonus.tier
    ,case when o.shipper_id is not null then 'transacting driver' else 'off' end as working_type
    

from shopeefood.foody_mart__profile_shipper_master sm
left join shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus
    on sm.shipper_id = bonus.uid and try_cast(sm.grass_date as date) = date(from_unixtime(bonus.report_date - 3600))
left join list_shipper_test t 
    on sm.shipper_id = t.shipper_id
left join dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_order_performance_dev o
    on sm.shipper_id = o.shipper_id and try_cast(sm.grass_date as date) = o.report_date and o.order_status = 'Delivered'
-- where try_cast(sm.grass_date as date) = date '2022-09-27'

-- select 
--     *
-- from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_order_performance_dev 
-- limit 10
)
,final as
(select 
    d0.*
    ,d1.current_driver_tier as previous_tier
    ,d1.report_date as previous_date
from pool_shipper d0
left join pool_shipper d1
    on d0.shipper_id = d1.shipper_id and d1.report_date = d0.report_date - interval '${num_day_compare}' day
where d0.is_test = 0
)
select 
    report_date
    ,city_name
    ,case when ${check_previous_tier} = 1 
        then (case when current_driver_tier = previous_tier then 'keep'
                    when coalesce(previous_tier,'na') = 'na' and coalesce(current_driver_tier,'na') != 'na' then 'new'
                    else 
                    concat(previous_tier,'-',current_driver_tier) end) else 'non-check' end as detail
    ,case 
    when current_driver_tier = previous_tier then 'keep'
    when coalesce(previous_tier,'na') = 'na' and coalesce(current_driver_tier,'na') != 'na' then 'new'
    when coalesce(previous_tier,'na') = 'T1' and coalesce(current_driver_tier,'na') in ('T2','T3','T4','T5') then 'up tier'
    when coalesce(previous_tier,'na') = 'T2' and coalesce(current_driver_tier,'na') in ('T3','T4','T5') then 'up tier'
    when coalesce(previous_tier,'na') = 'T3' and coalesce(current_driver_tier,'na') in ('T4','T5') then 'up tier'
    when coalesce(previous_tier,'na') = 'T4' and coalesce(current_driver_tier,'na') in ('T5') then 'up tier'
    when coalesce(previous_tier,'na') = 'T5' and coalesce(current_driver_tier,'na') in ('T1','T2','T3','T4') then 'down tier'
    when coalesce(previous_tier,'na') = 'T4' and coalesce(current_driver_tier,'na') in ('T1','T2','T3') then 'down tier'
    when coalesce(previous_tier,'na') = 'T3' and coalesce(current_driver_tier,'na') in ('T1','T2') then 'down tier'
    when coalesce(previous_tier,'na') = 'T2' and coalesce(current_driver_tier,'na') in ('T1') then 'down tier'
    when coalesce(previous_tier,'na') in ('T1','T2','T3','T4','T5','part_time') and coalesce(current_driver_tier,'na') = 'hub' then 'convert to hub'
    when coalesce(previous_tier,'na') = 'hub' and coalesce(current_driver_tier,'na') in ('T1','T2','T3','T4','T5','part_time') then 'out hub'
    else concat(coalesce(previous_tier,'na'),'-',coalesce(current_driver_tier,'na')) end as type_
    ,working_type
    ,current_driver_tier
    ,count(distinct shipper_id) total_driver
from final
where report_date between date '2022-09-01' and date '2022-09-30'
group by 1,2,3,4,5,6
