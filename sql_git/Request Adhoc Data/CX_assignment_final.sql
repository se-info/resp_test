-- assignment condition 
WITH assignment AS
(SELECT * 
       from dev_vnfdbi_opsndrivers.phong_raw_assignment_test
       )
-- select * from assignment where 1 = 1 and shipper_id = 11135646 and date_ = date'2022-07-17'

,final as 
(select 
         a.date_ 
        ,a.shipper_id
        ,a.order_id
        ,sm.shipper_name
        ,sm.city_name as driver_city_name
        ,city.name_en as order_city_name
        ,case when (total_item >= 15 or total_amount >= 700000) then 'High Value/Bulky Order' end as order_filter
        ,case when sm.shipper_type_id = 12 
                and slot.uid is not null 
                and (cast(hour(a.timestamp) as double) + cast(minute(a.timestamp) as double)/60) between slot.start_time and slot.end_time then concat('Hub','-',cast(slot.shift_hour as varchar),' hour shift')
                else coalesce(concat('Non Hub','-',current.current_driver_tier),'Others') end as working_type
        ,dot.ref_order_code as order_code
        ,case when smm.shipper_type_id = 12 then 'Hub' else 'Non Hub' end as final_status_working_type
        ,a.order_type
        ,a.issue_category
        ,a.timestamp
        ,a.rank



from assignment a 

--- tier 
LEFT JOIN
(SELECT cast(from_unixtime(bonus.report_date - 60*60) as date) as report_date
,bonus.uid as shipper_id
,case when hub.shipper_type_id = 12 then 'Hub'
when bonus.tier in (1,6,11) then 'T1' when bonus.tier in (2,7,12) then 'T2'
when bonus.tier in (3,8,13) then 'T3'
when bonus.tier in (4,9,14) then 'T4'
when bonus.tier in (5,10,15) then 'T5'
else null end as current_driver_tier
,bonus.total_point
,bonus.daily_point

FROM shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus
LEFT JOIN
(SELECT shipper_id
,shipper_type_id
,case when grass_date = 'current' then date(current_date)
else cast(grass_date as date) end as report_date

from shopeefood.foody_mart__profile_shipper_master

where 1=1
and (grass_date = 'current' OR cast(grass_date as date) >= date('2019-01-01'))
GROUP BY 1,2,3
)hub on hub.shipper_id = bonus.uid and hub.report_date = cast(from_unixtime(bonus.report_date - 60*60) as date)

)current on current.shipper_id = a.shipper_id and current.report_date = a.date_ 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = a.shipper_id and try_cast(sm.grass_date as date) = a.date_

left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on dot.ref_order_id = a.order_id and a.order_code = dot.ref_order_category

left join shopeefood.foody_mart__profile_shipper_master smm on smm.shipper_id = dot.uid and try_cast(smm.grass_date as date) = date(from_unixtime(dot.real_drop_time - 3600))

left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86

--HUB SHIFT CHECK 
    left  join ( select  uid 
                        ,date(from_unixtime(date_ts - 3600)) as date_ts 
                        ,(cast(start_time as double)/3600) as start_time 
                        ,(cast(end_time as double)/3600) as end_time
                        ,(end_time - start_time)/3600 as shift_hour 
    from 
    shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live

    ) slot 

    on slot.uid = a.shipper_id and a.date_ = date_ts 

-- and (total_item >= 15 or total_amount >= 700000)
-- and dot.order_status

)
-- select * from final where order_type = '1. Food/Market' and issue_category != 'Ignore'
    select   
         date_ 
        ,shipper_id
        ,driver_city_name 
        ,working_type
        ,ps.gender
        ,substr(cast(ps.birth_date as varchar),1,4) as year_of_birth

        -- ,final_status_working_type
        -- ,driver_city_name
        --total ignore/denied

        ,count(distinct case when issue_category = 'Ignore' then order_code else null end) as total_ignore_order
        ,count(distinct case when issue_category != 'Ignore' then order_code else null end) as total_denied_order
        --ignore/denied filter high value/bulky
        ,count(distinct case when issue_category = 'Ignore' and order_filter = 'High Value/Bulky Order' then order_code else null end) as total_ignore_order_filter_high_value_bulky
        ,count(distinct case when issue_category != 'Ignore' and order_filter = 'High Value/Bulky Order' then order_code else null end) as total_denied_order_filter_high_value_bulky                
        ,count(distinct case when issue_category = 'Driver_Fault' and order_filter = 'High Value/Bulky Order' then order_code else null end) as total_denied_driver_fault_filter_high_value_bulky
        ,count(distinct case when issue_category = 'Order_Fault' and order_filter = 'High Value/Bulky Order' then order_code else null end) as total_denied_order_fault_filter_high_value_bulky
        ,count(distinct case when issue_category = 'Order_Pending' and order_filter = 'High Value/Bulky Order' then order_code else null end) as total_denied_order_pending_filter_high_value_bulky
        ,count(distinct case when issue_category = 'System_Fault' and order_filter = 'High Value/Bulky Order' then order_code else null end) as total_denied_system_fault_filter_high_value_bulky


from final 

left join shopeefood.foody_internal_db__shipper_info_personal_tab__reg_daily_s2_live ps on ps.uid = final.shipper_id


where order_type = '1. Food/Market'

-- and issue_category = 'Ignore'

and date_ between date'2022-07-01' and date'2022-07-17'
-- and order_id = 318911908 
-- and shipper_id in() 


group by 1,2,3,4,5,6