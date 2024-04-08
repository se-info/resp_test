-- assignment condition 
WITH assignment AS
(SELECT * 
       from dev_vnfdbi_opsndrivers.phong_raw_assignment_test
       )
-- select * from assignment where 1 = 1 and shipper_id = 11135646 and date_ = date'2022-07-17'

,total_assign_turn as 
(select 
        date_ 
       ,shipper_id
       ,working_type
       ,count(order_id) as total_assign 


from 
(select a.order_id 
        ,a.order_type
        ,a.shipper_id 
        ,from_unixtime(a.create_time - 3600) as timestamp
        ,date(from_unixtime(a.create_time - 3600)) as date_  
        ,a.status 
        ,case when sm.shipper_type_id = 12 
                and slot.uid is not null 
                and (cast(hour(from_unixtime(a.create_time - 3600)) as double) + cast(minute(from_unixtime(a.create_time - 3600)) as double)/60) between slot.start_time and slot.end_time then concat('Hub','-',cast(slot.shift_hour as varchar),' hour shift')
                else coalesce(concat('Non Hub','-',current.current_driver_tier),'Others') end as working_type



            FROM
                (SELECT
                    CONCAT(CAST(order_id AS VARCHAR), '-', CAST(order_type AS VARCHAR)) AS order_uid
                    , order_id, city_id, assign_type, update_time, create_time, status, order_type
                    , experiment_group, shipper_uid AS shipper_id

                FROM shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                -- WHERE status IN (8,9) -- shipper incharge + ignore
                WHERE  1 = 1
                AND DATE(FROM_UNIXTIME(create_time - 3600))  BETWEEN current_date - interval '45' day and current_date - interval '1' day
                UNION ALL

                SELECT
                    CONCAT(CAST(order_id as VARCHAR), '-', CAST(order_type AS VARCHAR)) AS order_uid
                    , order_id, city_id, assign_type, update_time, create_time, status, order_type
                    , experiment_group, shipper_uid AS shipper_id

                FROM shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                -- WHERE status IN (8,9) -- shipper incharge + ignore
                WHERE  1 = 1 
                AND DATE(FROM_UNIXTIME(create_time - 3600))  BETWEEN current_date - interval '45' day and current_date - interval '1' day
                ) a 
    left  join ( select  uid 
                        ,date(from_unixtime(date_ts - 3600)) as date_ts 
                        ,(cast(start_time as double)/3600) as start_time 
                        ,(cast(end_time as double)/3600) as end_time
                        ,(end_time - start_time)/3600 as shift_hour 
    from 
    shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live

    ) slot 

    on slot.uid = a.shipper_id and date(from_unixtime(a.create_time)) = date_ts 
left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = a.shipper_id and try_cast(sm.grass_date as date) = date(from_unixtime(a.create_time))
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

-- where cast(from_unixtime(bonus.report_date - 60*60) as date) = date(current_date) - interval '1' day
)current on current.shipper_id = a.shipper_id and current.report_date = date(from_unixtime(a.create_time)) 
)            
GROUP BY 1,2,3
            )

-- select * from total_assign_turn            
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
        ,a.reason



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

-- where cast(from_unixtime(bonus.report_date - 60*60) as date) = date(current_date) - interval '1' day
)current on current.shipper_id = a.shipper_id and current.report_date = a.date_ 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = a.shipper_id and try_cast(sm.grass_date as date) = a.date_

left join shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot on dot.ref_order_id = a.order_id and a.order_code = dot.ref_order_category

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



    select   
         final.date_ 
        ,final.shipper_id
        ,final.driver_city_name 
        ,final.working_type
        -- ,ps.gender
        -- ,substr(cast(ps.birth_date as varchar),1,4) as year_of_birth

        -- ,final_status_working_type
        -- ,driver_city_name
        --total ignore/denied
        ,cast(json_extract(hub.extra_data,'$.total_income')as bigint) as total_income
        ,case when cast(json_extract(hub.extra_data,'$.is_apply_fixed_amount') as VARCHAR) = 'true' then (cast(json_extract(hub.extra_data,'$.total_income') as bigint) - cast(json_extract(hub.extra_data,'$.calculated_shipping_shared') as bigint))
        else 0 end as extra_ship
        ,coalesce(cast(json_extract(hub.extra_data,'$.total_bonus') as bigint),0) as daily_bonus
        ,cast(json_extract(hub.extra_data,'$.total_order')as bigint) as total_order
        ,ass.total_assign
        ,count(distinct case when issue_category = 'Ignore' then order_code else null end) as total_ignore_order
        ,count(distinct case when issue_category != 'Ignore' then order_code else null end) as total_denied_order
        ,count(distinct case when issue_category != 'Ignore' and reason = 'Did not accept order belongs type "Auto accept"' then order_code else null end) as denied_auto_accept
        


from final 

LEFT JOIN total_assign_turn ass on ass.shipper_id = final.shipper_id and final.date_ = ass.date_ and final.working_type = ass.working_type

left join shopeefood.foody_internal_db__shipper_info_personal_tab__reg_daily_s2_live ps on ps.uid = final.shipper_id

left join shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hub on hub.uid = final.shipper_id and final.date_ = date(from_unixtime(hub.report_date - 3600))

-- where order_type = '1. Food/Market'
where 1 = 1 
and final.working_type not like '%Non Hub%'
and final.working_type != 'Others'
-- and issue_category = 'Ignore'


-- and date_ between ${start_date} and ${end_date}
and final.date_ = current_date  - interval '1' day
-- and order_id = 318911908 
-- and shipper_id = 2996387


group by 1,2,3,4,5,6,7,8,9
