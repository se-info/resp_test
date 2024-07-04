with raw as 
(select  
       try_cast(sm.grass_date as date) as cut_off_date
      ,sm.shipper_id
      ,sp.shopee_uid 
      ,sp.gender
      ,sm.shipper_name 
      ,sm.city_name
      ,sp.birth_date
      ,coalesce(current.current_driver_tier,'Others') as tier_type
      ,case when sm.shipper_type_id = 12 then 'Hub' else 'Non Hub' end as working_group
      ,case WHEN sp.shift_category = 1 then '5 hour shift'
            WHEN sp.shift_category = 2 then '8 hour shift'
            WHEN sp.shift_category = 3 then '10 hour shift'
            ELSE 'Non Hub' END AS hub_type
      ,date(from_unixtime(sp.create_time - 3600)) as onboard_date
      ,date_diff('day',date(from_unixtime(sp.create_time - 3600)),current_date) as seniority
      ,case when sp.take_order_status = 1 then 'Normal' 
            when sp.take_order_status = 2 then 'Stop'
            else 'Pending' end as order_status
      ,case when sp.take_order_status = 3 then date(from_unixtime(pd.update_time - 3600)) 
            else null end as pending_date
      ,case when sp.take_order_status = 3 then coalesce(re.name_eng,null)
            else null end as pending_reason                        
      ,hub_name as hub_location            
      ,MAX(ado.report_date) as last_active_date
    --   ,coalesce(sum(case when ado.report_date between current_date - interval '1' day and current_date - interval '1' day then ado.total_order else null end),0) as total_order_l1d
    --   ,coalesce(sum(case when ado.report_date between current_date - interval '7' day and current_date - interval '1' day then ado.total_order else null end),0) as total_order_l7d
    --   ,coalesce(sum(case when ado.report_date between current_date - interval '15' day and current_date - interval '1' day then ado.total_order else null end),0) as total_order_l15d
      ,coalesce(count(distinct case when ado.report_date between try_cast(sm.grass_date as date) - interval '29' day and try_cast(sm.grass_date as date) then ado.report_date else null end),0) as total_working_day_l30d
      ,coalesce(sum(case when ado.report_date between try_cast(sm.grass_date as date) - interval '29' day and try_cast(sm.grass_date as date) then ado.total_order else null end),0) as total_order_l30d
      ,coalesce(sum(case when ado.report_date between try_cast(sm.grass_date as date) - interval '29' day and try_cast(sm.grass_date as date) then ado.total_online_time else null end),0) as total_online_time_l30d
      
    --   ,coalesce(sum(case when ado.report_date between current_date - interval '60' day and current_date - interval '1' day then ado.total_order else null end),0) as total_order_l60d
    --   ,coalesce(sum(case when ado.report_date between current_date - interval '90' day and current_date - interval '1' day then ado.total_order else null end),0) as total_order_l90d

from shopeefood.foody_mart__profile_shipper_master sm

---ado
left join
        (
    SELECT 
         date(from_unixtime(dot.real_drop_time - 3600)) as report_date
        ,year(date(from_unixtime(dot.real_drop_time - 3600)))*100 + week(date(from_unixtime(dot.real_drop_time - 3600))) as create_year_week
        ,dot.uid
        ,dl.total_online_seconds/cast(3600 as double) as total_online_time
        ,sum(dot.delivery_distance/cast(1000 as double)) as total_distance
        , count(dot.ref_order_code) as total_order
FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id
        left join shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live dl on dl.uid = dot.uid and date(from_unixtime(dl.report_date - 3600)) = date(from_unixtime(dot.real_drop_time - 3600))
        where dot.order_status = 400
        and date(from_unixtime(dot.real_drop_time - 3600)) between date'${cut_off_date}' - interval '29' day and date'${cut_off_date}'
        group by 1,2,3,4
        )ado on ado.uid = sm.shipper_id

LEFT JOIN shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live sp on sp.uid = sm.shipper_id 

LEFT JOIN 
(SELECT 
        uid 
        ,array_agg(hub_name) as hub_name

from     
(select a.*,b.hub_name

from shopeefood.foody_internal_db__shipper_hub_mapping_tab__reg_daily_s0_live a 

left join shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live b on b.id = a.hub_id)

group by 1 
) hub_name on hub_name.uid = sm.shipper_id

--pending date 
left join shopeefood.foody_internal_db__shipper_log_pending_reason_tab__reg_daily_s0_live pd on pd.uid = sm.shipper_id
left join shopeefood.foody_internal_db__shipper_log_pending_reason_tab__reg_daily_s0_live filter on filter.uid = pd.uid and pd.update_time < filter.update_time
left join dev_vnfdbi_opsndrivers.driver_pending_reason_tab re on cast(re.reason_id as bigint) = pd.reason_id


---tier 
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

where cast(from_unixtime(bonus.report_date - 60*60) as date) =  date'${cut_off_date}'
)current on sm.shipper_id = current.shipper_id


where try_cast(sm.grass_date as date) = date'${cut_off_date}'
and sm.shipper_status_code = 1 
--and sm.shipper_id = 2996387
and sm.city_name not like '%Test%'
and filter.uid is null

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
)

select  * 

from raw 


