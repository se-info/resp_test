with base as
(select
date(from_unixtime(create_time-3600)) create_time
,from_unixtime(create_time-3600) c_ts
--,id
-- ,previous_balance/100+ previous_deposit/100 as total_balance
,user_id
,sum(balance*1.000/100) over (partition by user_id order by id asc rows between unbounded preceding and current row) as balance
,row_number () over(partition by user_id order by from_unixtime(create_time-3600) desc) as rank
,sum(deposit/100) over (partition by user_id order by id asc rows between unbounded preceding and current row) as deposit
--,sum(balance/100) over (partition by user_id order by id asc rows between unbounded preceding and current row) + sum(deposit/100) over (partition by user_id order by id asc rows between unbounded preceding and current row) as total_balance

from shopeefood.foody_accountant_db__partner_transaction_tab__reg_daily_s0_live
-- where user_id = 18512834
--where date(from_unixtime(create_time-3600)) <= ${end_date}
-- date'2022-04-25'
--and user_id = 2996387
)
SELECT sm.shipper_id
,sm.shipper_name
,sm.city_name
,date(from_unixtime(ps.create_time - 3600)) as onboard_date
,date(from_unixtime(sm.last_order_timestamp - 3600)) as last_active_date
,coalesce(date_diff('day',date(from_unixtime(sm.last_order_timestamp - 3600)),date(current_date)-interval '1' day),0)as non_active
,case
WHEN st.shift_category = 1 and wt.start_time/3600 = 10 then '5 hour shift S'
WHEN st.shift_category = 1 then '5 hour shift'
WHEN st.shift_category = 2 then '8 hour shift'
WHEN st.shift_category = 3 then '10 hour shift'
ELSE 'Part-time' END AS hub_type
,coalesce(current.current_driver_tier,'') as tier_
,case when ps.take_order_status = 1 then 'Normal'
when ps.take_order_status = 3 then 'Pending'
else 'N/A' end as order_status
, case when working_status = 1 then 'Normal'
else 'Off' end as working_status
,case when ps.take_order_status = 3 then date(from_unixtime(pd.update_time - 3600))
else null end as pending_date
,case when ps.take_order_status = 3 then rea.name_eng else null end as pending_reason
,bl.balance*1.000/100 as main_wallet
,lc.deposit*1.000/100 as deposit_wallet
,lc.balance as balance_at_end_date


from
shopeefood.foody_mart__profile_shipper_master sm


left join shopeefood.foody_internal_db__shipper_info_personal_tab__reg_daily_s2_live ps on ps.uid = sm.shipper_id
--pending date
left join shopeefood.foody_internal_db__shipper_log_pending_reason_tab__reg_daily_s0_live pd on pd.uid = sm.shipper_id
left join shopeefood.foody_internal_db__shipper_log_pending_reason_tab__reg_daily_s0_live filter on filter.uid = pd.uid and pd.update_time < filter.update_time
left join dev_vnfdbi_opsndrivers.driver_pending_reason_tab rea on cast(rea.reason_id as bigint) = pd.reason_id


--balance current

left join shopeefood.foody_accountant_db__partner_balance_tab__reg_daily_s0_live bl on bl.user_id = sm.shipper_id

left join shopeefood.foody_internal_db__shipper_registration_tab__reg_daily_s0_live rt on sm.shipper_id = cast(json_extract(rt.extra_data,'$.shipper_uid') as bigint)

left join base lc on lc.user_id = sm.shipper_id
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

where cast(from_unixtime(bonus.report_date - 60*60) as date) = date(current_date) - interval '1' day
)current on current.shipper_id = sm.shipper_id

---SHIFT
LEFT JOIN ( shopeefood.foody_internal_db__shipper_info_work_tab__reg_daily_s0_live ) st on st.uid = sm.shipper_id
LEFT JOIN shopeefood.foody_internal_db__shipper_config_working_time_tab__vn_daily_s0_live wt on wt.id = st.working_time_id
-- ---HUB
-- LEFT JOIN shopeefood.foody_internal_db__shipper_hub_mapping_tab__reg_daily_s0_live hm on hm.uid = sm.shipper_id
-- LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live hi on hi.id = hm.hub_id

where 1 = 1
and lc.rank = 1
and lc.create_time <= ${end_date}
and filter.uid is null
--and sm.city_id in (217)
--and sm.shipper_type_id = 12
--and sm.shipper_id = 4826244
and sm.grass_date = 'current'
and sm.shipper_status_code = 1