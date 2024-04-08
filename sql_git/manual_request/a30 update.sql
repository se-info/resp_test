with raw as 
(
select 
            date_ 
           ,city_name
           ,working_group
           ,tier_type
           ,count(distinct case when total_order_l30d > 0 then shipper_id else null end) as a30 
           ,count(distinct case when total_order_l1d > 0 then shipper_id else null end) as a1

from
(select  
       try_cast(sm.grass_date as date) as date_ 
      ,sm.shipper_id
      ,sm.shipper_name 
      ,sm.city_name
      ,coalesce(current.current_driver_tier,'Others') as tier_type
      ,case when sm.shipper_type_id = 12 then 'Hub' else 'Non Hub' end as working_group
      --,min(case when ado.report_date between current_date - interval '29' day and current_date - interval '1' day then ado.report_date else null end) as min_date
      ,coalesce(sum(case when ado.report_date between try_cast(sm.grass_date as date) - interval '29' day and try_cast(sm.grass_date as date) then ado.total_order else null end),0) as total_order_l30d
      ,coalesce(sum(case when ado.report_date = try_cast(sm.grass_date as date) then ado.total_order else null end),0) as total_order_l1d



from
shopeefood.foody_mart__profile_shipper_master sm

---ado
left join
(SELECT date(from_unixtime(dot.real_drop_time - 3600)) as report_date
,year(date(from_unixtime(dot.real_drop_time - 3600)))*100 + week(date(from_unixtime(dot.real_drop_time - 3600))) as create_year_week
,dot.uid
, count(dot.ref_order_code) as total_order
FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id
where dot.order_status = 400

--and dot.uid = 8644811
and date(from_unixtime(dot.real_drop_time - 3600)) >= date'2022-01-01'
--and date(from_unixtime(dot.real_drop_time - 3600)) < date(current_date)
--and date(from_unixtime(dot.real_drop_time - 3600)) between date('2021-10-27') and date('2021-11-10')

group by 1,2,3)ado on ado.uid = sm.shipper_id

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

where cast(from_unixtime(bonus.report_date - 60*60) as date) = date(current_date) - interval '1' day
)current on sm.shipper_id = current.shipper_id


where sm.grass_date != 'current'
and sm.shipper_status_code = 1 
--and sm.shipper_id = 2996387
and sm.city_name not like '%Test%'


group by 1,2,3,4,5,6
)
group by 1,2,3,4
)

select  
        month(date_) as month_ 
       ,city_name
       ,tier_type
       ,working_group
       ,sum(a1)/cast(count(distinct date_) as double) as a1_  
       ,sum(a30)/cast(count(distinct date_) as double) as a30_  


from raw 

where date_ >= date'2022-01-01'
group by 1,2,3,4
-- where tier_type in ('T1','T2','T3')
-- and city_name = 'Ha Noi City'
