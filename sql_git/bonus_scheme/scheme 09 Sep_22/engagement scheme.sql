with ado as 
(
SELECT 
         date(from_unixtime(dot.real_drop_time - 3600)) as report_date
        ,dot.uid
        ,hour(from_unixtime(dot.real_drop_time - 3600)) * 100 + minute(from_unixtime(dot.real_drop_time - 3600)) as hour_min
        ,coalesce(rate.shipper_rate,0) as rating_ 
        ,dot.ref_order_id as order_id
        ,dot.delivery_distance/cast(1000 as double) as working_distance
        -- ,count(dot.ref_order_code) as total_order
FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

---rating
LEFT JOIN
(SELECT order_id
,shipper_uid as shipper_id
,case when cfo.shipper_rate = 0 then null
when cfo.shipper_rate = 1 or cfo.shipper_rate = 101 then 1
when cfo.shipper_rate = 2 or cfo.shipper_rate = 102 then 2
when cfo.shipper_rate = 3 or cfo.shipper_rate = 103 then 3
when cfo.shipper_rate = 104 then 4
when cfo.shipper_rate = 105 then 5
else null end as shipper_rate
,from_unixtime(cfo.create_time - 60*60) as create_ts

FROM shopeefood.foody_user_activity_db__customer_feedback_order_tab__reg_daily_s0_live cfo
)rate ON dot.ref_order_id = rate.order_id and dot.uid = rate.shipper_id

where dot.order_status = 400 
and date(from_unixtime(dot.real_drop_time - 3600)) = date'${input_date}'
)
,driver_base as 
(select 
         sm.shipper_id
        ,spp.shopee_uid
        ,sm.shipper_name 
        ,sm.city_name
        ,spp.main_phone
        ,case when sm.shipper_type_id = 12 then 'Hub' else 'Non Hub' end as working_type
        ,case when sm.shipper_status_code = 1 then 'Working' 
              else 'Off' end as working_status
        ,case when spp.take_order_status = 1 then 'Normal'
              when spp.take_order_status = 3 then 'Pending'
              else 'N/A' end as order_status 
        ,date(from_unixtime(spp.create_time - 3600)) as onboard_date
        ,date_diff('day',date(from_unixtime(spp.create_time - 3600)),date'${input_date}' ) as seniority_cutoff_dday
        ,rp.completed_rate/cast(100 as double) as sla_rate
        ,coalesce(count(distinct case when ado.report_date = date'${input_date}' then ado.order_id else null end),0) as total_order_dday_allday
        ,coalesce(count(distinct case when ado.report_date = date'${input_date}' and ado.hour_min between 1100 and 1300 then ado.order_id else null end),0) as total_order_dday_11_13
        ,coalesce(count(distinct case when ado.report_date = date'${input_date}' and ado.hour_min between 1700 and 1900 then ado.order_id else null end),0) as total_order_dday_17_19
        ,coalesce(sum(case when ado.report_date = date'${input_date}' then ado.working_distance else null end),0) as working_distance 
        ,coalesce(count(distinct case when ado.report_date = date'${input_date}' and ado.rating_>= 5 then ado.order_id else null end),0) as raing_5_star
        -- ,max(ado.report_date) as max_date 
        -- ,min(ado.report_date) as min_date

from shopeefood.foody_mart__profile_shipper_master sm 

left join shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live spp on spp.uid = sm.shipper_id

left join ado on ado.uid = sm.shipper_id

left join shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live rp on rp.uid = sm.shipper_id and date(from_unixtime(rp.report_date - 3600)) = date'${input_date}'

where try_cast(sm.grass_date as date) = date'${input_date}'

and sm.shipper_status_code = 1 

and sm.city_name not like '%Test%'

group by 1,2,3,4,5,6,7,8,9,10,11
)


select * 


from driver_base 


where 1 = 1 

and total_order_dday_allday > 0 
