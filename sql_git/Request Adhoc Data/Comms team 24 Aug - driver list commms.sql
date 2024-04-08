with ado as 
(
SELECT 
         date(from_unixtime(dot.real_drop_time - 3600)) as report_date
        ,dot.uid
        ,count(dot.ref_order_code) as total_order
FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
where dot.order_status = 400 
and date(from_unixtime(dot.real_drop_time - 3600)) >= current_date - interval '45' day
group by 1,2
)
,driver_base as 
(select 
         sm.shipper_id
        ,spp.shopee_uid
        ,sm.shipper_name 
        ,sm.city_name
        ,case when sm.shipper_status_code = 1 then 'Working' 
              else 'Off' end as working_status
        ,case when spp.take_order_status = 1 then 'Normal'
              when spp.take_order_status = 3 then 'Pending'
              else 'N/A' end as order_status 
        ,date(from_unixtime(spp.create_time - 3600)) as onboard_date
        ,date_diff('day',date(from_unixtime(spp.create_time - 3600)),current_date ) as seniority
        ,coalesce(sum(case when ado.report_date between current_date - interval '30' day and current_date - interval '1' day then ado.total_order else null end),0) as total_order_l30d
        -- ,max(ado.report_date) as max_date 
        -- ,min(ado.report_date) as min_date

from shopeefood.foody_mart__profile_shipper_master sm 

left join shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live spp on spp.uid = sm.shipper_id

left join ado on ado.uid = sm.shipper_id

where sm.grass_date = 'current'

and sm.shipper_status_code = 1 

and sm.city_name not like '%Test%'

group by 1,2,3,4,5,6,7
)


select * 


from driver_base 

where 1 = 1 

and seniority>= 30

and total_order_l30d >= 100

and working_status = 'Working'

and order_status = 'Normal'