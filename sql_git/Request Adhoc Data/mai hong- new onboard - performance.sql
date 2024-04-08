with base as 
(select a.uid as shipper_id 
       ,case when sm.shipper_type_id = 12 then 'Hub' else 'Non Hub' end as shipper_type
       ,sm.city_name
       ,1 as mapping 
       ,date(from_unixtime(a.create_time - 3600)) as onboard_date
       ,year(date(from_unixtime(a.create_time - 3600)))*100 + week(date(from_unixtime(a.create_time - 3600))) as onboard_week




from shopeefood.foody_internal_db__shipper_info_personal_tab__reg_daily_s2_live a 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = a.uid and try_cast(sm.grass_date as date) = date(from_unixtime(a.create_time - 3600))


)
, report_date AS
(SELECT
    DATE(report_date) AS report_date
    ,1 as mapping 
FROM
    ((SELECT sequence(date'2022-07-01'  , current_date - interval '1' day) bar)
CROSS JOIN
    unnest (bar) as t(report_date)
)
)
,final as 
(select   a.report_date
        ,b.shipper_id
        ,b.city_name
        ,b.onboard_date
        ,b.onboard_week
      


from report_date a 

left join base b on b.mapping = a.mapping
-- from vnfdbi_opsndrivers.snp_foody_shipper_daily_report b 
-- left join base a on b.shipper_id = a.uid 

where 1 = 1 
and b.city_name not in ('HCM City','Ha Noi City')

)

select a.* 
      ,b.cnt_total_order_delivered
      ,b.total_working_time
      ,b.total_online_time 
      ,b.total_online_time - b.total_working_time as down_time 

from final a 

left join vnfdbi_opsndrivers.snp_foody_shipper_daily_report b on b.shipper_id = a.shipper_id and b.report_date = a.report_date