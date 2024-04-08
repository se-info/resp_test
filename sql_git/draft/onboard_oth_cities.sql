with base as 
(select a.uid 
       ,case when sm.shipper_type_id = 12 then 'Hub' else 'Non Hub' end as shipper_type
       ,sm.city_name
       ,date(from_unixtime(a.create_time - 3600)) as onboard_date




from shopeefood.foody_internal_db__shipper_info_personal_tab__reg_daily_s2_live a 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = a.uid and try_cast(sm.grass_date as date) = date(from_unixtime(a.create_time - 3600))


)

,open as
(select 
    uid
    ,date(min(from_unixtime(create_time-3600))) account_open_date
    -- ,*
from shopeefood.foody_internal_db__shipper_log_change_tab__reg_daily_s0_live

where 1 = 1 
and change_type = 'IsActive'

group by 1
)

select 
       a.* 
       ,b.account_open_date 



from base a 


left join open b on b.uid = a.uid 


where a.city_name not in ('HCM City','Ha Noi City')


order by a.onboard_date desc
