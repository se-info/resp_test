select 
        city.name_en as city_name 
       ,di.name_en as district_name 
       ,weather as weather_type
       ,is_rain_negative
       ,is_rain 
       ,start_time
       ,end_time
       ,running_time/cast(60 as double) as total_run_time 

from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_rain_tracking_tab rain_mode



left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = rain_mode.city_id and city.country_id = 86

left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live di on di.id = rain_mode.district_id --and di.province_id = 86


where 1 = 1 
and date(start_time) between date'2022-09-25' and date'2022-09-29'
