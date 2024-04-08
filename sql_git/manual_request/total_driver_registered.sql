select year(from_unixtime(a.create_time - 3600)) as year_ 
      ,month(from_unixtime(a.create_time - 3600)) as month_ 
      ,city.name_en as city_name 
      ,count(distinct a.id) as total_driver_registered  


from shopeefood.foody_internal_db__shipper_registration_tab__reg_daily_s0_live a 


left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = a.city_id and city.country_id = 86


-- where date(from_unixtime(create_time - 3600)) between date'2020-01-01' and current_date
where 1 = 1 

and year(from_unixtime(a.create_time - 3600)) between 2020 and 2022

group by 1,2,3