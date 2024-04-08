with raw as 
(select 
json_extract(hub.extra_data,'$.geo_data.points') as geo_data
,hub.hub_name
,city.name_en as city_name 


from shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live hub 
left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = hub.city_id and city.country_id = 86

where 1 = 1 
and hub.id <> 6
)
,geo_data as 
(select 
        -- array_agg(hub_lat_long) as agg_geo
        hub_lat_long
        ,hub_name
        ,city_name 
from 
( 
( 
select cast(geo_data as array<json>) as a
           ,hub_name
           ,city_name  
from raw
)

cross join unnest (a) as t(hub_lat_long)

)
)
,final_metrics as
(select 
        hub_name
       ,city_name  
       ,array_join(array_agg(test_1),',') as geo_data  

from 
(select 
        array_join(cast(hub_lat_long as array<json>),',') as test_1
        ,hub_name 
        ,city_name 

from geo_data)

group by hub_name,city_name )

select 
         a.*
        ,b.geo_data


from dev_vnfdbi_opsndrivers.phong_hub_order_tracking_v1 a 

left join final_metrics b on a.hub_name = b.hub_name
