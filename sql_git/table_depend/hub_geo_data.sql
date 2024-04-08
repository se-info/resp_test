with raw as 
(
    select 
json_extract(extra_data,'$.geo_data.points') as geo_data
,hub_name
,hub_priority
,city_id



from vnfdbi_opsndrivers.phong_test_table
-- from shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live


where 1 = 1 
)

select     hub_name
          ,cast(hub_lat_long as array<json>)[1] as hub_lat
          ,cast(hub_lat_long as array<json>)[2] as hub_long
          ,hub_priority
          ,city_id
        --   ,array_join(array_agg(cast(hub_lat_long as array<json>)[1]),',') as list_lat  
        --   ,array_join(array_agg(cast(hub_lat_long as array<json>)[2]),',') as list_long  

from 
( 
( 
    select cast(geo_data as array<json>) as a
           ,hub_name,hub_priority,city_id
    from raw
)

cross join 
           unnest (a) as t(hub_lat_long)

)           
;
SELECT 
         city_id
        ,hub_name
        ,hub_priority
        ,CAST(hub_lat_long AS ARRAY<JSON>)[1] AS hub_lat
        ,CAST(hub_lat_long AS ARRAY<JSON>)[2] AS hub_long

        -- ,hub_lat_long[2] AS hub_long
FROM (
  SELECT city_id, hub_name,hub_priority,json_extract(extra_data,'$.geo_data.points') as hub
  
  FROM vnfdbi_opsndrivers.phong_test_table
) t1
CROSS JOIN UNNEST(CAST((hub) AS ARRAY<JSON>)) AS t2(hub_lat_long)