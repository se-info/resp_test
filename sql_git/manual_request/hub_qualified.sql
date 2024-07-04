with raw as 
(SELECT 
        dot.ref_order_id
        ,date(from_unixtime(dot.submitted_time - 3600)) as created_date
        ,dot.uid
        ,extract(hour from from_unixtime(dot.submitted_time - 3600)) as created_hour
        ,dot.delivery_distance/cast(1000 as double) as distance
        ,city.name_en as city_name
        ,case when dot.pick_city_id = 217 then 'HCM'
              when dot.pick_city_id = 218 then 'HN'
              when dot.pick_city_id = 219 then 'DN'
              when dot.pick_city_id = 220 then 'HP' 
              ELSE 'OTH' end as city_group
        ,case when dot.ref_order_category = 0 then 'order-delivery' else 'ship-order' end as service
        ,sm.shipper_type_id
        ,dotet.order_data
        ,COALESCE(cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ),0) as hub_id
        ,COALESCE(cast(json_extract(dotet.order_data,'$.real_hub_id') as BIGINT ),0) as real_hub_id
        ,COALESCE(cast(json_extract(dotet.order_data,'$.pick_hub_id') as BIGINT ),0) as pick_hub_id
        ,COALESCE(cast(json_extract(dotet.order_data,'$.drop_hub_id') as BIGINT ),0) as drop_hub_id
        ,COALESCE(cast(json_extract(dotet.order_data,'$.real_pick_hub_id') as BIGINT ),0) as real_pick_hub_id
        ,COALESCE(cast(json_extract(dotet.order_data,'$.real_drop_hub_id') as BIGINT ),0) as real_drop_hub_id


FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet ON dot.id = dotet.order_id

LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = dot.uid and try_cast(sm.grass_date as date) = date(from_unixtime(dot.submitted_time - 3600))

LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86

WHERE 1 = 1

and dot.order_status = 400
and dot.pick_city_id in (217,218)

)
,metrics as 
(SELECT 
       created_date 
      ,created_hour
      ,service
      ,ref_order_id      
      ,city_name
      ,case when hub_id > 0 then 1 else 0 end as current_qualified
      ,case when hub_id > 0 then 1 
            when pick_hub_id > 0 and distance < 2.7 then 1 
            else 0 end as qualified_2km7
      ,case when hub_id > 0 then 1 
            when pick_hub_id > 0 and distance < 2.5 then 1 
            else 0 end as qualified_2km5
      ,case when hub_id > 0 then 1 
            when pick_hub_id > 0 and distance < 2.3 then 1 
            else 0 end as qualified_2km3

from raw                     
where (created_date = date'2022-08-08'
       or 
       created_date = date'2022-09-09')

)
select 
            created_date
           ,created_hour
           ,service 
           ,city_name
           ,current_qualified
           ,qualified_2km7
           ,qualified_2km5
           ,qualified_2km3
           ,count(distinct ref_order_id) 

from metrics 


group by 1,2,3,4,5,6,7,8
