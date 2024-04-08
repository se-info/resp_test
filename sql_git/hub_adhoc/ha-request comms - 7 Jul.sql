with driver_l60 as 
(select dot.uid 
    --    ,date(from_unixtime(dot.real_drop_time - 3600)) as date_ 
       ,count(distinct ref_order_code) as total_order 

from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 


where 1 = 1 
and order_status = 400
and date(from_unixtime(dot.real_drop_time - 3600)) between current_date - interval '60' day and current_date - interval '1' day

group by 1
) 
,driver_l14 as 
(select dot.uid 
    --    ,date(from_unixtime(dot.real_drop_time - 3600)) as date_ 
       ,count(distinct ref_order_code) as total_order 

from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 


where 1 = 1 
and order_status = 400
and date(from_unixtime(dot.real_drop_time - 3600)) between current_date - interval '14' day and current_date - interval '1' day

group by 1
) 

select sm.shipper_id 
      ,sm.shipper_name
      ,sm.city_name
      ,case when sm.shipper_type_id = 12 then 'Hub' else 'Non Hub' end as shipper_type
      ,(a.total_order) as total_order_l60 
      ,(b.total_order) as total_order_l14


from  shopeefood.foody_mart__profile_shipper_master sm 

left join driver_l60 a on sm.shipper_id = a.uid 

left join driver_l14 b on b.uid = sm.shipper_id 


where 1 = 1 

and sm.grass_date = 'current'

and sm.city_id in (217,218)

and sm.shipper_type_id != 12

-- group by 1,2,3