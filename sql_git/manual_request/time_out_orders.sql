select  created_date
       ,city_name
       ,count(distinct case when rank = 1 then order_id else null end) as total_timeout_order 



from 
(select date(from_unixtime(a.create_time - 3600)) as created_date 
,a.order_id
,city.name_en as city_name
,row_number()over(partition by a.order_id order by a.create_time desc) as rank 


from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live a 

left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on dot.ref_order_id = a.order_id and dot.ref_order_category = 0

left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86

where status = 22 

)

where created_date between date'2022-06-27' and date'2022-07-24'

and city_name in ('Nghe An','Quang Ninh','Thai Nguyen','Khanh Hoa','Lam Dong','Quang Nam')

group by 1,2