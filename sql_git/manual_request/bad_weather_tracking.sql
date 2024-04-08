with base as 
(select dot.uid 
       ,dot.ref_order_id 
       ,case when dot.pick_city_id = 217 then 'HCM City'
             when dot.pick_city_id = 218 then 'Ha Noi City'
             when dot.pick_city_id = 219 then 'Da Nang City'
             else 'Others' end as city_group
       ,city.name_en as city_name
       
       ,go.bad_weather_fee

       ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60))
                    when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
                    else date(from_unixtime(dot.submitted_time- 60*60)) end as created_date 
      
       ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then extract(hour from from_unixtime(dot.real_drop_time - 60*60))
                    when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then extract(hour from from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
                    else extract(hour from from_unixtime(dot.submitted_time- 60*60)) end as created_hour    
       ,oct.total_shipping_fee*1.00/100 as user_shipping_fee                                

from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 

left join shopeefood.foody_mart__fact_gross_order_join_detail go on go.id = dot.ref_order_id 

left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86

left join shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct on oct.id = dot.ref_order_id

left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dotet.order_id = dot.id 

where 1 = 1 

and dot.ref_order_category = 0 

and dot.order_status = 400
)

select  
        a.created_date
       ,year(a.created_date)*100+week(a.created_date) as created_year_week 
       ,a.created_hour 
       ,city_group
       ,count(distinct ref_order_id) as total_delivered_order
       ,count(distinct case when a.bad_weather_fee > 0 then ref_order_id else null end) as total_order_delivered_have_bwf
       ,sum(a.user_shipping_fee)*1.00/count(distinct ref_order_id) as avg_user_shipping_fee_exc_bwf
       ,sum(a.user_shipping_fee + a.bad_weather_fee)*1.00/count(distinct ref_order_id) as avg_user_shipping_fee_inc_bwf
       ,sum(a.bad_weather_fee)*1.00/count(distinct ref_order_id) as avg_bwf
       ,city_group





from base a 


where 1 = 1 

and created_date between current_date - interval '30' day and current_date - interval '1' day


group by 1,2,3,4,10     
