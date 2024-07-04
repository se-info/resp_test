with raw as 
(select  
         date(from_unixtime(oct.submit_time - 3600)) as created_date
        ,oct.id as order_id
        ,case when oct.city_id = 217 then 'HCM'
              when oct.city_id = 218 then 'HN'
              when oct.city_id = 219 then 'DN'
              else 'OTH' end as city_group   
        ,case when total_amount/cast(100 as double) < 0 then '1. Under 0'
              when total_amount/cast(100 as double) <= 100000 then '2. 0 - 100k'   
              when total_amount/cast(100 as double) <= 200000 then '3. 100 - 200k'   
              when total_amount/cast(100 as double) <= 300000 then '4. 200 - 300k'   
              when total_amount/cast(100 as double) <= 400000 then '5. 300 - 400k'   
              when total_amount/cast(100 as double) <= 600000 then '6. 400 - 600k'   
              when total_amount/cast(100 as double) <= 800000 then '7. 600 - 800k'   
              when total_amount/cast(100 as double) <= 1000000 then '8. 800 - 1000k'   
              when total_amount/cast(100 as double) <= 2000000  then '9. 1000 - 2000k'
              when total_amount/cast(100 as double) <= 5000000  then '10. 2000 - 5000k'
              when total_amount/cast(100 as double) > 5000000  then '11. > 5000k'
              else 'No transaction' end as amount_range
        ,HOUR(from_unixtime(oct.submit_time - 3600)) as created_hour
        ,case when oct.status = 7 then 'Delivered'
              when oct.status = 8 then 'Cancelled'
              when oct.status = 9 then 'Quit'
              end as order_status
        ,case when merchant_paid_status = 1 then 'Un-paid'
              when merchant_paid_status = 2 then 'Paid'
              when merchant_paid_status = 3 then 'Fail'
              when merchant_paid_status = 4 then 'Refunded' 
              end as merchant_payment                                                        

from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct 

left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = oct.city_id and city.country_id = 86

)

select
        created_date
       ,created_hour
       ,city_group
       ,amount_range
       ,merchant_payment
       ,order_status
       ,count(distinct order_id) as total_orders 

from raw 


where created_date between current_date - interval '7' day and current_date - interval '1' day 

group by 1,2,3,4,5,6
