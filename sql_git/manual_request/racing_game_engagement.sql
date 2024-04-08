with snp as 
(
    select 
            date_ 
           ,uid  
           ,count(distinct ref_order_code) as total_order    
           ,count(distinct case when hour_minute between 1000 and 1200 then ref_order_code else null end) as total_order_10_12
           ,count(distinct case when hour_minute between 1700 and 1900 then ref_order_code else null end) as total_order_17_19
from    
    (
    select 
         date(from_unixtime(dot.real_drop_time - 3600)) as date_ 
        ,extract(hour from from_unixtime(dot.real_drop_time - 3600))*100 + minute(from_unixtime(dot.real_drop_time - 3600))  as hour_minute
        ,ref_order_code
        ,uid 



from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 

where 1 = 1 

and date(from_unixtime(dot.real_drop_time - 3600)) = date'2022-07-07'

and dot.order_status = 400
    )

-- where uid = 2996387
group by 1,2

)

select 
         snp.date_  
        ,snp.uid
        ,sm.shipper_name
        ,sm.city_name
        ,case when sm.shipper_type_id = 12 then 'Hub' else 'Non Hub' end as working_type 
        ,snp.total_order
        ,snp.total_order_10_12
        ,snp.total_order_17_19


from snp 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = snp.uid and try_cast(sm.grass_date as date) = snp.date_ 
