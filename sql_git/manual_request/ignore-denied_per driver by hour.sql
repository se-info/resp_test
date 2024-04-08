with hour_mapping as 
(
SELECT 
       t.hour_
      ,1 as mapping 

from (SELECT sequence(0,23) as hour_ ) 
cross join unnest (hour_) as t(hour_)
)
,date as 
(
SELECT
    DATE(report_date) AS report_date
    ,1 as mapping
FROM
    (
(
SELECT sequence(current_date - interval '90' day, current_date - interval '1' day) bar)
CROSS JOIN

    unnest (bar) as t(report_date)
)
)

,final as 
(
select 
         date_add('hour',hour_,cast(date.report_date as TIMESTAMP)) as start_time_slot
		,date_add('second',0,date_add('minute',60,date_add('hour',hour_,cast(date.report_date as TIMESTAMP)))) as end_time_slot
        ,date.report_date
        ,base.hour_	
        ,1 as mapping 			

from hour_mapping base 

left join date on date.mapping = base.mapping
)
,metrics as 
(select 
         report_date
        ,hour_ 
        ,start_time_slot
        ,end_time_slot
        ,shipper_id
        ,city_name
        ,shipper_type
        ,order_type
        -- ,final_status
        ,count(order_id) as total_ignore
        -- ,count(distinct shipper_id) as total_driver



from
(select 
         sa.order_id 
        ,sa.order_type
        ,sa.assign_type
        ,from_unixtime(sa.create_time - 3600) as create_timestamp
        ,b.hour_ 
        ,b.report_date
        ,b.start_time_slot
        ,b.end_time_slot
        ,case when sm.shipper_type_id = 12 then 'hub' else 'non hub' end as shipper_type
        ,case when from_unixtime(sa.create_time - 3600) >= b.start_time_slot 
              and from_unixtime(sa.create_time - 3600) <= b.end_time_slot then 1 
              else 0 end as is_valid
        ,sa.shipper_id
        ,city.name_en as city_name
        -- ,case when od.order_status != 'Cancelled' then od.order_status
        --       else concat(od.order_status,'-',od.cancel_by) end as final_status  




from 
(
SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id,order_type, city_id, assign_type, update_time, create_time,status, 1 as mapping,shipper_uid as shipper_id
                                
        from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live                                

        where status in (8,9)
UNION
                                    
SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id,order_type, city_id, assign_type, update_time, create_time,status,1 as mapping,shipper_uid as shipper_id
                                
        from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live

        where status in (8,9)
)sa                                     



left join final b on b.mapping = sa.mapping and date(from_unixtime(sa.create_time - 3600)) = b.report_date

left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = sa.city_id and city.country_id = 86

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = sa.shipper_id and try_cast(sm.grass_date as date) = date(from_unixtime(sa.create_time))

-- left join dev_vnfdbi_opsndrivers.food_raw_phong od on od.id = sa.order_id and od.order_type = sa.order_type
-- select * from dev_vnfdbi_opsndrivers.food_raw_phong od 

)

where is_valid = 1 

and report_date between current_date - interval '14' day and current_date - interval '1' day 

group by 1,2,3,4,5,6,7,8
)

select 
        report_date
       ,hour_
       ,start_time_slot
       ,end_time_slot
       ,case when order_type = 0 then 'order-delivery' else 'order-spxi' end as service_
       ,case when city_name = 'HCM City' then 'HCM'
             when city_name = 'Ha Noi City' then 'HN'
             when city_name = 'Da Nang City' then 'DN'
             else 'Oths' end as city_group 
       ,shipper_type
    --    ,final_status
       ,sum(total_ignore) as total_ignore
       ,count(distinct shipper_id) as total_driver_ignore
       ,count(distinct case when total_ignore >= 2 then shipper_id else null end) as total_driver_ignore_over_2
       ,count(distinct case when total_ignore >= 3 then shipper_id else null end) as total_driver_ignore_over_3
       ,count(distinct case when total_ignore >= 4 then shipper_id else null end) as total_driver_ignore_over_4
       ,count(distinct case when total_ignore >= 5 then shipper_id else null end) as total_driver_ignore_over_5


from metrics 

group by 1,2,3,4,5,6,7


