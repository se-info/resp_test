WITH order_base_v1 as 
(SELECT
      orders.uid as shipper_id
    , date(from_unixtime(real_drop_time - 3600)) as report_date
    , COUNT(DISTINCT ref_order_id) AS delivered_orders

FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) orders

-- WHERE report_date <= date'2022-08-24'
where order_status = 400
GROUP BY 1,2
)

,driver_base_v1 as 
(
SELECT  
-- case when sm.grass_date = 'current' then current_date else cast(grass_date as date) end as report_date
        sm.shipper_id
       ,sm.shipper_name
       ,sm.city_name
       ,IF(sm.shipper_type_id = 12, 'hub' , 'non hub') as working_type
    --    ,ob.delivered_orders
       ,max(ob.report_date) as last_active_date

FROM shopeefood.foody_mart__profile_shipper_master sm 

LEFT JOIN order_base_v1 ob on ob.shipper_id = sm.shipper_id 

WHERE 1 = 1 

AND sm.shipper_type_id <> 12 

AND sm.city_id in (217,218)

AND sm.grass_date = 'current'

AND ob.report_date <= date'2022-08-21'
GROUP BY 1,2,3,4
)

select   
         a.shipper_id
        ,a.shipper_name
        ,a.city_name
        ,a.working_type
        ,a.last_active_date
        ,coalesce(sum(case when ob.report_date = date'2022-08-23' then delivered_orders else null end),0) as delivered_orders_23
        ,coalesce(sum(case when ob.report_date = date'2022-08-24' then delivered_orders else null end),0) as delivered_orders_24


from driver_base_v1 a 

left join order_base_v1 ob on ob.shipper_id = a.shipper_id 

where a.last_active_date <= date'2022-08-18'

and a.city_name not like '%Test%'

group by 1,2,3,4,5






