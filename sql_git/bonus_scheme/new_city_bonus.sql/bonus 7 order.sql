select *,row_number()over(order by delivered_timestamp asc) as ranking_driver from
(select 
        ro.order_code,
        ro.shipper_id,
        sm.shipper_name,
        sm.city_name,
        ro.delivered_timestamp,
        row_number()over(partition by ro.shipper_id order by ro.delivered_timestamp asc) as order_rank,
        date(from_unixtime(spp.create_time - 3600)) as onboard_date



from driver_ops_raw_order_tab ro 
left join shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live spp 
    on spp.uid = ro.shipper_id
left join shopeefood.foody_mart__profile_shipper_master sm 
    on sm.shipper_id = ro.shipper_id 
    and sm.grass_date = 'current'
where 1 = 1 
and date(ro.delivered_timestamp) between date'2023-10-17' and date'2023-10-31'
and ro.order_status in ('Delivered','Returned')
and date(from_unixtime(spp.create_time - 3600)) between date'2023-09-26' and date'2023-10-15'
and sm.city_name = 'Long An'
)
where 1 = 1 
and order_rank = 7
limit 20 