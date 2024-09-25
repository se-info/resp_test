select * from
(select 
        *,
        row_number()over(partition by city_name order by delivered_timestamp asc) as ranking_driver,
        count(shipper_id)over (partition by city_name order by delivered_timestamp asc) as ranking_city,
        'spf_do_0013|Hoan thanh 7 don nhanh nhat_'||date_format(last_active,'%Y-%m-%d') as txn_note
from
(select 
        ro.order_code,
        ro.shipper_id,
        sm.shipper_name,
        sm.city_name,
        ro.delivered_timestamp,
        la.last_active,
        row_number()over(partition by ro.shipper_id order by ro.delivered_timestamp asc) as order_rank,
        date(from_unixtime(spp.create_time - 3600)) as onboard_date



from driver_ops_raw_order_tab ro 

left join (select shipper_id,max(date(delivered_timestamp)) as last_active from driver_ops_raw_order_tab 
where date(delivered_timestamp) between date'2024-08-01' and date'2024-08-31'
and order_status = 'Delivered' 
group by 1
) la on la.shipper_id = ro.shipper_id

left join shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live spp 
    on spp.uid = ro.shipper_id
left join shopeefood.foody_mart__profile_shipper_master sm 
    on sm.shipper_id = ro.shipper_id 
    and sm.grass_date = 'current'
where 1 = 1 
and date(ro.delivered_timestamp) between date'2024-08-01' and date'2024-08-31'
and ro.order_status in ('Delivered','Returned')
and date(from_unixtime(spp.create_time - 3600)) between date'2024-07-01' and date'2024-08-31'
and sm.city_name in (
'Vinh Long',
'Ben Tre',
'Tra Vinh',
'Hau Giang',
'Soc Trang',
'Bac Lieu',
'Ca Mau'
)
)
where 1 = 1 
and order_rank = 7
)
where 1 = 1 
and ranking_driver <= 20
and ranking_city <= 20 