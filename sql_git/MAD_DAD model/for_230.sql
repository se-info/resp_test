select 
        t,
        count(shipper_id)/cast(count(distinct report_date) as double) as dad,
        count(distinct shipper_id) as mad
from
(select 
        case when di.name_en = 'Thu Dau Mot Town' then 'Center'
        else 'Outer' end as t,
        date(raw.delivered_timestamp) as report_date,
        raw.shipper_id,
        count(distinct raw.order_code) as total_order
        

from driver_ops_raw_order_tab raw  
left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live di 
    on di.id = raw.district_id
left join shopeefood.foody_mart__profile_shipper_master sm 
    on sm.shipper_id = raw.shipper_id and sm.grass_date = 'current'
where raw.city_id = 230 
and date(delivered_timestamp) between date'2023-10-01' and date'2023-10-31'
and sm.city_id = 230
and order_status in ('Delivered','Returned')
group by 1,2,3
)
group by 1