select 
        m.shipper_id,
        sm.shipper_name,
        sm.city_name,
        count(distinct m.order_code) as total_order,
        sum(m.bonus_value) as total_bonus
from
(select 
        id,
        order_code,
        shipper_id,
        source,
        2000 as bonus_value

from driver_ops_raw_order_tab
where 1 = 1 
and date(delivered_timestamp) = date'2023-10-26'
and shipper_id in 
(42078421,
42067326,
42061210,
42041912,
42027418,
42022121,
42021791,
42015439,
42013745,
41982904,
41982193,
41977044,
41976510,
41974261,
41932986,
41924295,
41923650,
41914525,
41879378,
41871067,
41869045,
41862153,
41855912,
41846375,
41845490)
and order_status in ('Delivered','Returned')
) m 
left join shopeefood.foody_mart__profile_shipper_master sm 
    on m.shipper_id = sm.shipper_id
    and sm.grass_date = 'current'
group by 1,2,3