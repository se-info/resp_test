with agg as 
(select 
        shipper_id,
        array_agg(distinct report_date) as delivered_date_agg


from driver_ops_driver_performance_tab

where total_order > 0 
group by 1 )
,s as 
(select 
        *,
        case 
        when city_name not in('Ha Noi City') and is_a7 = 1 then 1  
        when shipper_tier in ('Level 1','Level 2') and is_a7 = 1 then 1 
        when shipper_tier = 'Hub' and is_a60 = 1 and l3d = 0 then 1 
        else 0 end as is_bonus
from 
(select 
        r.report_date,
        r.shipper_id,
        sp.shopee_uid,
        r.city_name,
        if(cardinality(filter(agg.delivered_date_agg,x -> x between current_date - interval '7' day and current_date - interval '1' day)) > 0,1,0) as is_a7,
        if(cardinality(filter(agg.delivered_date_agg,x -> x between current_date - interval '60' day and current_date - interval '1' day)) > 0,1,0) as is_a60,
        if(cardinality(filter(agg.delivered_date_agg,x -> x between current_date - interval '3' day and current_date - interval '1' day)) > 0,1,0) as l3d,
        r.shipper_tier

from driver_ops_driver_performance_tab r 

left join agg on agg.shipper_id = r.shipper_id

left join shopeefood.foody_internal_db__shipper_profile_tab__reg_continuous_s0_live sp on sp.uid =  r.shipper_id

where report_date = date'2024-09-07'   
and city_name in ('Ha Noi City','Quang Ninh','Hai Duong','Thai Nguyen','Bac Ninh','Hai Phong City')
)
)
-- select city_name,count(*) from s where is_bonus = 1 group by 1 
select * from s where is_bonus = 1
