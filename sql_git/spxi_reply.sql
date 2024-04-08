with f as 
(select 
        r.*,
        count(distinct (sa.driver_id,sa.order_id,sa.create_time)) as total_assign,
        count(distinct case when status in (3,4) then (sa.driver_id,sa.order_id,sa.create_time) else null end) as no_incharged,
        count(distinct case when status in (8,9,17,18) then (sa.driver_id,sa.order_id,sa.create_time) else null end) as no_ignored,
        count(distinct case when status in (2,14,15) then (sa.driver_id,sa.order_id,sa.create_time) else null end) as no_deny          
from
(select  
        ogi.id,
        ogi.ref_order_category,
        cast(json_extract(ogi.extra_data,'$.re') as DOUBLE) as re,
        case 
        when cast(json_extract(ogi.extra_data,'$.is_stack') as varchar) = 'true' then 'stack'
        else 'group' end as is_stack,
        t.delivery_id,
        r.id as ref_id,
        from_unixtime(ogi.create_time - 3600) as created,
        from_unixtime(ogi.update_time - 3600) as updated,
        row_number()over(partition by ogi.id order by cast(t.delivery_id as bigint) asc) as rank_order
      

from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi

cross join unnest (cast(json_extract(ogi.extra_data,'$.distance_matrix.mapping') as array<json>)) as t(delivery_id)

left join driver_ops_raw_order_tab r 
        on r.delivery_id = cast(t.delivery_id as bigint)
        and r.order_type = ogi.ref_order_category
where cast(t.delivery_id as bigint) > 0
) r 
left join (select * from driver_ops_order_assign_log_tab) sa 
        on sa.ref_order_id = r.ref_id
        and sa.create_time between r.created and r.updated

where 1 = 1 

group by 1,2,3,4,5,6,7,8,9
)
select * from f 