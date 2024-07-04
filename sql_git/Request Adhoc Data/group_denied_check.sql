with raw as 
(select 
        a.*,
        case 
        when ogi.group_status is not null then 1 else 0 end as is_group,
        ogi.group_id,
        ogi.group_distance,
        ogi.group_code,
        r.distance as single_distance

from driver_ops_deny_log_tab a 

left join driver_ops_raw_order_tab r on a.delivery_id = r.delivery_id and a.order_type = r.order_type

LEFT JOIN 
(select ogi.*,ogm.uid,ogm.group_status,ogi.create_time as order_create_time, ogm.create_time as group_create_time,ogm.distance*1.00/100000 as group_distance,ogm.group_code

from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da 
where date(dt) = current_date - interval '1' day) ogi 

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da
where date(dt) = current_date - interval '1' day
) ogm on ogm.id = ogi.group_id

) ogi 
    on ogi.ref_order_code = a.order_code 
    and ogi.ref_order_category = a.order_type
    and ogi.uid = a.shipper_id
    and ogi.mapping_status = 1

LEFT JOIN driver_ops_order_assign_log_tab assign    
    on assign.order_id = ogi.group_id 
    and ogi.ref_order_category = assign.order_category
    and ogi.uid = assign.driver_id
    and ogi.mapping_status = 22

where 1 = 1 
and reason_id in (62,117)
)
select 
        created,
        created_ts,
        reason_name_vn,
        reason_id,
        shipper_id,
        order_code,
        group_code,
        group_distance,
        if(order_type=0,'1. delivery','2. spxi') as source,
        single_distance


from raw 

where created between current_date - interval '7' day and current_date - interval '1' day 

order by group_code desc

