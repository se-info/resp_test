with ogm as 
(select 
        ogm.group_id,
        ogm.ref_order_category,
        array_agg(distinct dot.order_status) as order_status_agg,
        array_agg(distinct coalesce(hub.slot_id,0)) as slot_id,
        sum(dot.single_fee) as single_fee,
        array_agg(case when ogm.mapping_status = 11 then ogm.ref_order_id else null end) as order_group_info


from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da ogm 

left join 
(select ref_order_id,ref_order_category,delivery_cost*1.0000/100 as single_fee,order_status
from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da 
where date(dt) = current_date - interval '2' day
) dot on dot.ref_order_id = ogm.ref_order_id
      and dot.ref_order_category = ogm.ref_order_category
left join shopeefood.foody_partner_archive_db__shipper_hub_order_tab__reg_continuous_s0_live hub
    on hub.ref_order_id = ogm.ref_order_id
    and hub.ref_order_category = ogm.ref_order_category

where date(dt) = current_date - interval '1' day

group by 1,2
) 
,group_info as 
(select 
        coalesce(cast(json_extract(extra_data,'$.group_and_assign') as bigint),0) as is_new_group,
        coalesce(cast(json_extract(extra_data,'$.re') as DOUBLE),0) as re,
        case 
        when ogi.ref_order_category = 0 then 'food' 
        else 'spxi' end as source,
        from_unixtime(create_time - 3600) as created_ts,
        date(from_unixtime(create_time - 3600)) as created_date,
        group_status,
        cast(json_extract(extra_data,'$.distance_matrix.mapping') as array<bigint>) as order_in_group,
        cast(json_extract(extra_data,'$.pick_city_id') as bigint) as pick_city_id,
        ROUND(cast(json_extract(extra_data,'$.ship_fee_info.min_fee') as double),0) as minfee_system,
        group_code,
        ship_fee*1.0000/100 as group_fee,
        ogm.single_fee,
        ogm.slot_id,
        ogi.id as group_id,
        distance*1.00/100000 as group_distance,
        cast(json_extract(extra_data,'$.ship_fee_info.per_km') as bigint) as unit_fee,
        cast(json_extract(extra_data,'$.ship_fee_info.surge_rate') as double) as surge_rate,
        ogm.order_status_agg,
        ogm.order_group_info


from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da ogi 

left join ogm on ogm.group_id = ogi.id and ogm.ref_order_category = ogi.ref_order_category
-- cross join unnest (cast(json_extract(ogi.extra_data,'$.distance_matrix.mapping') as array<json>)) as t(delivery_id)

where date(dt) = current_date - interval '1' day
and ogi.ref_order_category = 6
and date(from_unixtime(create_time - 3600)) between date'2024-01-15' and date'2024-01-21'
order by 1 desc 
)
,f as 
(select 
        cardinality(filter(order_group_info,x->x>0)) as total_order_in_group_as_final,
        cardinality(filter(order_in_group,x->x>0)) as total_order_in_group,
        cardinality(filter(slot_id,x->x>0)) as slot_id_cnt,
        cardinality(filter(order_status_agg,x->x in (400,405))) as total_completed_order,
        *,
        unit_fee*surge_rate*group_distance as total_shipping_fee,
        round(group_fee - least(greatest(minfee_system,unit_fee*surge_rate*group_distance),single_fee),0) as extra_fee,
        group_fee - round(group_fee - least(greatest(minfee_system,unit_fee*surge_rate*group_distance),single_fee),0) as fee_excluded_extra

from group_info
where source != 'food'
)
,metrics as 
(select 
        case 
        when fee_excluded_extra = minfee_system then 1
        else 2 end as fee_segment,
        *
from f )
select 
        fee_segment,
        re_range,
        is_new_group,
        total_order_in_group as total_order_in_group_at_created,
        count(distinct group_code) as cnt_group,
        sum(no_assign) as no_assign,
        sum(no_incharged) as no_incharged,
        sum(no_ignored) as no_ignored,
        sum(no_deny) as no_deny,
        count(distinct created_date) as days
from
(select  
        m.created_date,
        m.group_id,
        m.group_code,
        m.fee_segment,
        m.total_order_in_group,
        m.is_new_group,
        case 
        when m.re <= 1.2 then '1. 1-1.2'
        when m.re <= 1.4 then '2. 1.2-1.4'
        when m.re <= 1.6 then '3. 1.4-1.6'
        when m.re <= 2 then '4. 1.6-2'
        when m.re > 2 then '5. ++2' end as re_range,
        coalesce(sa.no_assign,0) as no_assign,
        coalesce(sa.no_incharged,0) as no_incharged,
        coalesce(sa.no_ignored,0) as no_ignored,
        coalesce(sa.no_deny,0) as no_deny



from metrics m 

left join 
(select 
        order_id,
        count(distinct case when status in (3,4,2,14,15,8,9,17,18) then (driver_id,order_id,create_time) else null end) as no_assign,
        count(distinct case when status in (3,4) then (driver_id,order_id,create_time) else null end) as no_incharged,
        count(distinct case when status in (8,9,17,18) then (driver_id,order_id,create_time) else null end) as no_ignored,
        count(distinct case when status in (2,14,15) then (driver_id,order_id,create_time) else null end) as no_deny

from driver_ops_order_assign_log_tab
where status in (3,4,2,14,15,8,9,17,18) 
and order_category = 6
and order_type = 'Group'
group by 1 
) sa on sa.order_id = m.group_id
)
group by 1,2,3,4 

