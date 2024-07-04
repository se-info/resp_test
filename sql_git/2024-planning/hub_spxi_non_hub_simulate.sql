with r_list as 
(select * from vnfdbi_opsndrivers.phong_test_table
where cast(rank as double) <= 0.3
)
,raw as 
(select 
        raw.group_id,
        raw.order_type,
        raw.id,
        raw.order_code,
        case 
        when raw.group_id > 0 and raw.order_assign_type != 'Group' then 'stack'
        when raw.group_id > 0 and raw.order_assign_type = 'Group' then 'group'
        else 'single' end as assign_type,
        raw.distance,
        raw.created_timestamp,
        raw.delivered_timestamp,
        raw.last_incharge_timestamp,
        ogi.min_group_created,
        ogi.max_group_delivered,
        ogi.ship_fee*1.00/100 as group_fee,
        ogi.group_fee_allocate, 
        ogi.distance*1.00/100000 as group_distance,
        dot.delivery_cost as single_fee,
        case 
        when raw.group_id > 0 then date_diff('second',ogi.min_group_created,ogi.max_group_delivered)*1.00/60 
        else date_diff('second',raw.last_incharge_timestamp,raw.delivered_timestamp)*1.00/60 end as lt_adj,
        raw.is_asap,
        bf.bonus_non_hub_v2 as bonus_fee,
        raw.created_date,
        raw.driver_policy,
        raw.shipper_id


from driver_ops_raw_order_tab raw 

left join (select id,delivery_cost*1.00/100 as delivery_cost from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
        on dot.id = raw.delivery_id

left join 
(select 
        raw.group_id,
        ogi.ship_fee,
        ogi.distance,
        (ogi.ship_fee*1.00/100)/count(raw.id) as group_fee_allocate,
        count(raw.id) as total_order_in_group,
        min(raw.last_incharge_timestamp) as min_group_created,
        max(raw.delivered_timestamp) as max_group_delivered

from driver_ops_raw_order_tab raw
left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi 
        on ogi.id = raw.group_id and ogi.ref_order_category = raw.order_type

where group_id > 0 
and order_status in ('Delivered','Returned')
group by 1,2,3,ship_fee
) ogi on ogi.group_id = (case when raw.group_id > 0 then raw.group_id else 0 end)

left join (select order_id,ref_order_category,bonus,bonus_non_hub,bonus_non_hub_v2 from vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level) bf 
        on bf.order_id = raw.id and bf.ref_order_category = raw.order_type

where raw.created_date = date'2024-01-11'
and raw.order_status in ('Delivered','Returned')
and raw.driver_policy != 2 
and city_name in ('HCM City','Ha Noi City')
-- and raw.order_type = 6 
)
,f as 
(select 
        raw.group_id,
        raw.shipper_id,
        raw.order_code,
        r.group_rank,
        case 
        when r.shipper_id is not null and HOUR(raw.last_incharge_timestamp) between 8 and 12 then 2
        else raw.driver_policy end as new_policy,
        case 
        when raw.group_id > 0 then raw.group_fee_allocate
        else raw.single_fee end as shipping_fee,
        raw.bonus_fee

from raw 

left join r_list r 
    on cast(r.shipper_id as bigint) = raw.shipper_id
    and raw.order_type = 6
)
select 
        sum(order_completed) as ado,
        sum(order_impacted) as ado_impacted,
        sum(bonus_fee_current)/cast(count(distinct shipper_id) as double) as bonus_fee_current,
        sum(bonus_fee_new)/cast(count(distinct shipper_id) as double) as bonus_fee_new,
        sum(income_current)/cast(count(distinct shipper_id) as double) as income_current,
        sum(income_new)/cast(count(distinct shipper_id) as double) as income_new,
        sum(case when order_impacted > 0 then income_current else null end)/cast(count(distinct case when order_impacted > 0 then shipper_id else null end) as double) as impacted_income_current,
        sum(case when order_impacted > 0 then income_new else null end)/cast(count(distinct case when order_impacted > 0 then shipper_id else null end) as double) as impacted_income_new,
        max(order_impacted) as max_order_impacted,
        count(distinct shipper_id) as a1
        -- min(case when order_impacted > 0 then order_impacted else null end) as min_order_impacted
from
(select
        shipper_id,
        count(distinct order_code) as order_completed,
        count(distinct case when new_policy = 2 then order_code else null end) as order_impacted,
        sum(shipping_fee) as shipping_fee,
        sum(bonus_fee) as bonus_fee_current,
        sum(case when new_policy != 2 then bonus_fee else 0 end) as bonus_fee_new,
        sum(shipping_fee) + sum(bonus_fee) as income_current,
        sum(shipping_fee) + sum(case when new_policy != 2 then bonus_fee else 0 end) as income_new

from f 
group by 1)