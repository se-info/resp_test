with raw as
(select 
        raw.group_id,
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
        bf.bonus_non_hub as bonus_fee,
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

where order_type = 6
and created_date between date'2024-01-01' and date'2024-01-31'
and order_status in ('Delivered','Returned')
and city_name in ('HCM City','Ha Noi City')
)
,m as 
(select  
        created_date,
        case 
        when assign_type = 'single' then 0 else group_id end as group_id,
        max_by(assign_type,id) as assign_type,
        -- array_agg(order_code) as order_info,
        cardinality(array_agg(order_code)) as cnt_order_in_group,
        avg(distance) as avg_single_distance,
        avg(group_distance) as avg_group_distance,
        sum(single_fee) as total_single_fee,
        sum(group_fee_allocate) as total_group_fee,
        sum(bonus_fee) as total_bonus_fee,
        case when max_by(assign_type,id) = 'single' then sum(lt_adj)/cardinality(array_agg(order_code)) 
        else max(lt_adj)*1.00/cardinality(array_agg(order_code)) end as lt_adj

from raw 
group by 1,2
)
select
        assign_type,
        case when assign_type in ('single','stack') then 1 else cnt_order_in_group
        end as cnt_order_in_group,
        sum(cnt_order_in_group) as cnt_order,
        count(distinct case when assign_type != 'single' then group_id else null end) as cnt_group,
        avg(avg_single_distance) as avg_single_distance,
        avg(avg_group_distance) as avg_group_distance,
        avg(lt_adj) as avg_ata_adj,
        sum(total_single_fee) as total_single_fee,
        sum(total_group_fee) as total_group_fee,
        sum(total_bonus_fee) as total_bonus_fee,
        count(distinct created_date) as days


from m 
group by 1,2

