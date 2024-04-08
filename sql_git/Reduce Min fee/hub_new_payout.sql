with raw as 
(select 
        raw.order_code,
        raw.city_name,
        raw.shipper_id,
        13500 as current_shipping_fee,
        13500 as opt1,
        13500 as opt2,
        case 
        when distance <= 2 then 12000
        when distance <= 3.5 then 13500
        when distance >= 3.5 then ((ceiling(distance) - 3.5)/0.5) *1000 + 13500 end as opt3,
        case 
        when distance <= 2 then 12000
        when distance <= 3.5 then 13500
        when distance >= 3.5 then ((ceiling(distance) - 3.5)/0.5) *1000 + 13500 end as opt4,
        raw.driver_policy,
        (hi.shift_end_time - hi.shift_start_time)/3600 as shift_hour,
        cast(json_extract(hi.extra_data,'$.is_apply_fixed_amount') as varchar) as apply_extra_fee,
        hub.slot_id,
        date(delivered_timestamp) as report_date,
        -- extra_data
        raw.group_id,
        raw.distance,
        ceiling(distance),
        raw.created_timestamp


from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab raw 

left join shopeefood.foody_partner_archive_db__shipper_hub_order_tab__reg_continuous_s0_live hub 
    on hub.ref_order_id = raw.id
    and hub.ref_order_category = raw.order_type

left join shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hi 
    on hi.id = hub.autopay_report_id

where driver_policy = 2 
and date(delivered_timestamp) between date'2023-10-01' and date'2023-10-31'
and raw.order_type = 0 
-- and raw.source = 'order_food'
and raw.order_status = 'Delivered'
and raw.shipper_id > 0 
and raw.city_id in (217,218,220)
and hub.slot_id is not null
)
,group_info as 
(select 
        raw.group_id,
        raw.single_fee,
        raw.single_fee_opt1,
        raw.single_fee_opt2,
        raw.single_fee_opt3,
        raw.single_fee_opt4,
        raw.total_order_in_group,
        cast(json_extract(ogi.extra_data,'$.re') as double) as re_group,
        ogi.distance/cast(100000 as double ) as group_distance,
        
        fee_1_opt1 + (fee_2_opt1/cast(json_extract(ogi.extra_data,'$.re') as double)) * 0.8 as fee_1_opt1,
        fee_2_opt1 + (fee_1_opt1/cast(json_extract(ogi.extra_data,'$.re') as double)) * 0.8 as fee_2_opt1,

        fee_1_opt2 + (fee_2_opt2/cast(json_extract(ogi.extra_data,'$.re') as double)) * 0.7 as fee_1_opt2,
        fee_2_opt2 + (fee_1_opt2/cast(json_extract(ogi.extra_data,'$.re') as double)) * 0.7 as fee_2_opt2,

        fee_1_opt3 + (fee_2_opt3/cast(json_extract(ogi.extra_data,'$.re') as double)) * 0.9 as fee_1_opt3,
        fee_2_opt3 + (fee_1_opt3/cast(json_extract(ogi.extra_data,'$.re') as double)) * 0.9 as fee_2_opt3,
        
        fee_1_opt4 + (fee_2_opt4/cast(json_extract(ogi.extra_data,'$.re') as double)) * 0.8 as fee_1_opt4,
        fee_2_opt4 + (fee_1_opt4/cast(json_extract(ogi.extra_data,'$.re') as double)) * 0.8 as fee_2_opt4

from         
(select
        raw.group_id,
        sum(raw.current_shipping_fee) as single_fee,
        sum(raw.opt1) as single_fee_opt1,
        sum(raw.opt2) as single_fee_opt2,
        sum(raw.opt3) as single_fee_opt3,
        sum(raw.opt4) as single_fee_opt4,
        sum(case when rank_ = 1 then raw.opt1 else 0 end) as fee_1_opt1,
        sum(case when rank_ = 2 then raw.opt1 else 0 end) as fee_2_opt1,

        sum(case when rank_ = 1 then raw.opt2 else 0 end) as fee_1_opt2,
        sum(case when rank_ = 2 then raw.opt2 else 0 end) as fee_2_opt2,

        sum(case when rank_ = 1 then raw.opt3 else 0 end) as fee_1_opt3,
        sum(case when rank_ = 2 then raw.opt3 else 0 end) as fee_2_opt3,

        sum(case when rank_ = 1 then raw.opt4 else 0 end) as fee_1_opt4,
        sum(case when rank_ = 2 then raw.opt4 else 0 end) as fee_2_opt4,

        max(rank_) as total_order_in_group

from
(select  
        raw.group_id,
        raw.order_code,
        raw.current_shipping_fee,
        row_number()over(partition by raw.group_id order by raw.created_timestamp asc) as rank_,
        raw.opt1,
        raw.opt2,
        raw.opt3,
        raw.opt4,
        raw.distance

from raw

where raw.group_id > 0 
) raw 
group by 1 
) raw 
left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi 
    on ogi.id = raw.group_id
)
,metrics as 
(select 
        raw.order_code,
        raw.city_name,
        raw.shift_hour,
        raw.slot_id,
        raw.apply_extra_fee,
        raw.group_id,
        raw.current_shipping_fee,
        raw.shipper_id,
        raw.report_date,

        case 
        when gi.total_order_in_group = 2 then 
        greatest(
            greatest(gi.fee_1_opt1,gi.fee_2_opt1)                   
        ,least(gi.group_distance*3750*1,gi.single_fee_opt1)
        )*1.0000/gi.total_order_in_group
        when gi.total_order_in_group > 2 then greatest((gi.group_distance*3750*1),gi.single_fee_opt1)*1.0000/gi.total_order_in_group   
        else raw.opt1 end as fee_opt1,

        case 
        when gi.total_order_in_group = 2 then 
        greatest(
            greatest(gi.fee_1_opt2,gi.fee_2_opt2)                   
        ,least(gi.group_distance*3750*1,gi.single_fee_opt2)
        )*1.0000/gi.total_order_in_group
        when gi.total_order_in_group > 2 then greatest((gi.group_distance*3750*1),gi.single_fee_opt2)*1.0000/gi.total_order_in_group   
        else raw.opt2 end as fee_opt2,

        case 
        when gi.total_order_in_group = 2 then 
        greatest(
            greatest(gi.fee_1_opt3,gi.fee_2_opt3)                   
        ,least(gi.group_distance*3750*1,gi.single_fee_opt3)
        )*1.0000/gi.total_order_in_group
        when gi.total_order_in_group > 2 then greatest((gi.group_distance*3750*1),gi.single_fee_opt3)*1.0000/gi.total_order_in_group   
        else raw.opt3 end as fee_opt3,

        case 
        when gi.total_order_in_group = 2 then 
        greatest(
            greatest(gi.fee_1_opt4,gi.fee_2_opt4)                   
        ,least(gi.group_distance*3750*1,gi.single_fee_opt4)
        )*1.0000/gi.total_order_in_group
        when gi.total_order_in_group > 2 then greatest((gi.group_distance*3750*1),gi.single_fee_opt4)*1.0000/gi.total_order_in_group   
        else raw.opt4 end as fee_opt4
from raw 

left join group_info gi 
    on gi.group_id = raw.group_id 
)
,auto_pay as 
(select 
        report_date,
        shipper_id,
        city_name,
        apply_extra_fee,
        slot_id,
        shift_hour,
        count(distinct order_code) as total_order,
        count(distinct case when group_id > 0 then order_code else null end) as stack_group_order,
        sum(current_shipping_fee) as ship_shared,
        sum(fee_opt1) as ship_shared_opt1,
        sum(fee_opt2) as ship_shared_opt2,
        sum(fee_opt3) as ship_shared_opt3,
        sum(fee_opt4) as ship_shared_opt4

from metrics 

where 1 = 1 
group by 1,2,3,4,5,6
)
,s as 
(select 
        ap.*,
        hp.extra_ship,
        hp.daily_bonus,
        case 
        when ap.apply_extra_fee = 'true' and hp.extra_ship > 0 and shift_hour = 10 and ap.total_order < 30 then ((30-ap.total_order)*13500) 
        when ap.apply_extra_fee = 'true' and hp.extra_ship > 0 and shift_hour = 8 and ap.total_order < 25 then ((25-ap.total_order)*13500) 
        else 0 end as extra_opt1,
        case 
        when ap.apply_extra_fee = 'true' and hp.extra_ship > 0 and shift_hour = 10 and ap.total_order < 30 then ((30-ap.total_order)*13500)
        when ap.apply_extra_fee = 'true' and hp.extra_ship > 0 and shift_hour = 8 and ap.total_order < 25 then ((25-ap.total_order)*13500) 
        else 0 end as extra_opt2,
        case 
        when ap.apply_extra_fee = 'true' and hp.extra_ship > 0 and shift_hour = 10 and ap.total_order < 30 then ((30-ap.total_order)*12000)
        when ap.apply_extra_fee = 'true' and hp.extra_ship > 0 and shift_hour = 8 and ap.total_order < 25 then ((25-ap.total_order)*12000) 
        else 0 end as extra_opt3,
        case 
        when ap.apply_extra_fee = 'true' and hp.extra_ship > 0 and shift_hour = 10 and ap.total_order < 30 then ((30-ap.total_order)*12500)
        when ap.apply_extra_fee = 'true' and hp.extra_ship > 0 and shift_hour = 8 and ap.total_order < 25 then ((25-ap.total_order)*12500)
        else 0 end as extra_opt4

from auto_pay ap 

left join driver_ops_hub_driver_performance_tab hp 
    on hp.uid = ap.shipper_id 
    and hp.slot_id = ap.slot_id
    and hp.total_order > 0
)
select 
        date_trunc('month',report_date) as month_,
        coalesce(city_name,'VN') as cities,
        count(distinct shipper_id) as unique_a1,
        sum(total_order) as hub_order,
        sum(stack_group_order) as hub_stack_group,
        sum(ship_shared + extra_ship ) as base_current,
        sum(ship_shared_opt1 + extra_opt1 ) as base_opt1,
        sum(ship_shared_opt2 + extra_opt2 ) as base_opt2,
        sum(ship_shared_opt3 + extra_opt3 ) as base_opt3,
        sum(ship_shared_opt4 + extra_opt4 ) as base_opt4,
        sum(daily_bonus) as daily_bonus,
        count(distinct report_date) as days
        
from s 

group by 1,grouping sets (city_name,())

