with raw as 
(select 
        raw.order_code,
        raw.city_name,
        raw.shipper_id,
        13500 as current_shipping_fee,
        case 
        when raw.distance <= 2 then 12000
        when raw.distance > 2 then 13500
        end as new_shipping_fee,
        raw.driver_policy,
        (hi.shift_end_time - hi.shift_start_time)/3600 as shift_hour,
        cast(json_extract(hi.extra_data,'$.is_apply_fixed_amount') as varchar) as apply_extra_fee,
        hub.slot_id,
        date(delivered_timestamp) as report_date,
        -- extra_data
        raw.group_id,
        raw.distance,
        raw.created_timestamp


from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab raw 

left join shopeefood.foody_partner_archive_db__shipper_hub_order_tab__reg_continuous_s0_live hub 
    on hub.ref_order_id = raw.id
    and hub.ref_order_category = raw.order_type

left join shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hi 
    on hi.id = hub.autopay_report_id

where driver_policy = 2 
and date(delivered_timestamp) between date'2023-10-01' and date'2023-10-30'
and raw.order_type = 0 
and raw.order_status = 'Delivered'
and raw.shipper_id > 0 
and raw.city_id in (217,218)
/*
Hub refresh lại việc 12k cho 0-2km, 2-3.xx: 13.5k và + extra fee if higher, cho chị lại impact của hub theo Total Hub/Long Shift/Mid Shift/Short Shift
*/
)
,metrics as 
(select
        raw.group_id,
        sum(raw.new_shipping_fee) as single_fee,
        sum(case when rank_ = 1 then raw.new_shipping_fee else 0 end) as fee_1,
        sum(case when rank_ = 2 then raw.new_shipping_fee else 0 end) as fee_2,
        case 
        when max(rank_) = 2 then
        greatest(
        
            greatest(sum(case when rank_ = 1 then raw.new_shipping_fee else 0 end) + (sum(case when rank_ = 2 then raw.new_shipping_fee else 0 end)/1.34) * 1,
            sum(case when rank_ = 2 then raw.new_shipping_fee else 0 end) + (sum(case when rank_ = 1 then raw.new_shipping_fee else 0 end)/1.34) * 1)                   
        ,
        sum(raw.distance)/cast(1.34 as double) * 3750 * 1
        ) 
        when max(rank_) > 2 then sum(raw.distance)/cast(1.34 as double) * 3750 * 1
        else sum(raw.new_shipping_fee) end as fee_2025,
        case 
        when max(rank_) = 2 then
        greatest(
            greatest(sum(case when rank_ = 1 then raw.new_shipping_fee else 0 end) + (sum(case when rank_ = 2 then raw.new_shipping_fee else 0 end)/1.37) * 0.9,
            sum(case when rank_ = 2 then raw.new_shipping_fee else 0 end) + (sum(case when rank_ = 1 then raw.new_shipping_fee else 0 end)/1.37) * 0.9)                   
        ,sum(raw.distance)/cast(1.37 as double) * 3750 * 1
        ) 
        when max(rank_) > 2 then sum(raw.distance)/cast(1.37 as double) * 3750 * 1
        else sum(raw.new_shipping_fee) end as fee_2026,

        case 
        when max(rank_) = 2 then
        greatest(
            greatest(sum(case when rank_ = 1 then raw.new_shipping_fee else 0 end) + (sum(case when rank_ = 2 then raw.new_shipping_fee else 0 end)/1.4) * 0.8,
            sum(case when rank_ = 2 then raw.new_shipping_fee else 0 end) + (sum(case when rank_ = 1 then raw.new_shipping_fee else 0 end)/1.4) * 0.8)                   
        ,
        sum(raw.distance)/cast(1.4 as double) * 3750 * 1
        ) 
        when max(rank_) > 2 then sum(raw.distance)/cast(1.4 as double) * 3750 * 1
        else sum(raw.new_shipping_fee) end as fee_2027,
        0 as extra_fee    


from
(select  
        raw.group_id,
        raw.order_code,
        row_number()over(partition by raw.group_id order by raw.created_timestamp asc) as rank_,
        raw.new_shipping_fee,
        raw.distance
from raw

where raw.group_id > 0 
) raw 
group by 1 
)
select 
        sum(single_fee) as single_fee,
        sum(fee_2025+extra_fee) as fee_2025,
        sum(fee_2026+extra_fee) as fee_2026,
        sum(fee_2027+extra_fee) as fee_2027
        
from metrics



