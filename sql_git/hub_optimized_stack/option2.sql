with group_info as 
(select 
        id,
        group_code,
        max(stacked_per_order_opt1) as stacked_per_order_opt1,
        greatest(
        least(max(group_shipping_fee),(max(single_fee_1)+max(single_fee_2)))
        ,greatest(
            max(single_fee_1)+(max(single_fee_2)/max(re_system))*max(rate_mode),
            (max(single_fee_1)/max(re_system))*max(rate_mode)+max(single_fee_2)
        )
        )*1.00/max(cnt_order) as non_hub_stacked
        
from
(select 
        a.id,
        a.group_code,
        a.distance*1.00/100000 as group_distance,
        cast(json_extract(a.extra_data,'$.re') as double) as re_system,
        JSON_ARRAY_LENGTH(json_extract(extra_data,'$.route'))*1.00/2 as cnt_order,
        3750*(a.distance*1.00/100000)*1 as group_shipping_fee,
        500 as extra_fee,
        -- #allocate per order
        -- least(13500*JSON_ARRAY_LENGTH(json_extract(extra_data,'$.route'))*1.00/2
        -- ,(greatest(3750*(a.distance*1.00/100000)*1,((13500+13500)*0.65)) + 
        --     -- # calculated new extra_fee
        --     (case when (a.distance*1.00/100000) < 4 then 5000 
        --           when (a.distance*1.00/100000) < 5 then 4000
        --           when (a.distance*1.00/100000) < 6 then 3000
        --           when (a.distance*1.00/100000) < 7 then 2000
        --           when (a.distance*1.00/100000) >= 7  then 1000 end)
        --     /JSON_ARRAY_LENGTH(json_extract(extra_data,'$.route'))*1.00/2))
        --     /(JSON_ARRAY_LENGTH(json_extract(extra_data,'$.route'))*1.00/2) as stacked_per_order_opt1,
        least(
        27000
        ,greatest(13500, 
            -- # calculated new extra_fee
            (3750*(a.distance*1.00/100000)*1) )+
            (case when (a.distance*1.00/100000) < 4 then 5000 
                  when (a.distance*1.00/100000) < 5 then 4000
                  when (a.distance*1.00/100000) < 6 then 3000
                  when (a.distance*1.00/100000) < 7 then 2000
                  when (a.distance*1.00/100000) >= 7  then 1000 end)
        )
            /(JSON_ARRAY_LENGTH(json_extract(extra_data,'$.route'))*1.00/2) as stacked_per_order_opt1,
        -- extra_data
        b.order_id,
        cast(json_extract(b.info,'$.distance') as double)/1000 as distance, 
        case 
        when row_number()over(partition by a.id order by b.order_id) = 1 then greatest(13500,3750*cast(json_extract(b.info,'$.distance') as double)/1000*1) 
        else 0 end as single_fee_1,
        case 
        when row_number()over(partition by a.id order by b.order_id) = 2 then greatest(13500,3750*cast(json_extract(b.info,'$.distance') as double)/1000*1) 
        else 0 end as single_fee_2,
        
        0.4 as rate_mode
    

from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da a 

-- cross join unnest (map_entries(cast(json_extract(a.extra_data,'$.fee_details.fee_config.sub_orders') as map<int,json>)))  as b(order_id,alue)
cross join unnest ((cast(json_extract(a.extra_data,'$.fee_details.fee_config.sub_orders') as map<int,json>)))  as b(order_id,info)

where date(dt) = current_date - interval '1' day
)
group by 1,2
)
,hub_order as 
(select 
        ho.uid as shipper_id,
        ho.slot_id,
        ho.autopay_report_id,
        ho.ref_order_id,
        ho.ref_order_category,
        r.group_id,
        case when coalesce(oct.risk_bearer_id,0) != 2 then 1 else 0 end as is_hub_order,
        date(from_unixtime(ho.autopay_date_ts-3600)) as autopay_date,
        13500 as original_base_fee,
        case 
        when r.group_id > 0 then gi.non_hub_stacked
        else 13500 end as stack_non_hub_formula_fee,
        case 
        when r.group_id > 0 then gi.stacked_per_order_opt1
        else 13500 end as opt1_fee,
        r.city_name

from shopeefood.foody_partner_archive_db__shipper_hub_order_tab__reg_daily_s0_live ho

left join driver_ops_raw_order_tab r on ho.ref_order_id = r.id and ho.ref_order_category = r.order_type

left join group_info gi on gi.id = r.group_id

left join 
(select id,cast(json_extract_scalar(oct.extra_data, '$.risk_bearer_type') as int) as risk_bearer_id 
from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
where date(from_unixtime(submit_time - 3600)) between date'2024-02-01' and date'2024-03-24'
) oct
    on ho.ref_order_id = oct.id and ho.ref_order_category = 0
where 1 = 1 
and date(from_unixtime(ho.autopay_date_ts-3600)) between date'2024-03-01' and date'2024-03-24'
)
,f as 
(select 
        ho.autopay_date,
        ho.shipper_id,
        ho.slot_id,
        ho.autopay_report_id,
        ho.city_name,
        case 
        when hp.extra_ship > 0 then hp.total_income - sum(ho.original_base_fee)
        else 0 end as orignal_extra_ship,
        case
        when hp.extra_ship > 0 then hp.total_income - sum(ho.stack_non_hub_formula_fee) 
        else 0 end as non_hub_extra_ship,
        case
        when hp.extra_ship > 0 then hp.total_income - sum(ho.opt1_fee) 
        else 0 end as option1_extra_ship,
        count(distinct case when ho.is_hub_order = 1 then ho.ref_order_id else null end) as total_order,
        count(distinct case when ho.group_id > 0 and ho.is_hub_order = 1 then ho.ref_order_id else null end) as cnt_group_order,
        sum(ho.original_base_fee) as original_ship_shared,
        sum(ho.opt1_fee) as option1_ship_shared,
        sum(ho.stack_non_hub_formula_fee) as stack_non_hub_formula_fee

from hub_order ho

left join driver_ops_hub_driver_performance_tab hp
    on hp.uid = ho.shipper_id and hp.slot_id = ho.slot_id

group by 1,2,3,4,5,hp.extra_ship,hp.total_income
)
select  
        autopay_date as report_date,
        city_name,
        count(distinct (shipper_id,slot_id)) as total_slot_actived,
        count(distinct shipper_id) as total_driver_actived,
        sum(total_order) as ado,
        sum(original_ship_shared) as original_base,
        sum(orignal_extra_ship) as original_extra,
        sum(option1_ship_shared) as option1_base,
        sum(option1_extra_ship) as option1_extra,
        sum(stack_non_hub_formula_fee) as stack_non_hub_ship_shared,
        sum(non_hub_extra_ship) as non_hub_extra_ship

from f 

group by 1,2

            

