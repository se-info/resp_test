with group_info as 
(select 
        id,
        group_code,
        group_distance_range,
        max(cnt_order) as total_order_in_group,
            greatest(
        least(max(group_shipping_fee),(max(single_fee_1)+max(single_fee_2)))
        ,greatest(
            max(single_fee_1)+(max(single_fee_2)/max(re_system))*max(rate_mode),
            (max(single_fee_1)/max(re_system))*max(rate_mode)+max(single_fee_2)
        )
        )*1.00/max(cnt_order)  + 500 as non_hub_stacked,
        greatest(
        least(max(group_shipping_fee),(max(single_fee_1)+max(single_fee_2)))
        ,greatest(
            max(single_fee_1)+(max(single_fee_2)/max(re_system))*max(rate_mode),
            (max(single_fee_1)/max(re_system))*max(rate_mode)+max(single_fee_2)
        )
        )*1.00/max(cnt_order) as non_hub_stacked_non_extra,
        greatest
        (least(
            (max(cnt_order)*13500), -- sum_single
            greatest((max(cnt_order)*13500)*0.65,max(group_shipping_fee)) + max(cnt_order)*500  -- min_group_shipping & total_shipping
            )
            ,
            13500
        )*1.00/max(cnt_order) as option1,
        greatest
        (least(
            (max(cnt_order)*13500), -- sum_single
            greatest((max(cnt_order)*13500)*0,max(group_shipping_fee)) + max(cnt_order)*2500  -- min_group_shipping & total_shipping
            )
            ,
            13500
        )*1.00/max(cnt_order) as option2,

        greatest
        (least(
            (max(cnt_order)*13500), -- sum_single
            greatest((max(cnt_order)*13500)*0.65,max(group_shipping_fee))  -- min_group_shipping & total_shipping
            )
            ,
            13500
        )*1.00/max(cnt_order) as option1_non_extra,
        greatest
        (least(
            (max(cnt_order)*13500), -- sum_single
            greatest((max(cnt_order)*13500)*0,max(group_shipping_fee))  -- min_group_shipping & total_shipping
            )
            ,
            13500
        )*1.00/max(cnt_order) as option2_non_extra
from
(select 
        a.id,
        a.group_code,
        a.distance*1.00/100000 as group_distance,
        case 
        when (a.distance*1.00/100000) between 0 and 1 then '1. 0-1km'
        when (a.distance*1.00/100000) between 1 and 2 then '2. 1-2km'
        when (a.distance*1.00/100000) between 2 and 3 then '3. 2-3km'
        when (a.distance*1.00/100000) between 3 and 4 then '4. 3-4km'
        when (a.distance*1.00/100000) between 4 and 5 then '5. 4-5km'
        when (a.distance*1.00/100000) between 5 and 6 then '6. 5-6km'
        when (a.distance*1.00/100000) between 6 and 7 then '7. 6-7km'
        when (a.distance*1.00/100000) > 7 then '8. ++7km' end as group_distance_range,
        cast(json_extract(a.extra_data,'$.re') as double) as re_system,
        JSON_ARRAY_LENGTH(json_extract(extra_data,'$.route'))*1.00/2 as cnt_order,
        greatest(13500,3750*(a.distance*1.00/100000)*1) as group_shipping_fee,
        500 as extra_fee,
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
-- select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da a  
-- where id =64527710
group by 1,2,3
)
,hub_order as 
(select 
        ho.uid as shipper_id,
        ho.slot_id,
        ho.autopay_report_id,
        ho.ref_order_id,
        ho.ref_order_category,
        r.group_id,
        coalesce(gi.total_order_in_group,1) as total_order_in_group,
        case 
        when r.group_id > 0 then coalesce(gi.group_distance_range,'single') 
        else 'single' end as distance_range,
        case when coalesce(oct.risk_bearer_id,0) != 2 then 1 else 0 end as is_hub_order,
        date(from_unixtime(ho.autopay_date_ts-3600)) as autopay_date,
        13500 as original_base_fee,
        case 
        when r.group_id > 0 then coalesce(gi.non_hub_stacked,13500)
        else 13500 end as stack_non_hub_formula_fee,
        case 
        when r.group_id > 0 then coalesce(gi.non_hub_stacked_non_extra,13500)
        else 13500 end as stack_non_hub_non_extra,
        case 
        when r.group_id > 0 then coalesce(gi.option1,13500)
        else 13500 end as opt1_fee,
        case 
        when r.group_id > 0 then coalesce(gi.option2,13500)
        else 13500 end as opt2_fee,
        case 
        when r.group_id > 0 then coalesce(gi.option1_non_extra,13500)
        else 13500 end as option1_non_extra,
        case 
        when r.group_id > 0 then coalesce(gi.option2_non_extra,13500)
        else 13500 end as option2_non_extra,

        r.city_name,
        case 
        when r.group_id > 0 then 1 else 0 end as is_group

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
,compensate as 
(select 
        autopay_date,
        ho.shipper_id,
        ho.slot_id,
        hp.extra_ship as actual_extra_ship,
        count(distinct ref_order_id) as total_order,
        case 
        when hp.extra_ship > 0 and hp.hub_type_original in ('10 hour shift') then (13500*30) - sum(stack_non_hub_formula_fee) 
        when hp.extra_ship > 0 and hp.hub_type_original in ('8 hour shift') then (13500*25) - sum(stack_non_hub_formula_fee) 
        else 0 end as compensation_non_hub,
        case 
        when hp.extra_ship > 0 and hp.hub_type_original in ('10 hour shift') then (13500*30) - sum(opt1_fee) 
        when hp.extra_ship > 0 and hp.hub_type_original in ('8 hour shift') then (13500*25) - sum(opt1_fee) 
        else 0 end as compensation_v1,
        case 
        when hp.extra_ship > 0 and hp.hub_type_original in ('10 hour shift') then (13500*30) - sum(opt2_fee) 
        when hp.extra_ship > 0 and hp.hub_type_original in ('8 hour shift') then (13500*25) - sum(opt2_fee) 
        else 0 end as compensation_v2,
        sum(opt1_fee) as opt1_fee,
        sum(opt2_fee) as opt2_fee,
        sum(stack_non_hub_formula_fee) as stack_non_hub_formula_fee



from hub_order ho 
left join driver_ops_hub_driver_performance_tab hp on ho.shipper_id = hp.uid and ho.slot_id = hp.slot_id

group by 1,2,3,4,hp.extra_ship,hp.hub_type_original
)

select  
        distance_range as stack_range,
        count(distinct ref_order_id)/cast(count(distinct autopay_date) as double) as stack_ado,
        sum(stack_non_hub_non_extra)/cast(count(distinct autopay_date) as double) as base_fee_non_hub_formula ,
        sum(stack_non_hub_formula_fee - stack_non_hub_non_extra)/cast(count(distinct autopay_date) as double) as extra_fee_non_hub_formula ,
        sum(compensation_non_hub)/cast(count(distinct autopay_date) as double) as compensation_non_hub,
        sum(option1_non_extra)/cast(count(distinct autopay_date) as double) as base_fee_opt1,
        sum(opt1_fee - option1_non_extra)/cast(count(distinct autopay_date) as double) as extra_fee_opt1,
        sum(compensation_v1)/cast(count(distinct autopay_date) as double) as compensation_v1,
        sum(option2_non_extra)/cast(count(distinct autopay_date) as double) as base_fee_opt2,
        sum(opt2_fee - option2_non_extra)/cast(count(distinct autopay_date) as double) as extra_fee_opt2,
        sum(compensation_v2)/cast(count(distinct autopay_date) as double) as compensation_v2,
        sum(original_base_fee)/cast(count(distinct autopay_date) as double) as original_base_fee,
        sum(actual_compensation)/cast(count(distinct autopay_date) as double) as actual_compensation

from 
(select 
        h.*,
        c.compensation_non_hub*1.00/c.total_order as compensation_non_hub,
        c.compensation_v1*1.00/c.total_order as compensation_v1,
        c.compensation_v2*1.00/c.total_order as compensation_v2,
        c.actual_extra_ship*1.00/c.total_order as actual_compensation


from hub_order h 
left join compensate c on h.shipper_id = c.shipper_id and h.slot_id = c.slot_id
)
where total_order_in_group =2 
group by 1


