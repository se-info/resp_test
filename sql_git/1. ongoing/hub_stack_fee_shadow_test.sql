with group_info as 
(select 
        id,
        group_code,
        max(stacked_per_order_opt1) as stacked_per_order_opt1,
        max(stacked_per_order_opt2) as stacked_per_order_opt2,
        max(stacked_per_order_opt3) as stacked_per_order_opt3,
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
        least(13500*JSON_ARRAY_LENGTH(json_extract(extra_data,'$.route'))*1.00/2
        ,(greatest(3750*(a.distance*1.00/100000)*1,((13500+13500)*0.7)) + 1000*JSON_ARRAY_LENGTH(json_extract(extra_data,'$.route'))*1.00/2))
            /(JSON_ARRAY_LENGTH(json_extract(extra_data,'$.route'))*1.00/2) as stacked_per_order_opt1,
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
            /(JSON_ARRAY_LENGTH(json_extract(extra_data,'$.route'))*1.00/2) as stacked_per_order_opt2,
        least(
        27000
        ,greatest(13500, 
            -- # calculated new extra_fee
            (3750*(a.distance*1.00/100000)*1) )+
            5000
        )
            /(JSON_ARRAY_LENGTH(json_extract(extra_data,'$.route'))*1.00/2) as stacked_per_order_opt3,
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
        when r.group_id > 0 then coalesce(gi.stacked_per_order_opt1,13500)
        else 13500 end as opt1_fee,
        case 
        when r.group_id > 0 then gi.stacked_per_order_opt2
        else 13500 end as opt2_fee,
        case 
        when r.group_id > 0 then gi.stacked_per_order_opt3
        else 13500 end as opt3_fee,
        r.city_name,
        case 
        when r.group_id > 0 then 1 else 0 end as is_group,
        r.delivery_id

from shopeefood.foody_partner_archive_db__shipper_hub_order_tab__reg_daily_s0_live ho

left join driver_ops_raw_order_tab r on ho.ref_order_id = r.id and ho.ref_order_category = r.order_type

left join group_info gi on gi.id = r.group_id

left join 
(select id,cast(json_extract_scalar(oct.extra_data, '$.risk_bearer_type') as int) as risk_bearer_id 
from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
where date(from_unixtime(submit_time - 3600)) between date'2024-06-19' - interval '7' day and date'2024-06-19'
) oct
    on ho.ref_order_id = oct.id and ho.ref_order_category = 0
where 1 = 1 
and date(from_unixtime(ho.autopay_date_ts-3600)) between date'2024-06-19' - interval '7' day and date'2024-06-19'
)
,raw AS 
(select 
        SPLIT(REGEXP_REPLACE(delivery_order_ids,'[\[\]]',''),',') AS ids,
        * 


from dev_vnfdbi_opsndrivers.driver_ops_hub_stack_fee_show_test_1906
)
-- SELECT 
--         group_id,
--         ARRAY_AGG(DISTINCT delivery_id) AS log_delivery_ids,
--         SUM(hub_stack_shipping_fee) AS log_new_stack_fee,
--         SUM(opt1_fee) AS re_calculated_new_stack_fee 
SELECT *        
FROM
(SELECT 
        t.delivery_id,
        TRY_CAST(raw.final_shipping_fee AS DOUBLE)/CARDINALITY(raw.ids) final_shipping_fee,
        TRY_CAST(raw.total_single_shipping_fee AS DOUBLE)/CARDINALITY(raw.ids) total_single_shipping_fee,
        TRY_CAST(raw.hub_stack_shipping_fee AS DOUBLE)/CARDINALITY(raw.ids) hub_stack_shipping_fee,
        TRY_CAST(raw.hub_stack_fee AS DOUBLE)/CARDINALITY(raw.ids) hub_stack_fee,
        TRY_CAST(raw.hub_extra_fee AS DOUBLE)/CARDINALITY(raw.ids) hub_extra_fee,
        CARDINALITY(raw.ids) AS len_ids,
        ho.opt1_fee,
        ho.ref_order_id,
        ho.group_id



FROM raw 

CROSS JOIN UNNEST (raw.ids) AS t(delivery_id)

LEFT JOIN hub_order ho ON ho.delivery_id = CAST(t.delivery_id AS BIGINT)

WHERE CARDINALITY(raw.ids) >= 2 )
WHERE opt1_fee IS NULL
-- GROUP BY 1 