with group_info as 
(select 
        id,
        group_code,
        max(stacked_per_order_opt1) as stacked_per_order_opt1
        
from
(select 
        a.id,
        a.group_code,
        a.distance*1.00/100000 as group_distance,
        cast(json_extract(a.extra_data,'$.re') as double) as re_system,
        r.cnt_order*1.00 as cnt_order,
        3750*(a.distance*1.00/100000)*1 as group_shipping_fee,
        500 as extra_fee,
        13500*r.cnt_order*1.00 AS sum_single,
        least(
            13500*r.cnt_order*1.00
        ,(greatest(3750*(a.distance*1.00/100000)*1,((13500*r.cnt_order)*0.7)) + (1000*(r.no_pickup - 1) + 1000*(no_dropoff - 1) ) *1.00)
        )
            /r.cnt_order*1.00 as stacked_per_order_opt1
    

from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da a 

left join 
(select 
        group_id,
        order_type,
        count(distinct order_code) as cnt_order,
        count(sender_name) as no_pickup,
        count(receiver_name) as no_dropoff

from driver_ops_raw_order_tab
where order_status IN ('Delivered','Quit','Returned')
group by 1,2 
) r ON r.group_id = a.id and r.order_type = a.ref_order_category 

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
        gi.group_code,
        case when coalesce(oct.risk_bearer_id,0) != 2 then 1 else 0 end as is_hub_order,
        date(from_unixtime(ho.autopay_date_ts-3600)) as autopay_date,
        13500 as original_base_fee,
        case 
        when r.group_id > 0 then coalesce(gi.stacked_per_order_opt1,13500)
        else 13500 end as opt1_fee,
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
where date(from_unixtime(submit_time - 3600)) between current_date - interval '30' day and current_date - interval '1' day
) oct
    on ho.ref_order_id = oct.id and ho.ref_order_category = 0
where 1 = 1 
and date(from_unixtime(ho.autopay_date_ts-3600)) between current_date - interval '30' day and current_date - interval '1' day
)
-- SELECT * FROM hub_order WHERE delivery_id = 516374029
,raw AS 
(select 
        SPLIT(REGEXP_REPLACE(delivery_order_ids,'[\[\]]',''),',') AS ids,
        FROM_UNIXTIME(TRY_CAST(REPLACE("timestamp",'.00','') AS BIGINT)/1000 - 3600) AS "cast_timestamp",
        *

from
(select * from dev_vnfdbi_opsndrivers.driver_ops_shadow_test_hub_stack_p1
UNION ALL 
select * from dev_vnfdbi_opsndrivers.driver_ops_shadow_test_hub_stack_p2
)
)
,m AS 
(SELECT *,ROW_NUMBER()OVER(PARTITION BY delivery_id ORDER BY cast_timestamp DESC) AS rank_
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
        ho.group_id,
        ho.group_code,
        raw.cast_timestamp



FROM raw 

CROSS JOIN UNNEST (raw.ids) AS t(delivery_id)

LEFT JOIN hub_order ho ON ho.delivery_id = CAST(t.delivery_id AS BIGINT)

WHERE CARDINALITY(raw.ids) >= 2 )
WHERE opt1_fee IS NOT NULL
)
select * from
(SELECT 
        group_id,
        group_code,
        ARRAY_AGG(DISTINCT delivery_id) AS log_delivery_ids,
        ARRAY_AGG(DISTINCT ref_order_id) AS hub_ref_order_id,
        SUM(hub_stack_shipping_fee) AS log_new_stack_fee,
        SUM(opt1_fee) AS re_calculated_new_stack_fee  

FROM m 
WHERE rank_ = 1 
GROUP BY 1,2
HAVING (CARDINALITY(ARRAY_AGG(DISTINCT delivery_id)) >= 2)
)