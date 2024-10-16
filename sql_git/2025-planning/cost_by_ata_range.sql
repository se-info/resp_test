with raw as 
(select 
        raw.order_code,
        raw.city_name,
        raw.shipper_id,
        date(delivered_timestamp) as report_date,
        hour(delivered_timestamp) as hour_,
        raw.group_id,
        ogi.min_group_created,
        ogi.max_group_delivered,
        raw.created_timestamp,
        raw.last_incharge_timestamp,
        raw.delivered_timestamp,
        ogi.total_order_in_group,
        if(order_type=0,'delivery','spxi') as source,
        raw.driver_distance*1.0000/sum_single_distance as distance_allocate,
        greatest(raw.driver_distance,0.1) as driver_distance,
        case 
        when raw.driver_distance <= 3 then '1. 0 - 3km'
        when raw.driver_distance <= 4 then '2. 3 - 4km'
        when raw.driver_distance <= 6 then '3. 4 - 6km'
        when raw.driver_distance > 6 then '4. ++6km'
        end as distance_range,
        bf.driver_cost_base_n_surge+return_fee_share as driver_cost_base_n_surge,
        (case 
        when is_nan(bf.bonus) = true then 0.00 
        when bf.delivered_by = 'hub' then bf.bonus_hub
        when bf.delivered_by != 'hub' then bf.bonus_non_hub
        else null end) as driver_bonus,
        bf.delivered_by

from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab raw 

left join vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf on bf.order_id = raw.id and bf.ref_order_category = raw.order_type

left join 
(select 
        group_id,
        count(id) as total_order_in_group,
        min(last_incharge_timestamp) as min_group_created,
        max(delivered_timestamp) as max_group_delivered,
        sum(greatest(driver_distance,0.1)) as sum_single_distance
from driver_ops_raw_order_tab
where group_id > 0 
and order_status in ('Delivered')
group by 1 
) ogi on ogi.group_id = (case when raw.group_id > 0 then raw.group_id else 0 end)

where 1 = 1 
and date(delivered_timestamp) >= date'2024-08-01'
and date(delivered_timestamp) <= date'2024-08-15'
and raw.source = 'order_food'
and raw.city_id NOT IN (217,218)
AND raw.city_id != 238
and raw.order_status in ('Delivered')
and raw.shipper_id > 0 
and raw.shipper_id != 9644310 -- test
)
,s as 
(select 
        *,
        case 
        when lt_completed_adj <= 15 then '1. <= 15'
        when lt_completed_adj <= 20 then '2. <= 20'
        when lt_completed_adj <= 30 then '3. <= 30'
        when lt_completed_adj > 30 then '4. ++ 30' end as ata_range
from
(select  
        *,
        case 
        when group_id > 0 then date_diff('second',min_group_created,max_group_delivered)*1.0000/60*distance_allocate
        else date_diff('second',last_incharge_timestamp,delivered_timestamp)*1.0000/60 end as lt_completed_adj,
        date_diff('second',last_incharge_timestamp,delivered_timestamp)*1.0000/60 as e2e_original

from raw

where 1 = 1     
)
where 1 = 1
)
-- select * from s where ata_range is null
select
        distance_range,
        ata_range,
        avg(driver_distance) as avg_distance,
        avg(lt_completed_adj) as avg_lt_completed_adj,
        try(count(distinct order_code)*1.0000/count(distinct report_date)) as avg_order,
        try(count(distinct case when delivered_by = 'hub' then order_code else null end)*1.0000
                /count(distinct case when delivered_by = 'hub' then report_date else null end)) as avg_order_hub,
        try(count(distinct case when group_id > 0 then order_code else null end)*1.0000
                /count(distinct case when group_id > 0 then report_date else null end)) as avg_stacked_order,

        try(sum(driver_cost_base_n_surge + driver_bonus)*1.0000/count(distinct order_code)) as cpo_overall,
        try(sum(case when delivered_by = 'hub' then (driver_cost_base_n_surge+driver_bonus) else null end)*1.0000
                /count(distinct case when delivered_by = 'hub' then order_code else null end)) as cpo_hub,
        try(sum(case when delivered_by != 'hub' then (driver_cost_base_n_surge+driver_bonus) else null end)*1.0000
                /count(distinct case when delivered_by != 'hub' then order_code else null end)) as cpo_non_hub



from s 
where distance_range = '1. 0 - 3km'
group by 1,2
UNION ALL 
select
        distance_range,
        '5. All' ata_range,
        avg(driver_distance) as avg_distance,
        avg(lt_completed_adj) as avg_lt_completed_adj,
        try(count(distinct order_code)*1.0000/count(distinct report_date)) as avg_order,
        try(count(distinct case when delivered_by = 'hub' then order_code else null end)*1.0000
                /count(distinct case when delivered_by = 'hub' then report_date else null end)) as avg_order_hub,
        try(count(distinct case when group_id > 0 then order_code else null end)*1.0000
                /count(distinct case when group_id > 0 then report_date else null end)) as avg_stacked_order,

        try(sum(driver_cost_base_n_surge + driver_bonus)*1.0000/count(distinct order_code)) as cpo_overall,
        try(sum(case when delivered_by = 'hub' then (driver_cost_base_n_surge+driver_bonus) else null end)*1.0000
                /count(distinct case when delivered_by = 'hub' then order_code else null end)) as cpo_hub,
        try(sum(case when delivered_by != 'hub' then (driver_cost_base_n_surge+driver_bonus) else null end)*1.0000
                /count(distinct case when delivered_by != 'hub' then order_code else null end)) as cpo_non_hub



from s 
where distance_range != '1. 0 - 3km'
group by 1,2


