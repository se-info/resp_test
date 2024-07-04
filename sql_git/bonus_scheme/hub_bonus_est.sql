with autopay_config as 
(select 
        a.id,
        shift_category,
        cast(json_extract(b.order_config,'$.order_idx') as bigint) as order_index,
        cast(json_extract(b.order_config,'$.shipping_shared') as bigint) as order_ship_shared,
        cast(json_extract(b.order_config,'$.bonus') as bigint) as order_bonus


from shopeefood.foody_internal_db__shipper_hub_income_config_tab__reg_daily_s0_live a 

cross join unnest (cast(json_extract(a.extra_data,'$.order_configs') as array<json>)) as b(order_config)
)
,m as 
(select 
        raw.id,
        raw.shipper_id,
        hub.slot_id,
        date(raw.delivered_timestamp) as report_date,
        row_number()over(partition by raw.shipper_id,date(raw.delivered_timestamp),hub.slot_id order by raw.delivered_timestamp asc ) as rank_,
        hub.autopay_config_id,
        hi.kpi

from driver_ops_raw_order_tab raw 

left join shopeefood.foody_partner_archive_db__shipper_hub_order_tab__reg_continuous_s0_live hub 
    on hub.ref_order_id = raw.id
    and hub.ref_order_category = raw.order_type

left join driver_ops_hub_driver_performance_tab hi 
        on hi.uid = raw.shipper_id and hi.slot_id = hub.slot_id and hi.registered_ = 1

where driver_policy = 2 
and order_status = 'Delivered'
and order_type = 0
)
,f as 
(select 
        m.*,
        a.order_ship_shared,
        case 
        when a.shift_category = 1 then 'hub5'
        when a.shift_category = 2 then 'hub8'
        when a.shift_category = 3 then 'hub10'
        when a.shift_category = 4 then 'hub3' end as shift_name, 
        a.shift_category,
        case 
        when m.kpi = 1 then a.order_bonus else 0 end as order_bonus,
        case 
        -- 5
        when m.kpi = 1 and a.shift_category = 1 and rank_ between 14 and 19 then 4000
        when m.kpi = 1 and a.shift_category = 1 and rank_ > 19 then 6000
        -- 8
        when m.kpi = 1 and a.shift_category = 2 and rank_ between 26 and 29 then 4000
        when m.kpi = 1 and a.shift_category = 2 and rank_ > 29 then 6000
        -- 10
        when m.kpi = 1 and a.shift_category = 3 and rank_ >= 31 then 6000
        -- 3
        when m.kpi = 1 and a.shift_category = 4 and rank_ between 7 and 10 then 2000
        when m.kpi = 1 and a.shift_category = 4 and rank_ > 10 then 3000
        else 0 end as bonus_v1,
        case 
        -- 5
        when m.kpi = 1 and a.shift_category = 1 and rank_ between 14 and 19 then 4000
        when m.kpi = 1 and a.shift_category = 1 and rank_ > 19 then 8000
        -- 8
        when m.kpi = 1 and a.shift_category = 2 and rank_ between 26 and 29 then 4000
        when m.kpi = 1 and a.shift_category = 2 and rank_ > 29 then 10000
        -- 10
        when m.kpi = 1 and a.shift_category = 3 and rank_ between 31 and 35 then 6000
        when m.kpi = 1 and a.shift_category = 3 and rank_ > 35 then 10000
        -- 3
        when m.kpi = 1 and a.shift_category = 4 and rank_ between 7 and 10 then 2000
        when m.kpi = 1 and a.shift_category = 4 and rank_ > 10 then 6000
        else 0 end as bonus_v2,
        case 
        -- 5
        when m.kpi = 1 and a.shift_category = 1 and rank_ between 14 and 16 then 4000
        when m.kpi = 1 and a.shift_category = 1 and rank_ > 16 then 8000
        -- 8
        when m.kpi = 1 and a.shift_category = 2 and rank_ between 26 and 29 then 4000
        when m.kpi = 1 and a.shift_category = 2 and rank_ > 29 then 10000
        -- 10
        when m.kpi = 1 and a.shift_category = 3 and rank_ between 31 and 35 then 6000
        when m.kpi = 1 and a.shift_category = 3 and rank_ > 35 then 10000
        -- 3
        when m.kpi = 1 and a.shift_category = 4 and rank_ between 7 and 9 then 2000
        when m.kpi = 1 and a.shift_category = 4 and rank_ >= 10 then 5000
        else 0 end as bonus_v3

from m 
left join autopay_config a on a.id = m.autopay_config_id and m.rank_ = a.order_index
where (m.report_date between date'2024-01-08' and date'2024-01-14'
or m.report_date = date'2023-10-20')
and m.slot_id is not null
)
select 
        report_date,
        shift_name,
        bonus_v1,
        bonus_v3,
        count(distinct (shipper_id,slot_id)) as num_of_driver,
        count(distinct case when kpi = 1 then (shipper_id,slot_id) else null end) as num_of_driver_pass_kpis,
        count(distinct case when bonus_v1 > 0 then (shipper_id,slot_id) else null end) as num_of_driver_pass_bonus,
        sum(order_bonus) as bonus_current,
        sum(bonus_v1) as bonus_v1,
        sum(bonus_v2) as bonus_v2,      
        sum(bonus_v3) as bonus_v3,
        count(distinct id) as total_order

from f 

group by 1,2,3,4



