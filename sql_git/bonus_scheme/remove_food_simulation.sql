with raw as 
(select 
        a.id,
        a.order_code,
        a.group_id,
        a.order_type,
        case when a.city_id = dp.city_id then p.point
        else 0 end as original_point,
        a.shipper_id,
        date(a.delivered_timestamp) as report_date,
        case when a.city_id = dp.city_id then (
        case when a.order_type = 0 then p.point
        when p.point = 10 then 6
        when p.point = 12 then 8
        when p.point = 14 then 10 end) else 0 end as point_v1,
        case when a.city_id = dp.city_id then (
        case
        when a.order_type = 0 then p.point
        else p.point - 2 end) else 0 end as point_v2,
        case when a.city_id = dp.city_id then (
        case 
        when a.order_type = 0 then p.point
        else 8 end) else 0 end as point_v3,
        date_diff('second',a.last_incharge_timestamp,a.delivered_timestamp)*1.00/3600 as delivering_time,
        case 
        when a.group_id > 0 then (date_diff('second',ogi.min_group_created,ogi.max_group_delivered)*1.00/3600)/ogi.cnt_order_in_group
        else date_diff('second',a.last_incharge_timestamp,a.delivered_timestamp)*1.00/3600 end as delivering_time_adjusted,
        di.name_en as district_name

from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab a 

left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live di on di.id = a.district_id

left join (
select 
        group_id,
        count(distinct id) as cnt_order_in_group,
        min(case when is_asap = 1 then created_timestamp else first_auto_assign_timestamp end) as min_group_created,
        max(case when order_status = 'Returned' then returned_timestamp else delivered_timestamp end) as max_group_delivered


from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab
where order_status in ('Delivered','Quit','Returned') 
group by 1
) ogi on ogi.group_id = a.group_id

left join driver_ops_driver_performance_tab dp 
        on dp.shipper_id = a.shipper_id
        and dp.report_date = date(a.delivered_timestamp)

left join shopeefood.foody_partner_db__order_point_log_tab__reg_daily_s0_live p 
        on p.order_id = a.id
        and p.order_type = a.order_type
where 1 = 1 
and a.shipper_id > 0 
and a.order_status in ('Delivered','Quit','Returned')
)
select *
from
(select 
        raw.report_date,
        raw.shipper_id,
        dp.city_id,
        dp.shipper_tier,
        dp.daily_point,
        dp.sla_rate,
        dp.driver_daily_bonus,
        dp.work_hour,
        dp.driver_income as total_income,
        sum(case when raw.order_type = 0 then delivering_time_adjusted else null end)
        *1.00/avg(case when raw.order_type != 0 then delivering_time_adjusted else null end) as estimation_spxi_ado,
        count(distinct case when raw.order_type != 0 then raw.order_code else null end) as spxi_ado,

        avg(case when raw.order_type != 0 then delivering_time else null end) as avg_delivering_time,
        avg(case when raw.order_type != 0 then delivering_time_adjusted else null end) as avg_delivering_time_adjusted,
        -- sum(case when raw.order_type = 0 then delivering_time else null end) as food_working_time,
        count(distinct raw.order_code) as ado,
        sum(raw.original_point) as actual_earned_point,
        sum(case when order_type != 0 then raw.original_point else null end) as spxi_points,
        array_agg(distinct district_name) as district_name_agg
        
from raw 

left join driver_ops_driver_performance_tab dp 
        on dp.shipper_id = raw.shipper_id
        and dp.report_date = raw.report_date

-- where raw.report_date between date_trunc('month',current_date) - interval '1' month and current_date - interval '1' day
where raw.report_date = date'2024-01-25'
and dp.shipper_tier != 'Hub'
and dp.city_id in (217,218)
and dp.total_order_spxi > 0  
group by 1,2,3,4,5,6,7,8,9,dp.online_hour
)