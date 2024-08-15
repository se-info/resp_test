with order_performance as 
(select 
        report_date,
        shipper_id,
        sum(distance) as overall_distance,
        sum(case when source = 'delivery' then distance else 0 end) as delivery_distance,
        sum(case when source != 'delivery' then distance else 0 end) as spxi_distance,

        sum(delivery_time) as overall_delivery_time,
        sum(case when source = 'delivery' then delivery_time else 0 end) as delivery_delivery_time,
        sum(case when source != 'delivery' then delivery_time else 0 end) as spxi_delivery_time,

        count(distinct case when group_id > 0 then order_code else null end) as total_stack,
        count(distinct case when group_id > 0 and source = 'delivery' then order_code else null end) as delivery_stack,
        count(distinct case when group_id > 0 and source != 'delivery' then order_code else null end) as spxi_stack

from
(select  
        date(delivered_timestamp) as report_date,
        if(order_type=0,'delivery','spxi') as source,
        order_code,
        group_id,
        if(driver_policy=2,1,0) as is_hub,
        date_diff('second',last_incharge_timestamp,delivered_timestamp)/60.0000 as delivery_time,
        distance,
        shipper_id

from driver_ops_raw_order_tab

where 1 = 1 
and order_status = 'Delivered'
and date(delivered_timestamp) between date'2024-07-01' and date'2024-07-31'
)
group by 1,2 
)
,summary as 
(select 
        dp.report_date as date_ts,
        dp.shipper_id as uid,
        case when dp.city_id in (217,218,219) then dp.city_name
        else 'oth' end as city_group,        
        case 
        when dp.total_order <= 8 then '1. 0 - 8'
        when dp.total_order <= 14 then '2. 8 - 14'
        when dp.total_order <= 22 then '3. 14 - 22'
        when dp.total_order <= 30 then '4. 22 - 30'
        when dp.total_order <= 40 then '5. 30 - 40'
        when dp.total_order > 40 then '6. ++40' end as order_range,
        dp.total_order,
        dp.stack_order,
        dp.group_order,
        dp.total_order_food,
        dp.total_order_spxi,
        op.*,
        ds.online_,
        case 
        when dp.shipper_type = 12 and hub_order = 0 then '1. non hub'
        when dp.shipper_type = 12 and hub_order > 0 then '2. hub'
        else '1. non hub' end as type_,
        di.total_ship_shared + di.driver_daily_bonus + di.total_other_income as driver_income,
        di.ship_shared_delivery + di.daily_bonus_delivery + di.other_income_delivery as delivery_income,
        dp.total_order*1.00/ds.online_ as productivity,
        case 
        when (dp.total_order*1.00/ds.online_) <= 0.5 then '1. 0.5'
        when (dp.total_order*1.00/ds.online_) <= 1 then '2. 1'
        when (dp.total_order*1.00/ds.online_) <= 1.5 then '3. 1.5'
        when (dp.total_order*1.00/ds.online_) <= 2 then '4. 2'
        when (dp.total_order*1.00/ds.online_) <= 2.5 then '5. 2.5'
        when (dp.total_order*1.00/ds.online_) <= 3 then '6. 3'
        when (dp.total_order*1.00/ds.online_) <= 3.5 then '7. 3.5'
        when (dp.total_order*1.00/ds.online_) <= 4 then '8. 4'
        when (dp.total_order*1.00/ds.online_) <= 4.5 then '9. 4.5'
        when (dp.total_order*1.00/ds.online_) <= 5 then '10. 5'
        when (dp.total_order*1.00/ds.online_) > 5 then '11. ++5'
        end as productivity_range
        



from driver_ops_driver_performance_tab dp

left join order_performance op on op.report_date = dp.report_date and op.shipper_id = dp.shipper_id

left join  
(select         
        created,
        uid,
        sum(online_by_hour)/3600.00 as online_

from driver_ops_driver_supply_tab
group by 1,2
)ds on ds.created = dp.report_date and ds.uid = dp.shipper_id

left join driver_ops_driver_income_tracking_tab di on di.report_date = dp.report_date and di.shipper_id = dp.shipper_id

where dp.total_order > 0 
and dp.report_date between date'2024-07-01' and date'2024-07-31'
and ds.online_ is not null
)
-- select * from summary 
select
        date_trunc('month',date_ts) as month_,
        type_,
        city_group,
        productivity_range,
        count(distinct (uid,report_date))*1.00/count(distinct report_date) as avg_a1,
        avg(total_order) as avg_order,
        avg(total_stack) as avg_stack_order,
        avg(delivery_stack) as avg_delivery_stack,
        avg(spxi_stack) as avg_spxi_stack,
        avg(total_order_food) as avg_delivery_order,
        avg(total_order_spxi) as avg_spxi_order,
        sum(overall_distance)*1.00/sum(total_order) as avg_overall_distance,
        sum(delivery_distance)*1.00/sum(total_order_food) as avg_delivery_distance,
        sum(spxi_distance)*1.00/sum(total_order_spxi) as avg_spxi_distance,

        sum(overall_delivery_time)*1.00/sum(total_order) as avg_overall_delivery_time,
        sum(delivery_delivery_time)*1.00/sum(total_order_food) as avg_delivery_delivery_time,
        sum(spxi_delivery_time)*1.00/sum(total_order_spxi) as avg_spxi_delivery_time,

        avg(online_) as avg_supply_hour,

        avg(driver_income) as avg_income,
        avg(delivery_income) as avg_delivery_income
        

from summary 

group by 1,2,3,4

/*
A1
Avg daily ADO
% SPXI on Driver ADO
% Stacked

Avg distance
ATA

Suplly hour
Daily income
"Driver Cost
(Delivery service)"
*/