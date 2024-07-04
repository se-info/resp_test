with raw as 
(select 
        it.order_id,
        name.name,
        r.shipper_id,
        r.city_name,
        r.created_date,
        r.order_status,
        it.quantity

from shopeefood.foody_order_db__order_item_completed_tab__reg_daily_s0_live it 

left join driver_ops_raw_order_tab r on r.id = it.order_id and r.order_type = 0

left join shopeefood.foody_merchant_db__dish_tab__reg_daily_s0_live name on it.dish_id = name.id
where r.created_date = date'2024-03-08'
and r.order_status = 'Delivered'
and r.shipper_id > 0 
order by order_id desc
)
select
        raw.created_date,
        raw.order_id,
        raw.shipper_id,
        dp.sla_rate,
        dp.city_name,
        sum(quantity) as total_item,
        array_join(array_agg(distinct name),',') as dish_list

from raw 

left join driver_ops_driver_performance_tab dp 
        on dp.shipper_id = raw.shipper_id
        and dp.report_date = raw.created_date

where 1 = 1 
group by 1,2,3,4,5