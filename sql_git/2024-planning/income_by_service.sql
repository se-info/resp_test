with raw_order_tab as 
(select 
        coalesce(date(raw.delivered_timestamp),created_date) as delivered,
        shipper_id,
        raw.order_code,
        raw.delivery_id,
        raw.source,
        raw.order_type,
        case 
        when driver_policy = 2 then 13500 else doet.delivery_cost end as shipping_fee,
        coalesce(doet.return_fee,0) as return_fee,
        coalesce(doet.parking_fee,0) as parking_fee,
        raw.holiday_service_fee,
        raw.bad_weather_fee,
        raw.late_night_service_fee,
        case 
        when driver_policy = 2 then 1 else 0 end as is_hub,
        raw.city_name,
        raw.created_timestamp

from driver_ops_raw_order_tab raw

-- driver_fee
left join 
(select 
        order_id,
        cast(json_extract(order_data,'$.shopee.shipping_fee_info.return_fee') as bigint) as return_fee, 
        cast(json_extract(order_data,'$.delivery.bad_weather_fee.user_pay_amount') as bigint) as bad_weather_fee,
        cast(json_extract(order_data,'$.delivery.parking_fee') as bigint) as parking_fee,
        cast(json_extract(order_data,'$.delivery.shipping_fee.total') as bigint) as delivery_cost,
        CAST(json_extract(order_data,'$.shipper_policy.type') AS BIGINT) AS driver_policy 

from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da raw 
where date(dt) = current_date - interval '1' day 
) doet 
    on doet.order_id = raw.delivery_id

where raw.order_status in ('Delivered','Returned')
)
,summary as 
(select
        raw.delivered as report_date,
        raw.shipper_id,
        dp.shipper_tier,
        coalesce(dp.food_bonus,0) food_bonus,
        coalesce(dp.spxi_bonus,0) spxi_bonus,
        case 
        when dp.shipper_tier = 'Hub' then dp.income else 0 end as hub_income,
        count(distinct case when source in ('order_fresh','order_market','order_food')  then order_code else null end) as order_food,
        count(distinct case when source in ('now_ship_merchant','now_ship_multi_drop','now_ship_same_day','now_ship_shopee') then order_code else null end) as order_spxi,
        coalesce(sum(case when source in ('order_fresh','order_market','order_food')  then shipping_fee else null end),0) as food_ship_shared,
        coalesce(sum(case when source in ('now_ship_merchant','now_ship_multi_drop','now_ship_same_day','now_ship_shopee')  then shipping_fee else null end),0) as spxi_ship_shared,
        coalesce(sum(case when source in ('now_ship_merchant','now_ship_multi_drop','now_ship_same_day','now_ship_shopee')  then return_fee else null end),0) as spxi_return_fee,
        coalesce(sum(bad_weather_fee) + sum(parking_fee) + sum(late_night_service_fee),0) as other_income,
        min_by(raw.city_name,raw.created_timestamp) as first_city_name

from raw_order_tab raw

left join 
(select 
        report_date,
        shipper_id,
        case 
        when shipper_tier = 'Hub' and hub_order > 0 then 'Hub'
        when shipper_tier = 'Hub' and hub_order = 0 then 'Level 1' else shipper_tier end as shipper_tier,
        (driver_income/total_order)*total_order_food as income,
        driver_daily_bonus,
        (driver_daily_bonus*1.0000/total_order) * coalesce(total_order_food,0) as food_bonus,
        (driver_daily_bonus*1.0000/total_order) * coalesce(total_order_spxi,0) as spxi_bonus

from driver_ops_driver_performance_tab
where total_order > 0 
) dp on dp.shipper_id = raw.shipper_id and raw.delivered = dp.report_date

where (raw.delivered between date'2023-09-01' and date'2023-09-30'
or 
raw.delivered between date'2022-12-01' and date'2022-12-31')  

group by 1,2,3,4,5,6
)
select 
        date_trunc('month',report_date) as month_,
        case 
        when first_city_name in ('HCM City','Ha Noi City' ,'Da Nang City') then first_city_name
        when first_city_name IN ('Hai Phong City', 'Hue City', 'Can Tho City', 'Dong Nai', 'Binh Duong', 'Vung Tau') then first_city_name
        when regexp_like(lower(first_city_name),'dak lak|thanh hoa|binh thuan|binh dinh') = true THEN 'new cities' 
        else 'T3' end as city_group,
        sum(order_food)/cast(count(distinct report_date) as double) as food,
        sum(order_spxi)/cast(count(distinct report_date) as double) as spxi,
        sum(case when shipper_tier = 'Hub' then hub_income else food_ship_shared + food_bonus end)/cast(count(distinct (shipper_id,report_date)) as double) as food_income,
        sum(spxi_ship_shared + spxi_return_fee)/cast(count(distinct (shipper_id,report_date)) as double) as spxi_income,
        sum(other_income)/cast(count(distinct (shipper_id,report_date)) as double) as other_income,
        count(distinct (shipper_id,report_date))/cast(count(distinct report_date) as double) as a1 

from summary
group by 1,2



