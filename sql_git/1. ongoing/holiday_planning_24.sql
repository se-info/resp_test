with base as 
(select 
        raw.grass_date,
        raw.partner_id as shipper_id,
        raw.city_name_full,
        raw.order_id,
        raw.ref_order_category,
        raw.driver_cost_base_n_surge,
        raw.bonus,
        raw.total_bad_weather_cost,
        raw.total_late_night_cost,
        raw.total_holiday_fee_cost,
        sm.city_name


from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level raw 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = raw.partner_id and sm.grass_date = 'current'

where 1 = 1 
and (raw.grass_date between date'2024-04-15' and date'2024-05-06'
or 
raw.grass_date between date'2023-08-21' and date'2023-09-05')
and raw.status in (7,11)
)
select 
        grass_date,
        city_name,
        count(distinct (shipper_id,grass_date)) as a1,
        count(distinct case when ref_order_category = 0 then (shipper_id,grass_date) else null end) as a1_delivery,
        count(distinct case when ref_order_category != 0 then (shipper_id,grass_date) else null end) as a1_spxi,
        count(distinct order_id) as total_order,
        count(distinct case when ref_order_category = 0 then order_id else null end) as delivery_order,
        count(distinct case when ref_order_category != 0 then order_id else null end) as spxi_order,

        try(count(distinct order_id)/count(distinct (shipper_id,grass_date))*1.0000 )as driver_ado,
        try(count(distinct case when ref_order_category = 0 then order_id else null end)/
            count(distinct case when ref_order_category = 0 then (shipper_id,grass_date) else null end)*1.0000 )as driver_delivery_ado,
        try(count(distinct case when ref_order_category != 0 then order_id else null end)/
            count(distinct case when ref_order_category != 0 then (shipper_id,grass_date) else null end)*1.0000 )as driver_spxi_ado,

        try((sum(driver_cost_base_n_surge) + sum(bonus) + sum(total_bad_weather_cost) + sum(total_late_night_cost) + sum(total_holiday_fee_cost))/
            count(distinct (shipper_id,grass_date))*1.0000 )as driver_earning,

        try((sum(if(ref_order_category =0,driver_cost_base_n_surge,0)) + sum(if(ref_order_category =0,bonus,0)) 
        + sum(if(ref_order_category =0,total_bad_weather_cost,0)) + sum(if(ref_order_category =0,total_late_night_cost,0)) 
        + sum(if(ref_order_category =0,total_holiday_fee_cost,0)))/count(distinct case when ref_order_category = 0 then (shipper_id,grass_date) else null end)
         )as delivery_driver_earning,

        try((sum(if(ref_order_category !=0,driver_cost_base_n_surge,0)) + sum(if(ref_order_category !=0,bonus,0)) 
        + sum(if(ref_order_category !=0,total_bad_weather_cost,0)) + sum(if(ref_order_category !=0,total_late_night_cost,0)) 
        + sum(if(ref_order_category !=0,total_holiday_fee_cost,0)))/count(distinct case when ref_order_category != 0 then (shipper_id,grass_date) else null end)
        )as spxi_driver_earning

from base 

group by 1,2

