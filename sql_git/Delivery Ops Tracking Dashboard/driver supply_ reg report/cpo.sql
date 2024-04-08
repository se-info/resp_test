WITH driver_cost_base as 
(select 
    bf.*
                                                          
                                                                                      
                                              
    ,(driver_cost_base + return_fee_share_basic)/exchange_rate as dr_cost_base_usd
    ,(driver_cost_surge + return_fee_share_surge)/exchange_rate as dr_cost_surge_usd
    ,(case 
        when is_nan(bonus) = true then 0.00 
        when delivered_by = 'hub' then bonus_hub
        when delivered_by != 'hub' then bonus_non_hub
        else null end)  /exchange_rate as dr_cost_bonus_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_bad_weather_cost_hub else bf.total_bad_weather_cost_non_hub end)/exchange_rate as dr_cost_bw_fee_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_late_night_fee_temp_hub else bf.total_late_night_fee_temp_non_hub end)/exchange_rate as dr_cost_late_night_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_holiday_fee_temp_hub else bf.total_holiday_fee_temp_non_hub end)/exchange_rate as dr_cost_holiday_fee

from vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf
                                                                                                                   
WHERE grass_date != date'2023-06-06'
AND status = 7
AND source in ('Food')
and city_name in ('HCM','HN')
and distance <= 3.6
)
select 

    --  CASE WHEN city_name_full IN ('HCM City','Ha Noi City','Da Nang City') THEN city_name_full ELSE 'Others' END AS city_group
     date_trunc('month',grass_date) as month_    
    ,delivered_by
    ,count(distinct order_id)/CAST(COUNT(DISTINCT grass_date) AS DOUBLE) AS total_orders
    ,(sum(dr_cost_base_usd) + sum(dr_cost_surge_usd) + sum(dr_cost_bonus_usd))/count(order_id) as driver_cpo_base_surge_bonus
    ,(sum(dr_cost_base_usd)+ sum(dr_cost_surge_usd) )/count(order_id) AS base_n_surge
    ,sum(dr_cost_base_usd)/count(order_id) AS base 
    ,sum(dr_cost_surge_usd)/count(order_id) AS surge
    ,sum(dr_cost_bonus_usd)/count(order_id) AS bonus 

FROM driver_cost_base
where date_trunc('month',grass_date) between date_trunc('month',current_date) - interval '1' month and current_date - interval '1' day  
GROUP BY 1,2    

