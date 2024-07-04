with driver_cost_base as 
(select 
    *
    ,case when cardinality(split(prm_code,'_')) = 2 then split(prm_code,'_')[2] else null end as prm_code_v2
    ,(driver_cost_base + return_fee_share_basic)/exchange_rate as dr_cost_base_usd
    ,(driver_cost_surge + return_fee_share_surge)/exchange_rate as dr_cost_surge_usd
    ,bonus/exchange_rate as dr_cost_bonus_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_bad_weather_cost_hub else bf.total_bad_weather_cost_non_hub end)/exchange_rate as dr_cost_bw_fee_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_late_night_fee_temp_hub else bf.total_late_night_fee_temp_non_hub end)/exchange_rate as dr_cost_late_night_usd
    ,(case when bf.delivered_by = 'hub' then bf.total_holiday_fee_temp_hub else bf.total_holiday_fee_temp_non_hub end)/exchange_rate as dr_cost_holiday_fee

from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf
)
select 
    -- grass_date

    ,count(distinct partner_id) as total_drivers
    ,count(distinct order_id) as total_orders
    ,sum(dr_cost_base_usd) as total_dr_cost_base_usd
    ,sum(dr_cost_surge_usd) as total_dr_cost_surge_usd
    ,sum(dr_cost_bonus_usd) as total_dr_cost_bonus_usd
    ,sum(dr_cost_bw_fee_usd) as total_dr_cost_bw_fee_usd
    ,sum(dr_cost_late_night_usd) as total_dr_cost_late_night_usd
    ,sum(dr_cost_holiday_fee) as total_dr_cost_holiday_fee

from driver_cost_base
where grass_date between date '2022-05-01' and date '2022-05-31'
-- where source = 'Food'
-- where prm_code_v2 in ('QUANMOI30','XINCHAO')
group by 1
-- where grass_date between date '2022-05-21' and date '2022-05-21'
