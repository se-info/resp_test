with earning_tab as
(SELECT
    date_, 
    exchange_rate,
    case 
        when i.city_name_full in ('HCM City','Ha Noi City', 'Hai Phong City') and current_driver_tier = 'Hub' then 'Hub' 
        when i.city_name in ('HCM','HN') and current_driver_tier = 'T1' then 'T1'
        when i.city_name in ('HCM','HN') and current_driver_tier = 'T2' then 'T2'
        when i.city_name in ('HCM','HN') and current_driver_tier = 'T3' then 'T3'
        when i.city_name in ('HCM','HN') and current_driver_tier = 'T4' then 'T4'
        when i.city_name in ('HCM','HN') and current_driver_tier = 'T5' then 'T5'
                     
        when i.city_name not in ('HCM','HN') then 'OTH'
        else 'full time' 
    end as driver_tier
    ,total_earning_before_tax
    ,partner_id
from vnfdbi_opsndrivers.snp_foody_shipper_income_tab i
LEFT JOIN (SELECT distinct
    grass_date,
    exchange_rate
    FROM 
    mp_order.dim_exchange_rate__reg_s0_live 
    WHERE
    currency='VND'
    and grass_date >= date('2020-12-28')
)xrate on xrate.grass_date = i.date_
left join shopeefood.foody_mart__profile_shipper_master sm
    on i.partner_id = sm.shipper_id and i.date_ = try_cast(sm.grass_date as date)
where sm.shipper_type_id != 3 and sm.shipper_status_code = 1                                                                                                                  
)
,daily_income as
(select 
    date_trunc('month',date_) as report_month
    ,sum(case when driver_tier = 'Hub' then total_earning_before_tax/exchange_rate else 0 end) / count(distinct case when driver_tier = 'Hub' then (date_,partner_id) else null end) as hub_earning
    ,sum(case when driver_tier != 'Hub' then total_earning_before_tax/exchange_rate else 0 end) / count(distinct case when driver_tier != 'Hub' then (date_,partner_id) else null end) as non_hub_earning
    ,sum(total_earning_before_tax/exchange_rate) /  count(distinct (date_,partner_id)) as overall_earning
from earning_tab
where date_ between date_trunc('month', current_date - interval '1' day - interval '1' month) and current_date - interval '1' day
                                   
group by 1
)
select 
    *
from daily_income