with driver_cost_base as 
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


from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf
left join shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
    on bf.order_id = oct.id
)
,shift_info as
(
    select 
        hc.uid shipper_id
        ,DATE(FROM_UNIXTIME(hc.report_date - 3600)) AS report_date
        ,CAST(json_extract(hc.extra_data,'$.shift_category_name') AS varchar) as shift_category_name
    FROM shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hc
)
,raw as
(select 
    -- coalesce(shift.shift_category_name,sm.shift_name_text)
    coalesce(shift.shift_category_name,sm.shift_name_text) as hub_type
    ,base.*
    -- distinct coalesce(shift.shift_category_name,sm.shift_name_text) as hub_type
from driver_cost_base base
left join shift_info shift
    on base.partner_id = shift.shipper_id and base.grass_date = shift.report_date
left join shopeefood.foody_mart__profile_shipper_master sm
    on base.partner_id = sm.shipper_id and base.grass_date = try_cast(sm.grass_date as date)
where (base.grass_date between date '2021-12-01' and date '2021-12-31' 
        or base.grass_date between date '2022-03-01' and date '2022-03-31' 
        or base.grass_date between date '2022-06-01' and date '2022-06-30' 
        or base.grass_date between date '2022-07-01' and date '2022-07-24' 
)
and base.delivered_by = 'hub'
-- and base.grass_date between date '2022-07-01' and date '2022-07-24' 
and source in ('Food','Market')
)
select 
    date_trunc('month',grass_date) as grass_month
    ,case 
        when regexp_like(hub_type,'3 hour shift|HUB 3') then '1.Hub-3' 
        when regexp_like(hub_type,'5 hour shift|HUB 5') then '2.Hub-5' 
        when regexp_like(hub_type,'8 hour shift|HUB 8') then '3.Hub-8' 
        when regexp_like(hub_type,'10 hour shift|HUB 10') then '4.Hub-10'
        else 'OTH' end as hub_type 
    ,sum(dr_cost_base_usd + dr_cost_surge_usd) / count(distinct order_id) as cpo_base_surge
    ,sum(dr_cost_base_usd + dr_cost_surge_usd + dr_cost_bonus_usd) / count(distinct order_id) as cpo_
    ,count(distinct order_id) as total_orders
from raw
where regexp_like(hub_type,'HUB]Ca 2|Ca Full 1|test712|Part Time') = false
group by 1,2
order by grass_month,hub_type
limit 100