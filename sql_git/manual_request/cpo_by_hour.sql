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
    ,from_unixtime(oct.final_delivered_time-3600) as delivered_timestamp
    ,hour(from_unixtime(oct.final_delivered_time-3600)) as delivered_hour

from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf
left join shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
    on bf.order_id = oct.id
)
,cpo_base as
(
    select 
    city_name_full
    ,date_
    ,delivered_hour
    ,source
    -- orders
    ,count(distinct order_id) as total_orders
    ,count(case when delivered_by = 'hub' then order_id else null end) as total_order_hub
    ,count(case when delivered_by != 'hub' then order_id else null end) as total_order_non_hub

    -- cost overall
    ,sum(dr_cost_base_usd) as total_dr_cost_base_usd
    ,sum(dr_cost_surge_usd) as total_dr_cost_surge_usd
    ,sum(dr_cost_bonus_usd) as total_dr_cost_bonus_usd
    ,sum(dr_cost_bw_fee_usd) as total_dr_cost_bw_fee_usd
    ,sum(dr_cost_late_night_usd) as total_dr_cost_late_night_usd
    ,sum(dr_cost_holiday_fee) as total_dr_cost_holiday_fee

    -- hub
    ,sum(case when delivered_by = 'hub' then dr_cost_base_usd else 0 end) hub_dr_cost_base_usd
    ,sum(case when delivered_by = 'hub' then dr_cost_surge_usd else 0 end) hub_dr_cost_surge_usd
    ,sum(case when delivered_by = 'hub' then dr_cost_bonus_usd else 0 end) as hub_dr_cost_bonus_usd
    ,sum(case when delivered_by = 'hub' then dr_cost_bw_fee_usd else 0 end) as hub_dr_cost_bw_fee_usd
    ,sum(case when delivered_by = 'hub' then dr_cost_late_night_usd else 0 end) as hub_dr_cost_late_night_usd
    ,sum(case when delivered_by = 'hub' then dr_cost_holiday_fee else 0 end) as hub_dr_cost_holiday_fee

    -- non_hub
    ,sum(case when delivered_by != 'hub' then dr_cost_surge_usd else 0 end) non_hub_dr_cost_surge_usd
    ,sum(case when delivered_by != 'hub' then dr_cost_base_usd else 0 end) non_hub_dr_cost_base_usd
    ,sum(case when delivered_by != 'hub' then dr_cost_bonus_usd else 0 end) as non_dr_cost_bonus_usd
    ,sum(case when delivered_by != 'hub' then dr_cost_bw_fee_usd else 0 end) as non_hub_dr_cost_bw_fee_usd
    ,sum(case when delivered_by != 'hub' then dr_cost_late_night_usd else 0 end) as non_hub_dr_cost_late_night_usd
    ,sum(case when delivered_by != 'hub' then dr_cost_holiday_fee else 0 end) as non_hub_dr_cost_holiday_fee

    -- CPO all
    ,(sum(dr_cost_base_usd) + sum(dr_cost_surge_usd) + sum(dr_cost_bonus_usd))/count(distinct order_id) as driver_cpo_base_surge_bonus_all
    ,(sum(dr_cost_bw_fee_usd) + sum(dr_cost_late_night_usd) + sum(dr_cost_holiday_fee))/count(distinct order_id) as driver_cpo_etc_all


    -- CPO HUB
    ,(sum(case when delivered_by = 'hub' then dr_cost_base_usd else 0 end)
    +sum(case when delivered_by = 'hub' then dr_cost_surge_usd else 0 end) 
    +sum(case when delivered_by = 'hub' then dr_cost_bonus_usd else 0 end) 
    ) / count(case when delivered_by = 'hub' then order_id else null end) as driver_cpo_base_surge_bonus_hub

    -- CPO Non-HUB
    ,(sum(case when delivered_by != 'hub' then dr_cost_base_usd else 0 end)
    +sum(case when delivered_by != 'hub' then dr_cost_surge_usd else 0 end) 
    +sum(case when delivered_by != 'hub' then dr_cost_bonus_usd else 0 end)
    ) / count(case when delivered_by != 'hub' then order_id else null end) as driver_cpo_base_surge_bonus_non_hub

from driver_cost_base
where 1=1 
and (
    grass_date between date_trunc('month',current_date - interval '1' day) - interval '1' month  and current_date - interval '1' day 
    or grass_date in (date '2022-04-04',date '2022-05-05')
    )
and source in ('Food')
group by 1,2,3,4

union all 

select 

    city_name_full
    ,date_
    ,24 as delivered_hour
    ,source
    -- orders
    ,count(distinct order_id) as total_orders
    ,count(case when delivered_by = 'hub' then order_id else null end) as total_order_hub
    ,count(case when delivered_by != 'hub' then order_id else null end) as total_order_non_hub

    -- cost overall
    ,sum(dr_cost_base_usd) as total_dr_cost_base_usd
    ,sum(dr_cost_surge_usd) as total_dr_cost_surge_usd
    ,sum(dr_cost_bonus_usd) as total_dr_cost_bonus_usd
    ,sum(dr_cost_bw_fee_usd) as total_dr_cost_bw_fee_usd
    ,sum(dr_cost_late_night_usd) as total_dr_cost_late_night_usd
    ,sum(dr_cost_holiday_fee) as total_dr_cost_holiday_fee

    -- hub
    ,sum(case when delivered_by = 'hub' then dr_cost_base_usd else 0 end) hub_dr_cost_base_usd
    ,sum(case when delivered_by = 'hub' then dr_cost_surge_usd else 0 end) hub_dr_cost_surge_usd
    ,sum(case when delivered_by = 'hub' then dr_cost_bonus_usd else 0 end) as hub_dr_cost_bonus_usd
    ,sum(case when delivered_by = 'hub' then dr_cost_bw_fee_usd else 0 end) as hub_dr_cost_bw_fee_usd
    ,sum(case when delivered_by = 'hub' then dr_cost_late_night_usd else 0 end) as hub_dr_cost_late_night_usd
    ,sum(case when delivered_by = 'hub' then dr_cost_holiday_fee else 0 end) as hub_dr_cost_holiday_fee

    -- non_hub
    ,sum(case when delivered_by != 'hub' then dr_cost_surge_usd else 0 end) non_hub_dr_cost_surge_usd
    ,sum(case when delivered_by != 'hub' then dr_cost_base_usd else 0 end) non_hub_dr_cost_base_usd
    ,sum(case when delivered_by != 'hub' then dr_cost_bonus_usd else 0 end) as non_dr_cost_bonus_usd
    ,sum(case when delivered_by != 'hub' then dr_cost_bw_fee_usd else 0 end) as non_hub_dr_cost_bw_fee_usd
    ,sum(case when delivered_by != 'hub' then dr_cost_late_night_usd else 0 end) as non_hub_dr_cost_late_night_usd
    ,sum(case when delivered_by != 'hub' then dr_cost_holiday_fee else 0 end) as non_hub_dr_cost_holiday_fee

    -- CPO all
    ,(sum(dr_cost_base_usd) + sum(dr_cost_surge_usd) + sum(dr_cost_bonus_usd))/count(distinct order_id) as driver_cpo_base_surge_bonus_all
    ,(sum(dr_cost_bw_fee_usd) + sum(dr_cost_late_night_usd) + sum(dr_cost_holiday_fee))/count(distinct order_id) as driver_cpo_etc_all


    -- CPO HUB
    ,(sum(case when delivered_by = 'hub' then dr_cost_base_usd else 0 end)
    +sum(case when delivered_by = 'hub' then dr_cost_surge_usd else 0 end) 
    +sum(case when delivered_by = 'hub' then dr_cost_bonus_usd else 0 end) 
    ) / count(case when delivered_by = 'hub' then order_id else null end) as driver_cpo_base_surge_bonus_hub

    -- CPO Non-HUB
    ,(sum(case when delivered_by != 'hub' then dr_cost_base_usd else 0 end)
    +sum(case when delivered_by != 'hub' then dr_cost_surge_usd else 0 end) 
    +sum(case when delivered_by != 'hub' then dr_cost_bonus_usd else 0 end)
    ) / count(case when delivered_by != 'hub' then order_id else null end) as driver_cpo_base_surge_bonus_non_hub

from driver_cost_base
where 1=1 
and (
grass_date between date_trunc('month',current_date - interval '1' day) - interval '1' month  and current_date - interval '1' day 
or grass_date in (date '2022-04-04',date '2022-05-05')
)
and source in ('Food')
group by 1,2,3,4

)
select 
    cpo.date_
    ,cpo.delivered_hour
    ,cpo.city_name_full
    ,t.delivered_by
    ,t.metrics
    ,concat(cpo.city_name_full,'_',cast(cpo.delivered_hour as varchar),'_',t.delivered_by) as key_map
    ,t.cpo_base_surge_bonus

from cpo_base cpo
cross join unnest 
(array['all','hub','non_hub']
,array['cpo_base_surge_bonus','cpo_base_surge_bonus','cpo_base_surge_bonus']
,array[driver_cpo_base_surge_bonus_all,driver_cpo_base_surge_bonus_hub,driver_cpo_base_surge_bonus_non_hub]) t(delivered_by,metrics,cpo_base_surge_bonus)
