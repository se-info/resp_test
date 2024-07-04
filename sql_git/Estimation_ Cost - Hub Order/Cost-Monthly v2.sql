with raw_date as 
(SELECT

      DATE(report_date) AS report_date
FROM
    (
(
SELECT sequence(date'2022-05-01',current_date - interval '1' day) bar)

CROSS JOIN
    unnest (bar) as t(report_date)
)
)
,params_date(period_group,period,start_date,end_date,days) as 
(
-- SELECT 
--         '1. Daily'
--         ,CAST(report_date as varchar)
--         ,CAST(report_date as varchar)
--         ,CAST(report_date as varchar)
--         ,CAST(1 as double)

-- from raw_date
-- group by 1,2,3,4,5

-- UNION ALL 
SELECT 
        '2. Weekly'
        ,'W- ' || CAST(YEAR(date_trunc('week',report_date))*100 + WEEK(date_trunc('week',report_date)) as varchar)
        ,CAST(date_trunc('week',report_date) as varchar)
        ,CAST((date_trunc('week',report_date) + interval '7' day - interval '1' day) as varchar)
        ,CAST(date_diff('day',date_trunc('week',report_date),((date_trunc('week',report_date) + interval '7' day))) as double)

from raw_date

UNION 

SELECT 
        '3. Monthly'
        ,'M- ' || CAST(YEAR(date_trunc('month',report_date))*100 + MONTH(date_trunc('month',report_date)) as varchar)
        ,CAST(date_trunc('month',report_date) as varchar)
        ,CAST((date_trunc('month',report_date) + interval '1' month - interval '1' day) as varchar) 
        ,CAST(date_diff('day',date_trunc('month',report_date),(date_trunc('month',report_date) + interval '1' month)) as double)

from raw_date
) 
,driver_cost_base as 
(select 
    bf.*
    ,case when mgo.app_type_id in (50,51) then 'Shopee'
          when mgo.app_type_id in (1,2,3,4,10,11,20,21,26,27,28) then 'SPF' --Foody
          else 'SPF' end as app_ver        
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
left join shopeefood.foody_mart__fact_gross_order_join_detail mgo on bf.order_id = mgo.id and bf.source = 'Food'
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
    ,app_ver
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
-- and grass_date between current_date - interval '7' day and current_date - interval '1' day
and source in ('Food')
group by 1,2,3,4,5

union all 

select 

    'All' as city_name_full
    ,date_
    ,24 as delivered_hour
    ,source
    ,app_ver
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
-- and grass_date between current_date - interval '7' day and current_date - interval '1' day

and source in ('Food')
group by 1,2,3,4,5

)
,final_metrics as 
(select 
     cpo.date_
    ,cpo.app_ver
    ,cpo.city_name_full
    ,t.delivered_by
    ,concat(cpo.city_name_full,'_',cast(cpo.delivered_hour as varchar),'_',t.delivered_by) as key_map
    ,t.cpo_
    ,t.orders

from cpo_base cpo

cross join unnest 
(array['all','hub','non_hub']
-- ,array['cpo_base_surge_bonus','cpo_base_surge_bonus','cpo_base_surge_bonus','total_order','total_order_hub','total_order_non_hub']
,array[driver_cpo_base_surge_bonus_all,driver_cpo_base_surge_bonus_hub,driver_cpo_base_surge_bonus_non_hub]
,array[total_orders,total_order_hub,total_order_non_hub]
) t(delivered_by,cpo_,orders)

where delivered_hour = 24
and city_name_full = 'All'
)

select 
       p.period_group
      ,p.period
      ,p.start_date
      ,p.end_date
      ,a.city_name_full
      ,a.key_map
      ,sum(a.cpo_*orders) as cpo_  
      ,sum(a.orders) as orders
        
from final_metrics a 

inner join params_date p on a.date_ between cast(p.start_date as date) and cast(p.end_date as date)



group by 1,2,3,4,5,6
