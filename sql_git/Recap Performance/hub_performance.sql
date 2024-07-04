with raw_date as 
(SELECT

      DATE(report_date) AS report_date
FROM
    (
(
SELECT sequence(date'2021-01-01',current_date - interval '1' day) bar)

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
    -- ,case when mgo.app_type_id in (50,51) then 'Shopee'
    --       when mgo.app_type_id in (1,2,3,4,10,11,20,21,26,27,28) then 'SPF' --Foody
    --       else 'SPF' end as app_ver        
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

from vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf
-- left join shopeefood.foody_mart__fact_gross_order_join_detail mgo on bf.order_id = mgo.id and bf.source = 'Food'
left join shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
    on bf.order_id = oct.id
)
,cpo_base as
(
select 

     case when city_name_full in ('HCM City','Ha Noi City') then city_name_full
          else 'others' end as city_name_full  
    ,date_ 
    -- ,24 as delivered_hour
    ,source
    ,case when new_driver_tier = 'HUB_OTH' then 'others'
          when new_driver_tier = 'part_time' then 'others'
          when new_driver_tier in ('HUB10','HUB08','HUB05','HUB03') then new_driver_tier
          else new_driver_tier end as tier
    -- ,app_ver
    -- orders
    ,count(distinct order_id)/cast(count(distinct date_) as double) as total_orders
    ,count(case when delivered_by = 'hub' then order_id else null end)/cast(count(distinct date_) as double) as total_order_hub
    ,count(case when delivered_by != 'hub' then order_id else null end)/cast(count(distinct date_) as double) as total_order_non_hub
    ,count(distinct case when delivered_by = 'hub' then partner_id else null end)/cast(count(distinct date_) as double) as total_a1 


from driver_cost_base



where 1=1 
-- and grass_date between current_date - interval '1' day and current_date - interval '1' day
-- and p.period_group = '3. Monthly'
and source in ('Food')
-- and current_driver_tier = 'Hub'
group by 1,2,3,4

)
select 
         cpo_base.source
        ,cpo_base.city_name_full 
        ,cpo_base.tier
        ,p.period
        ,sum(total_order_hub)/cast(count(distinct date_) as double) as total_hub_od 
        ,sum(total_orders)/cast(count(distinct date_) as double) as total_od 
        ,sum(total_a1)/cast(count(distinct date_) as double) as a1 


from cpo_base

inner join params_date p on cpo_base.date_ between cast(p.start_date as date) and cast(p.end_date as date)


where p.period_group = '3. Monthly'

group by 1,2,3,4
