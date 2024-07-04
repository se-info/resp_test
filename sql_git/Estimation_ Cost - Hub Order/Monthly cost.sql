    with param_date(month_,start_date,end_date,days) as 
    (
    VALUES
    ('Jan',date'2022-01-01',date'2022-01-31',date_diff('day',date'2022-01-01',date'2022-01-31'))
    ,('Feb',date'2022-02-01',date'2022-02-28',date_diff('day',date'2022-02-01',date'2022-02-28'))
    ,('Mar',date'2022-03-01',date'2022-03-31',date_diff('day',date'2022-03-01',date'2022-03-31'))
    ,('Apr',date'2022-04-01',date'2022-04-30',date_diff('day',date'2022-04-01',date'2022-04-30'))
    ,('May',date'2022-05-01',date'2022-05-31',date_diff('day',date'2022-05-01',date'2022-05-31'))
    ,('Jun',date'2022-06-01',date'2022-06-30',date_diff('day',date'2022-06-01',date'2022-06-30'))
    ,('Jul',date'2022-07-01',date'2022-07-31',date_diff('day',date'2022-07-01',date'2022-07-31'))
    ,('Aug',date'2022-08-01',date'2022-08-31',date_diff('day',date'2022-08-01',date'2022-08-31'))
    ,('Sep',date'2022-09-01',date'2022-09-30',date_diff('day',date'2022-09-01',date'2022-09-30'))
    )
    -- select * from param_date
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

    from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf
    left join shopeefood.foody_mart__fact_gross_order_join_detail mgo on bf.order_id = mgo.id and bf.source = 'Food'
    left join shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
        on bf.order_id = oct.id
    )
    select 

        'All' as city_name
        ,b.month_
        ,24 as delivered_hour
        -- ,source
        -- ,app_ver
        -- orders
        ,count(distinct date_) as b_days 
        ,count(distinct order_id) as total_orders
        ,count(case when delivered_by = 'hub' then order_id else null end) as total_order_hub
        ,count(case when delivered_by != 'hub' then order_id else null end) as total_order_non_hub
        ,count(distinct case when is_stack_group_order = 2 then order_id else null end) as total_stack

        -- -- cost overall
        -- ,sum(dr_cost_base_usd) as total_dr_cost_base_usd
        -- ,sum(dr_cost_surge_usd) as total_dr_cost_surge_usd
        -- ,sum(dr_cost_bonus_usd) as total_dr_cost_bonus_usd
        -- ,sum(dr_cost_bw_fee_usd) as total_dr_cost_bw_fee_usd
        -- ,sum(dr_cost_late_night_usd) as total_dr_cost_late_night_usd
        -- ,sum(dr_cost_holiday_fee) as total_dr_cost_holiday_fee

        -- -- hub
        -- ,sum(case when delivered_by = 'hub' then dr_cost_base_usd else 0 end) hub_dr_cost_base_usd
        -- ,sum(case when delivered_by = 'hub' then dr_cost_surge_usd else 0 end) hub_dr_cost_surge_usd
        -- ,sum(case when delivered_by = 'hub' then dr_cost_bonus_usd else 0 end) as hub_dr_cost_bonus_usd
        -- ,sum(case when delivered_by = 'hub' then dr_cost_bw_fee_usd else 0 end) as hub_dr_cost_bw_fee_usd
        -- ,sum(case when delivered_by = 'hub' then dr_cost_late_night_usd else 0 end) as hub_dr_cost_late_night_usd
        -- ,sum(case when delivered_by = 'hub' then dr_cost_holiday_fee else 0 end) as hub_dr_cost_holiday_fee

        -- -- non_hub
        -- ,sum(case when delivered_by != 'hub' then dr_cost_surge_usd else 0 end) non_hub_dr_cost_surge_usd
        -- ,sum(case when delivered_by != 'hub' then dr_cost_base_usd else 0 end) non_hub_dr_cost_base_usd
        -- ,sum(case when delivered_by != 'hub' then dr_cost_bonus_usd else 0 end) as non_dr_cost_bonus_usd
        -- ,sum(case when delivered_by != 'hub' then dr_cost_bw_fee_usd else 0 end) as non_hub_dr_cost_bw_fee_usd
        -- ,sum(case when delivered_by != 'hub' then dr_cost_late_night_usd else 0 end) as non_hub_dr_cost_late_night_usd
        -- ,sum(case when delivered_by != 'hub' then dr_cost_holiday_fee else 0 end) as non_hub_dr_cost_holiday_fee

        -- CPO all
        ,try((sum(dr_cost_base_usd) + sum(dr_cost_surge_usd))/count(distinct order_id)) as base_surge_all
        ,try((sum(dr_cost_bonus_usd))/count(distinct order_id)) as bonus_all
        ,try((sum(dr_cost_base_usd) + sum(dr_cost_surge_usd) + sum(dr_cost_bonus_usd))/count(distinct order_id)) as driver_cpo_base_surge_bonus_all
        -- ,(sum(dr_cost_bw_fee_usd) + sum(dr_cost_late_night_usd) + sum(dr_cost_holiday_fee))/count(distinct order_id) as driver_cpo_etc_all


        -- CPO HUB
        ,try((sum(case when delivered_by = 'hub' then dr_cost_base_usd else 0 end)
        +sum(case when delivered_by = 'hub' then dr_cost_surge_usd else 0 end))/count(case when delivered_by = 'hub' then order_id else null end)) as base_surge_hub
        ,try(sum(case when delivered_by = 'hub' then dr_cost_bonus_usd else 0 end) 
        /count(case when delivered_by = 'hub' then order_id else null end)) as bonus_hub    
        ,try((sum(case when delivered_by = 'hub' then dr_cost_base_usd else 0 end)
        +sum(case when delivered_by = 'hub' then dr_cost_surge_usd else 0 end) 
        +sum(case when delivered_by = 'hub' then dr_cost_bonus_usd else 0 end) 
        ) / count(case when delivered_by = 'hub' then order_id else null end)) as driver_cpo_base_surge_bonus_hub

        -- CPO Non-HUB
        ,try((sum(case when delivered_by != 'hub' then dr_cost_base_usd else 0 end)
        +sum(case when delivered_by != 'hub' then dr_cost_surge_usd else 0 end))/count(case when delivered_by != 'hub' then order_id else null end)) as base_surge_non_hub
        ,try(sum(case when delivered_by != 'hub' then dr_cost_bonus_usd else 0 end) 
        /count(case when delivered_by != 'hub' then order_id else null end)) as bonus_non_hub       
        ,try((sum(case when delivered_by != 'hub' then dr_cost_base_usd else 0 end)
        +sum(case when delivered_by != 'hub' then dr_cost_surge_usd else 0 end) 
        +sum(case when delivered_by != 'hub' then dr_cost_bonus_usd else 0 end)
        ) / count(case when delivered_by != 'hub' then order_id else null end)) as driver_cpo_base_surge_bonus_non_hub

    from driver_cost_base a 

    inner join param_date b on a.date_ between b.start_date and b.end_date



    where 1=1 
    and grass_date >= date'2022-01-01'
    and source in ('Food')
    group by 1,2,3
