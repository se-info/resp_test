select *
      ,total_earning_before_tax - total_earning_hub as total_earning_non_hub

FROM
(
Select *
      ,shipping_fee_share + return_fee_share + additional_bonus + order_completed_bonus + other_payables + weekly_bonus + daily_bonus + hub_cost_auto + hub_weekly_bonus + total_bad_weather_cost +  total_late_night_cost + total_holiday_cost as total_earning_before_tax
      ,case when coalesce(current_driver_tier, 'others') = 'Hub' then  hub_cost_auto + hub_weekly_bonus + total_bad_weather_cost_hub + total_late_night_cost_hub + total_holiday_cost_hub else 0 end as total_earning_hub
   --   ,case when coalesce(current_driver_tier, 'others') != 'Hub' then shipping_fee_share + return_fee_share + additional_bonus + order_completed_bonus + other_payables + weekly_bonus + daily_bonus + total_bad_weather_cost_non_hub + total_late_night_cost_non_hub + total_holiday_cost_non_hub else 0 end total_earning_non_hub

from
(SELECT bill_fee_2.partner_id
,bill_fee_2.date_
,bill_fee_2.year_week
,bill_fee_2.city_name
,bill_fee_2.city_name_full
,bill_fee_2.shipper_type
,bill_fee_2.is_new_policy
,bill_fee_2.current_driver_tier
,bill_fee_2.new_driver_tier
,coalesce(bill_fee_2.total_bill,0) as total_bill
,coalesce(bill_fee_2.total_bill_hub,0) as total_bill_hub
,coalesce(bill_fee_2.total_bill,0) - coalesce(bill_fee_2.total_bill_hub,0) as total_bill_non_hub
,coalesce(bill_fee_2.total_shipping_fee_collected_from_customer,0) as total_shipping_fee_collected_from_customer
,coalesce(bill_fee_2.shipping_fee_share,0) as shipping_fee_share
,coalesce(bill_fee_2.return_fee_share,0) as return_fee_share
,coalesce(bill_fee_2.additional_bonus,0) as additional_bonus
,coalesce(bill_fee_2.order_completed_bonus,0) as order_completed_bonus
,coalesce(bill_fee_2.other_payables,0) as other_payables
,coalesce(bill_fee_2.shipping_fee_share_basic,0) as shipping_fee_share_basic
,coalesce(bill_fee_2.shipping_fee_share_surge,0) as shipping_fee_share_surge
,coalesce(bill_fee_2.return_fee_share_basic,0) as return_fee_share_basic
,coalesce(bill_fee_2.return_fee_share_surge,0) as return_fee_share_surge
,coalesce(bill_fee_2.total_distance,0) as total_distance
,coalesce(bonus_week.weekly_bonus,0)*1.0000/7 + coalesce(bonus_week_extra.weekly_bonus,0)*1.0000/(case when bonus_week_extra.start_date = date('2021-09-16') then 6.186332131 
                                                                                                       when bonus_week_extra.start_date = date('2021-09-24') then 6.431135596 else 6.33247502 end) as weekly_bonus
,case
    when bill_fee_2.date_ <= date('2021-10-24') then coalesce(hub_weekly.total_weekly_bonus_hub,0)*1.0000/5.8 + coalesce(hub_weekly_2.total_weekly_bonus_hub,0)*1.0000/6.2214
    when bill_fee_2.date_ between date('2022-01-29') and date('2022-02-06') then coalesce(hub_weekly.total_weekly_bonus_hub,0)*1.0000/8.636275773 + coalesce(hub_weekly_2.total_weekly_bonus_hub,0)*1.0000/8.636275773
    else coalesce(hub_weekly.total_weekly_bonus_hub,0)*1.0000/6.2214 + coalesce(hub_weekly_2.total_weekly_bonus_hub,0)*1.0000/6.2214
    end as hub_weekly_bonus

,(coalesce(bonus_day.daily_bonus,0) +  coalesce(bonus_day_adjust.daily_bonus,0) +  coalesce(bonus_day_extra.daily_bonus,0) +  coalesce(bonus_day_cp.daily_bonus,0)) as daily_bonus
,coalesce(bonus_day.daily_bonus,0) normal_daily_bonus
,coalesce(bonus_day_adjust.daily_bonus,0) hub_extra_10k_bonus
,coalesce(bonus_day_extra.daily_bonus,0) extra_daily_bonus_scheme
,coalesce(bonus_week_extra.weekly_bonus,0)*1.0000/6.186332131 as weekly_hcm_bonus_extra_scheme
,coalesce(hub_auto.hub_cost_auto,0) + coalesce(hub_adj.hub_cost_bonus_adjustment,0) as hub_cost_auto
,coalesce(hub_auto.hub_cost_auto_shipping_fee,0) as hub_cost_auto_shipping_fee
,coalesce(hub_auto.hub_cost_auto_daily_bonus,0) + coalesce(hub_adj.hub_cost_bonus_adjustment,0) as hub_cost_auto_daily_bonus

,coalesce(bill_fee_2.bad_weather_fee_temp,0) as bad_weather_fee_temp
,coalesce(bill_fee_2.total_bad_weather_cost,0) as total_bad_weather_cost
,coalesce(bill_fee_2.total_bad_weather_cost_hub,0) as total_bad_weather_cost_hub
,coalesce(bill_fee_2.total_bad_weather_cost_non_hub,0) as total_bad_weather_cost_non_hub

,coalesce(bill_fee_2.total_late_night_cost,0) as total_late_night_cost
,coalesce(bill_fee_2.total_late_night_cost_hub,0) as total_late_night_cost_hub
,coalesce(bill_fee_2.total_late_night_cost_non_hub,0) as total_late_night_cost_non_hub

,coalesce(bill_fee_2.total_holiday_cost,0) as total_holiday_cost
,coalesce(bill_fee_2.total_holiday_cost_hub,0) as total_holiday_cost_hub
,coalesce(bill_fee_2.total_holiday_cost_non_hub,0) as total_holiday_cost_non_hub

,coalesce(bill_fee_2.user_bwf,0) as user_bwf

-- rev calculation
,coalesce(bill_fee_2.rev_shipping_fee,0) as rev_shipping_fee
,coalesce(bill_fee_2.prm_cost,0) as prm_cost
,coalesce(bill_fee_2.rev_cod_fee,0) as rev_cod_fee
,coalesce(bill_fee_2.rev_return_fee,0) as rev_return_fee


-- adjustment
,coalesce(adj.adjustment,0) as adjustment
,coalesce(adj.adjustment_basic,0) as adjustment_basic
,coalesce(adj.adjustment_surge,0) as adjustment_surge

,coalesce(adj.adjustment_food,0) as adjustment_food
,coalesce(adj.adjustment_food_basic,0) as adjustment_food_basic
,coalesce(adj.adjustment_food_surge,0) as adjustment_food_surge

,coalesce(adj.adjustment_market,0) as adjustment_market
,coalesce(adj.adjustment_market_basic,0) as adjustment_market_basic
,coalesce(adj.adjustment_market_surge,0) as adjustment_market_surge


--- total bill by service
,coalesce(bill_fee_2.total_bill_food,0) as total_bill_food
,coalesce(bill_fee_2.total_bill_market,0) as total_bill_market
,coalesce(bill_fee_2.total_bill_now_ship,0) as total_bill_now_ship
,coalesce(bill_fee_2.total_bill_now_ship_shopee,0) as total_bill_now_ship_shopee
,coalesce(bill_fee_2.total_bill_now_ship_instant,0) as total_bill_now_ship_instant
,coalesce(bill_fee_2.total_bill_now_ship_food_merchant,0) as total_bill_now_ship_food_merchant
,coalesce(bill_fee_2.total_bill_now_ship_sameday,0) as total_bill_now_ship_sameday

--- total bill hub by service
,coalesce(bill_fee_2.total_bill_hub_food,0) as total_bill_hub_food
,coalesce(bill_fee_2.total_bill_hub_market,0) as total_bill_hub_market
,coalesce(bill_fee_2.total_bill_hub_now_ship,0) as total_bill_hub_now_ship
,coalesce(bill_fee_2.total_bill_hub_now_ship_shopee,0) as total_bill_hub_now_ship_shopee
,coalesce(bill_fee_2.total_bill_hub_now_ship_instant,0) as total_bill_hub_now_ship_instant
,coalesce(bill_fee_2.total_bill_hub_now_ship_food_merchant,0) as total_bill_hub_now_ship_food_merchant
,coalesce(bill_fee_2.total_bill_hub_now_ship_sameday,0) as total_bill_hub_now_ship_sameday

-- bad weather fee
,coalesce(bill_fee_2.total_bad_weather_cost_food,0) as total_bad_weather_cost_food
,coalesce(bill_fee_2.total_bad_weather_cost_hub_food,0) as total_bad_weather_cost_hub_food
,coalesce(bill_fee_2.total_bad_weather_cost_non_hub_food,0) as total_bad_weather_cost_non_hub_food

,coalesce(bill_fee_2.total_bad_weather_cost_market,0) as total_bad_weather_cost_market
,coalesce(bill_fee_2.total_bad_weather_cost_hub_market,0) as total_bad_weather_cost_hub_market
,coalesce(bill_fee_2.total_bad_weather_cost_non_hub_market,0) as total_bad_weather_cost_non_hub_market


from
(SELECT bill_fee.partner_id
,bill_fee.date_
,bill_fee.year_week
,bill_fee.city_name
,bill_fee.city_name_full
,bill_fee.shipper_type
,bill_fee.is_new_policy
,bill_fee.current_driver_tier
,bill_fee.new_driver_tier
,SUM(coalesce(bill_fee.total_bill,0)) as total_bill
,SUM(coalesce(bill_fee.total_bill_hub,0)) as total_bill_hub
,SUM(coalesce(bill_fee.total_shipping_fee_collected_from_customer,0)) as total_shipping_fee_collected_from_customer
,SUM(coalesce(bill_fee.shipping_fee_share,0)) as shipping_fee_share
,SUM(coalesce(bill_fee.return_fee_share,0)) as return_fee_share
,SUM(coalesce(bill_fee.additional_bonus,0)) as additional_bonus
,SUM(coalesce(bill_fee.order_completed_bonus,0)) as order_completed_bonus
,SUM(coalesce(bill_fee.other_payables,0)) as other_payables
,SUM(coalesce(bill_fee.shipping_fee_share_basic,0)) as shipping_fee_share_basic
,SUM(coalesce(bill_fee.shipping_fee_share_surge,0)) as shipping_fee_share_surge
,SUM(coalesce(bill_fee.return_fee_share_basic,0)) as return_fee_share_basic
,SUM(coalesce(bill_fee.return_fee_share_surge,0)) as return_fee_share_surge
,SUM(coalesce(bill_fee.distance,0)) as total_distance
,SUM(coalesce(bill_fee.bad_weather_fee_temp,0)) as bad_weather_fee_temp
,SUM(coalesce(bill_fee.total_bad_weather_cost,0)) as total_bad_weather_cost
,SUM(coalesce(bill_fee.total_bad_weather_cost_hub,0)) as total_bad_weather_cost_hub
,SUM(coalesce(bill_fee.total_bad_weather_cost_non_hub,0)) as total_bad_weather_cost_non_hub
,SUM(coalesce(bill_fee.total_late_night_cost,0)) as total_late_night_cost
,SUM(coalesce(bill_fee.total_late_night_fee_temp_hub,0)) as total_late_night_cost_hub
,SUM(coalesce(bill_fee.total_late_night_fee_temp_non_hub,0)) as total_late_night_cost_non_hub
,SUM(coalesce(bill_fee.total_holiday_fee_cost,0)) as total_holiday_cost
,SUM(coalesce(bill_fee.total_holiday_fee_temp_hub,0)) as total_holiday_cost_hub
,SUM(coalesce(bill_fee.total_holiday_fee_temp_non_hub,0)) as total_holiday_cost_non_hub


,sum(coalesce(bill_fee.user_bwf,0)) as user_bwf

-- rev calculation
,sum(bill_fee.rev_shipping_fee) as rev_shipping_fee
,sum(bill_fee.prm_cost) as prm_cost
,sum(bill_fee.rev_cod_fee) as rev_cod_fee
,sum(bill_fee.rev_return_fee) as rev_return_fee

-- service
--- total bill
,SUM(case when bill_fee.source = 'Food' then coalesce(bill_fee.total_bill,0) else 0 end) as total_bill_food
,SUM(case when bill_fee.source = 'Market' then coalesce(bill_fee.total_bill,0) else 0 end) as total_bill_market
,SUM(case when bill_fee.source = 'Now Ship' then coalesce(bill_fee.total_bill,0) else 0 end) as total_bill_now_ship
,SUM(case when bill_fee.source = 'Now Ship Shopee' then coalesce(bill_fee.total_bill,0) else 0 end) as total_bill_now_ship_shopee
,SUM(case when bill_fee.sub_source = 'NS Instant' then coalesce(bill_fee.total_bill,0) else 0 end) as total_bill_now_ship_instant
,SUM(case when bill_fee.sub_source = 'NS Food Merchant' then coalesce(bill_fee.total_bill,0) else 0 end) as total_bill_now_ship_food_merchant
,SUM(case when bill_fee.sub_source = 'NS Sameday' then coalesce(bill_fee.total_bill,0) else 0 end) as total_bill_now_ship_sameday

-- total bill hub
,SUM(case when bill_fee.source = 'Food' then coalesce(bill_fee.total_bill_hub,0) else 0 end) as total_bill_hub_food
,SUM(case when bill_fee.source = 'Market' then coalesce(bill_fee.total_bill_hub,0) else 0 end) as total_bill_hub_market
,SUM(case when bill_fee.source = 'Now Ship' then coalesce(bill_fee.total_bill_hub,0) else 0 end) as total_bill_hub_now_ship
,SUM(case when bill_fee.source = 'Now Ship Shopee' then coalesce(bill_fee.total_bill_hub,0) else 0 end) as total_bill_hub_now_ship_shopee
,SUM(case when bill_fee.sub_source = 'NS Instant' then coalesce(bill_fee.total_bill_hub,0) else 0 end) as total_bill_hub_now_ship_instant
,SUM(case when bill_fee.sub_source = 'NS Food Merchant' then coalesce(bill_fee.total_bill_hub,0) else 0 end) as total_bill_hub_now_ship_food_merchant
,SUM(case when bill_fee.sub_source = 'NS Sameday' then coalesce(bill_fee.total_bill_hub,0) else 0 end) as total_bill_hub_now_ship_sameday

-- bad weather fee
,SUM(case when bill_fee.source = 'Food' then coalesce(bill_fee.total_bad_weather_cost,0) else 0 end) as total_bad_weather_cost_food
,SUM(case when bill_fee.source = 'Food' then coalesce(bill_fee.total_bad_weather_cost_hub,0) else 0 end) as total_bad_weather_cost_hub_food
,SUM(case when bill_fee.source = 'Food' then coalesce(bill_fee.total_bad_weather_cost_non_hub,0) else 0 end) as total_bad_weather_cost_non_hub_food

,SUM(case when bill_fee.source = 'Market' then coalesce(bill_fee.total_bad_weather_cost,0) else 0 end) as total_bad_weather_cost_market
,SUM(case when bill_fee.source = 'Market' then coalesce(bill_fee.total_bad_weather_cost_hub,0) else 0 end) as total_bad_weather_cost_hub_market
,SUM(case when bill_fee.source = 'Market' then coalesce(bill_fee.total_bad_weather_cost_non_hub,0) else 0 end) as total_bad_weather_cost_non_hub_market


from
(SELECT raw2.order_id
    ,raw2.partner_id
    ,raw2.date_
    ,raw2.year_week
    ,raw2.city_name
    ,raw2.city_name_full
    ,raw2.shipper_type_id
    ,raw2.is_new_policy
    ,raw2.shipper_type
    ,raw2.source
    ,raw2.sub_source
    ,raw2.distance
    ,raw2.status
    ,raw2.total_shipping_fee
    ,raw2.total_shipping_fee_basic
    ,raw2.total_shipping_fee_surge
    ,raw2.bad_weather_cost_driver_new
    ,raw2.bad_weather_cost_driver_new_hub
    ,raw2.bad_weather_cost_driver_new_non_hub
    ,raw2.user_bwf
    ,raw2.total_return_fee
    ,raw2.total_return_fee_basic
    ,raw2.total_return_fee_surge
    ,raw2.current_driver_tier
    ,raw2.new_driver_tier
    ,coalesce(raw2.total_bill,0) as total_bill
    ,coalesce(raw2.total_bill_hub,0) as total_bill_hub
    ,coalesce(raw2.total_shipping_fee_collected_from_customer,0) as total_shipping_fee_collected_from_customer
    ,coalesce(raw2.shipping_fee_share,0) as shipping_fee_share -- add bad weather fee
    ,coalesce(raw2.return_fee_share,0) as return_fee_share
    ,coalesce(raw2.additional_bonus,0) as additional_bonus
    ,coalesce(raw2.order_completed_bonus,0) as order_completed_bonus
    ,coalesce(raw2.other_payables,0) as other_payables
    ,case when raw2.total_shipping_fee = 0 then 0
        else (coalesce(raw2.shipping_fee_share,0)*1.000000/raw2.total_shipping_fee)*raw2.total_shipping_fee_basic end as shipping_fee_share_basic
    ,case when raw2.total_shipping_fee = 0 then 0
        else (coalesce(raw2.shipping_fee_share,0)*1.000000/raw2.total_shipping_fee) *raw2.total_shipping_fee_surge  end as shipping_fee_share_surge  -- add bad weather fee/late night/ holiday fee into surge
    ,case when raw2.total_return_fee = 0 then 0
        else (coalesce(raw2.return_fee_share,0)*1.000000/raw2.total_return_fee)*raw2.total_return_fee_basic end as return_fee_share_basic
    ,case when raw2.total_return_fee = 0 then 0
        else (coalesce(raw2.return_fee_share,0)*1.000000/raw2.total_return_fee)*raw2.total_return_fee_surge end as return_fee_share_surge
    ,coalesce(raw2.bad_weather_fee_temp,0) as  bad_weather_fee_temp

    ,case when raw2.total_shipping_fee = 0 then 0
        else (coalesce(raw2.shipping_fee_share,0)*1.000000/raw2.total_shipping_fee)*raw2.bad_weather_cost_driver_new end as bad_weather_cost_driver_new_share

    ,coalesce(raw2.bad_weather_fee_temp,0) +
        (case when raw2.total_shipping_fee = 0 then 0
            else (coalesce(raw2.shipping_fee_share,0)*1.000000/raw2.total_shipping_fee)*raw2.bad_weather_cost_driver_new end) as total_bad_weather_cost

    ,coalesce(raw2.bad_weather_fee_temp_hub,0) +
        (case when raw2.total_shipping_fee = 0 then 0
            else (coalesce(raw2.shipping_fee_share,0)*1.000000/raw2.total_shipping_fee)*raw2.bad_weather_cost_driver_new_hub end) as total_bad_weather_cost_hub

    ,coalesce(raw2.bad_weather_fee_temp_non_hub,0) +
        (case when raw2.total_shipping_fee = 0 then 0
            else (coalesce(raw2.shipping_fee_share,0)*1.000000/raw2.total_shipping_fee)*raw2.bad_weather_cost_driver_new_non_hub end) as total_bad_weather_cost_non_hub

    ,coalesce(raw2.late_night_fee_temp_hub,0) as total_late_night_fee_temp_hub
    ,coalesce(raw2.late_night_fee_temp_non_hub,0) as total_late_night_fee_temp_non_hub
    ,coalesce(raw2.late_night_fee_temp,0) as total_late_night_cost

    ,coalesce(raw2.holiday_fee_temp_hub,0) as total_holiday_fee_temp_hub
    ,coalesce(raw2.holiday_fee_temp_non_hub,0) as total_holiday_fee_temp_non_hub
    ,coalesce(raw2.holiday_fee_temp,0) as total_holiday_fee_cost

    -- rev calculation
    ,coalesce(raw2.rev_shipping_fee,0) as rev_shipping_fee
    ,coalesce(raw2.prm_cost,0) as prm_cost
    ,coalesce(raw2.rev_cod_fee,0) as rev_cod_fee
    ,coalesce(raw2.rev_return_fee,0) as rev_return_fee

    from
    (Select raw.order_id
    ,raw.partner_id
    ,raw.date_
    ,raw.year_week
    ,raw.city_name
    ,city.name_en as city_name_full
    ,raw.partner_type as shipper_type_id
    ,case when raw.city_name in ('HCM','HN') then
            case when raw.partner_type = 1 then 0 -- 'full_time'
                when raw.partner_type = 3 then 0 -- 'tester'
            else 1
            end
        else 0 end as is_new_policy

    ,case when raw.city_name in ('HCM','HN') then
            case when raw.partner_type = 1 then 'full_time'
                when raw.partner_type = 3 then 'tester'
                when raw.partner_type = 12 then 'part_time_17'
            else 'driver_new_policy'
            end
        when raw.partner_type = 1 then 'full_time'
        when raw.partner_type = 2 then 'part_time'
        when raw.partner_type = 3 then 'tester'
        when raw.partner_type = 6 then 'part_time_09'
        when raw.partner_type = 7 then 'part_time_11'
        when raw.partner_type = 8 then 'part_time_12'
        when raw.partner_type = 9 then 'part_time_14'
        when raw.partner_type = 10 then 'part_time_15'
        when raw.partner_type = 11 then 'part_time_16'
        when raw.partner_type = 12 then 'part_time_17'
        else 'others' end as shipper_type
    ,raw.total_shipping_fee
    ,raw.total_shipping_fee_basic
    ,raw.total_shipping_fee_surge
    ,raw.bad_weather_cost_driver_new
    ,case when raw.partner_type = 12 and coalesce(raw.driver_payment_policy,0) <> 3 then raw.bad_weather_cost_driver_new else 0 end as bad_weather_cost_driver_new_hub
    ,raw.bad_weather_cost_driver_new - (case when raw.partner_type = 12 and coalesce(raw.driver_payment_policy,0) <> 3 then raw.bad_weather_cost_driver_new else 0 end) as bad_weather_cost_driver_new_non_hub

    ,raw.user_bwf
    ,raw.total_return_fee
    ,raw.total_return_fee_basic
    ,raw.total_return_fee_surge
    ,raw.source
    ,raw.sub_source
    ,raw.distance
    ,raw.status
    ,case when raw.partner_type = 12 then 'Hub' else raw.current_driver_tier end as current_driver_tier
    ,raw.new_driver_tier
    ,raw.rev_shipping_fee
    ,raw.prm_cost
    ,raw.rev_cod_fee
    ,raw.rev_return_fee

    ,count(DISTINCT raw.order_id) as total_bill
    ,count(distinct case when raw.partner_type = 12 and coalesce(raw.driver_payment_policy,0) <> 3 then raw.order_id else null end) as total_bill_hub
    ,sum(raw.total_shipping_fee_collected_from_customer) as total_shipping_fee_collected_from_customer
    ,SUM(case when trx.txn_type in (201,301,401,104,1000,2001,2101) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as shipping_fee_share
    ,SUM(case when trx.txn_type in (202,302,402,1001,2002,2102) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as return_fee_share
    ,SUM(case when trx.txn_type in (204,304,404,105,1003,2004,2104) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as additional_bonus
    ,SUM(case when trx.txn_type in (200,300,400,101,1006,2000,2100) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as order_completed_bonus
    ,SUM(case when trx.txn_type in (203,303,403,106,2003,2005,2006,2007,2105,2106,129,131,133,135,110) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as other_payables
    ,SUM(case when trx.txn_type in (112,115) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as bad_weather_fee_temp
    ,SUM(case when trx.txn_type in (112,115) and (raw.partner_type = 12 and coalesce(raw.driver_payment_policy,0) <> 3) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as bad_weather_fee_temp_hub
    ,SUM(case when trx.txn_type in (112,115) and (raw.partner_type <> 12 OR coalesce(raw.driver_payment_policy,0) = 3) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as bad_weather_fee_temp_non_hub

    ,SUM(case when trx.txn_type in (119) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as late_night_fee_temp
    ,SUM(case when trx.txn_type in (119) and (raw.partner_type = 12 and coalesce(raw.driver_payment_policy,0) <> 3) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as late_night_fee_temp_hub
    ,SUM(case when trx.txn_type in (119) and (raw.partner_type <> 12 OR coalesce(raw.driver_payment_policy,0) = 3) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as late_night_fee_temp_non_hub

    ,SUM(case when trx.txn_type in (117) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as holiday_fee_temp
    ,SUM(case when trx.txn_type in (117) and (raw.partner_type = 12 and coalesce(raw.driver_payment_policy,0) <> 3) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as holiday_fee_temp_hub
    ,SUM(case when trx.txn_type in (117) and (raw.partner_type <> 12 OR coalesce(raw.driver_payment_policy,0) = 3) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as holiday_fee_temp_non_hub


    -- raw data: order data --> total bill
    from    (  SELECT *
                ,case when temp.total_shipping_fee_collected_from_customer is null then 0
                        else 1 end as is_valid_for_calculating_shipping_fee_collected_from_customer

                from
                (SELECT o.order_id
                ,o.partner_id
                ,o.city_name
                ,o.city_id
                ,o.date_
                ,o.year_week
                ,o.partner_type
                ,case when o.source = 'Now Ship Shopee' then o.collect_from_customer
                    else o.total_shipping_fee end as total_shipping_fee_collected_from_customer
                ,o.source
                ,o.sub_source
                ,coalesce(o.distance,0) as distance
                ,o.status
                ,coalesce(o.total_shipping_fee,0) as total_shipping_fee
                ,coalesce(o.total_shipping_fee_basic,0) as total_shipping_fee_basic
                ,coalesce(o.total_shipping_fee_surge,0) as total_shipping_fee_surge
                ,coalesce(o.total_return_fee,0) as total_return_fee
                ,coalesce(o.total_return_fee_basic,0) as total_return_fee_basic
                ,coalesce(o.total_return_fee_surge,0) as total_return_fee_surge
                ,coalesce(o.bad_weather_cost_driver_new,0) as bad_weather_cost_driver_new
                ,coalesce(o.user_bwf,0) as user_bwf
                ,bonus.current_driver_tier
                ,bonus.new_driver_tier
                -- revenue calculation
                ,coalesce(o.rev_shipping_fee,0) as rev_shipping_fee
                ,coalesce(o.prm_cost,0) as prm_cost
                ,coalesce(o.rev_cod_fee,0) as rev_cod_fee
                ,coalesce(o.rev_return_fee,0) as rev_return_fee
                ,o.driver_payment_policy

                from
                        (--EXPLAIN ANALYZE
                        -- Food / Market
                        select  distinct ad_odt.order_id,ad_odt.partner_id
                            ,case when ad_odt.city_id = 217 then 'HCM'
                                  when ad_odt.city_id = 218 then 'HN'
                                  when ad_odt.city_id = 219 then 'DN'
                                  when ad_odt.city_id = 220 then 'HP'
                                  else 'OTH' end as city_name
                            ,ad_odt.city_id
                            ,cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time)  - 3600) as date) as date_
                            ,CASE
                                WHEN WEEK(DATE(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time) - 3600))) >= 52 AND MONTH(DATE(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time) - 3600))) = 1 THEN (YEAR(DATE(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time) - 3600)))-1)*100 + WEEK(DATE(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time) - 3600)))
                                WHEN WEEK(DATE(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time) - 3600))) = 1 AND MONTH(DATE(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time) - 3600))) = 12 THEN (YEAR(DATE(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time) - 3600)))+1)*100 + WEEK(DATE(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time) - 3600)))
                            ELSE YEAR(DATE(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time) - 3600)))*100 + WEEK(DATE(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time) - 3600))) END as year_week
                            ,ad_odt.partner_type
                            ,case when oct.foody_service_id = 1 then 'Food'
                                    else 'Market' end as source
                            ,case when oct.foody_service_id = 1 then 'Food'
                                    else 'Market' end as sub_source
                            ,0 as collect_from_customer
                            ,oct.distance
                            ,oct.status
                            -- ,oct.total_shipping_fee*1.00/100 as total_shipping_fee
                            --,coalesce(cast(json_extract(oct.extra_data,'$.bad_weather_fee.user_pay_amount') as decimal),0) as user_bwf
                            ,oct.user_bwf
                            ,coalesce(dotet.total_shipping_fee,0) as total_shipping_fee

                            ,case when oct.status = 9 then dotet.total_shipping_fee
                                  when cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time)  - 3600) as date) >= date('2021-02-01') then GREATEST(13500,coalesce(dotet.unit_fee,4500)*oct.distance)  -- change setting
                                  else GREATEST(15000,coalesce(dotet.unit_fee,5000)*oct.distance)
                                  end as total_shipping_fee_basic


                            ,case when oct.status = 9 then 0
                                when cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time)  - 3600) as date) >= date('2021-02-01') then GREATEST( dotet.total_shipping_fee - coalesce(GREATEST(13500,coalesce(dotet.unit_fee,4500)*oct.distance),0) ,0)
                                else GREATEST(dotet.total_shipping_fee -  coalesce(GREATEST(15000,coalesce(dotet.unit_fee,5000)*oct.distance),0)   ,0)
                                end as total_shipping_fee_surge


                            ,case when dotet.total_shipping_fee = coalesce(dotet.min_fee,0) + coalesce(dotet.bwf_surge_min_fee,0)
                                    then coalesce(dotet.bwf_surge_min_fee,0)
                                    else coalesce(dotet.unit_fee,0)*oct.distance*coalesce(dotet.bwf_surge_rate,0)
                                    end as bad_weather_cost_driver_new

                            ,0 as total_return_fee
                            ,0 as total_return_fee_basic
                            ,0 as total_return_fee_surge

                            -- revenue calculation
                            ,0 as rev_shipping_fee
                            ,0 as prm_cost
                            ,0 as rev_cod_fee
                            ,0 as rev_return_fee

                            -- hub order
                            ,coalesce(dotet.driver_payment_policy,1) as driver_payment_policy  -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)

                        from (select id,order_id,status_id,	payment_method_id,merchant_payment_method_id,city_id,partner_id,partner_run_type,partner_type,partner_contract_type,partner_service,delivered_date,create_time,	merchant_id,merchant_name,order_code,extra_data
 
                                from shopeefood.foody_accountant_db__order_delivery_tab__reg_daily_s0_live
                                UNION ALL 
                                select id,order_id,status_id,	payment_method_id,merchant_payment_method_id,city_id,partner_id,partner_run_type,partner_type,partner_contract_type,partner_service,delivered_date,create_time,	merchant_id,merchant_name,order_code,extra_data
                                from shopeefood.foody_accountant_archive_db__order_delivery_tab__reg_daily_s0_live
                                ) ad_odt
                        left join (SELECT id,submit_time,foody_service_id,distance,status,total_shipping_fee,extra_data
                                        ,coalesce(cast(json_extract(oct.extra_data,'$.bad_weather_fee.user_pay_amount') as decimal),0) as user_bwf

                                    from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
                                    where submit_time > 1609439493
                                    )oct on oct.id = ad_odt.order_id and oct.submit_time > 1609439493


                            left JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on dot.ref_order_id = oct.id and dot.ref_order_category = 0 and dot.submitted_time > 1609439493
                            left join (SELECT order_id
                                            ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
                                            ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)

                                        from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet

                                        )dotet on dot.id = dotet.order_id

                        where cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time)  - 3600) as date) >= date('2020-12-31')  -- date(current_date) - interval '75' day
                        and cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time)  - 3600) as date) <= date(current_date)
                        and ad_odt.partner_id > 0

                        union all

                      --  EXPLAIN ANALYZE
                        -- NS User = NS Instant
                        select  distinct ad_ns.order_id,ad_ns.partner_id --,dot.ref_order_code
                            ,case when ad_ns.city_id = 217 then 'HCM'
                                  when ad_ns.city_id = 218 then 'HN'
                                  when ad_ns.city_id = 219 then 'DN'
                                  when ad_ns.city_id = 220 then 'HP'
                                  else 'OTH' end as city_name
                            ,ad_ns.city_id
                            ,cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) as date_
                            ,CASE
                                WHEN WEEK(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600))) >= 52 AND MONTH(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600))) = 1 THEN (YEAR(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600)))-1)*100 + WEEK(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600)))
                                WHEN WEEK(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600))) = 1 AND MONTH(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600))) = 12 THEN (YEAR(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600)))+1)*100 + WEEK(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600)))
                            ELSE YEAR(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600)))*100 + WEEK(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600))) END as year_week
                            ,ad_ns.partner_type
                            ,'Now Ship' as source
                            ,'NS Instant' as sub_source
                            ,0 as collect_from_customer
                            ,ebt.distance*1.00/1000 as distance
                            ,ebt.status
                            ,0 as user_bwf
                            ,dot.delivery_cost*1.00000000/100 as total_shipping_fee  -- ok

                            ,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) >= date('2021-02-01') then GREATEST(13500,coalesce(dotet.unit_fee,4050)*(ebt.distance*1.00/1000))
                                else GREATEST(15000,coalesce(dotet.unit_fee,5000)*(ebt.distance*1.00/1000))
                                end as total_shipping_fee_basic

                            ,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) >=  date('2021-02-01') then
                                    GREATEST(dot.delivery_cost*1.00000000/100 - GREATEST(13500,coalesce(dotet.unit_fee,4050)*(ebt.distance*1.00/1000))  ,0)
                                else GREATEST(dot.delivery_cost*1.00000000/100 - GREATEST(15000,coalesce(dotet.unit_fee,5000)*(ebt.distance*1.00/1000))   ,0)
                                end as total_shipping_fee_surge

                            ,0 as bad_weather_cost_driver_new
                            ,case when ebt.status in (14,19) then cast(json_extract(ebt.extra_data, '$.shipping_fee.return_fee') as DOUBLE) -- returned
                                    else 0 end as total_return_fee
                            ,case when ebt.status in (14,19) then GREATEST(15000,5000*(ebt.distance*1.00/1000))*1.00/2
                                    else 0 end as total_return_fee_basic
                            ,case when ebt.status in (14,19) then GREATEST(coalesce(cast(json_extract(ebt.extra_data, '$.shipping_fee.return_fee') as DOUBLE),0) - coalesce(GREATEST(15000,5000*(ebt.distance*1.00/1000))*1.00/2,0),0)
                                    else 0 end as total_return_fee_surge
                           -- ,dot.ref_order_id
                            --,dot.ref_order_code

                            -- revenue calculation
                            ,eojd.delivery_cost_amount as rev_shipping_fee
                            ,case when prm.code LIKE '%NOW%' then eojd.foody_discount_amount
								  when prm.code LIKE '%NOWSHIP%' then eojd.foody_discount_amount
                                  else 0 end as prm_cost

                           -- , case when prm.code LIKE 'NOW%' and cast(json_extract(prm.conditions, '$.promotion_type') as DOUBLE) = 2 then 'ns_prm'
                            --       when prm.code LIKE 'NS%' and cast(json_extract(prm.conditions, '$.promotion_type') as DOUBLE) = 1 then 'e_voucher'
                            --       else null end as prm_type
                        --    , case when ebt.promotion_code_id = 0 then 'no promotion'
                          --          when prm.code LIKE 'NOW%'  then 'ns_prm'
                            --       when prm.code LIKE 'NS%'  then 'e_voucher'
                             --      else null end as prm_type_test

                            ,case when cast(json_extract(ebt.extra_data, '$.other_fees[0].other_fee_type') as DOUBLE) = 6 then cast(json_extract(ebt.extra_data, '$.other_fees[0].value') as DOUBLE)
                                    else 0 end as rev_cod_fee
                            ,case when ebt.status = 14 then eojd.shipping_return_fee else 0 end as rev_return_fee

                            -- hub order
                            ,coalesce(dotet.driver_payment_policy,1) as driver_payment_policy  -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)


                        from shopeefood.foody_accountant_db__order_now_ship_user_tab__reg_daily_s0_live ad_ns
                        Left join shopeefood.foody_express_db__booking_tab__reg_continuous_s0_live ebt on ebt.id = ad_ns.order_id and ebt.create_time > 1609439493
                        left join
                                 (SELECT id,create_timestamp,delivery_cost_amount,foody_discount_amount,shipping_return_fee
                                  FROM shopeefood.foody_mart__fact_express_order_join_detail

                                  WHERE grass_region = 'VN'
                                 )eojd on eojd.id = ebt.id and eojd.create_timestamp > 1609439493
                        left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on ebt.id = dot.ref_order_id and dot.ref_order_category = 4 and dot.submitted_time > 1609439493

                        left join (SELECT order_id
                                            ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
                                            ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)

                                        from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet

                                        )dotet on dot.id = dotet.order_id

                        left join shopeefood.foody_express_db__promotion_tab__reg_daily_s0_live prm on ebt.promotion_code_id = prm.id

                        where cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) >= date('2020-12-31') -- date(current_date) - interval '75' day
                        and cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) <= date(current_date)
                        and ad_ns.partner_id > 0

                        union all

                    --    EXPLAIN ANALYZE
                        -- NS Merchant = NS Food Merchant
                        select  distinct ad_ns.order_id,ad_ns.partner_id
                            ,case when ad_ns.city_id = 217 then 'HCM'
                                  when ad_ns.city_id = 218 then 'HN'
                                  when ad_ns.city_id = 219 then 'DN'
                                  when ad_ns.city_id = 220 then 'HP'
                                  else 'OTH' end as city_name
                            ,ad_ns.city_id
                            ,cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) as date_
                            ,CASE
                                WHEN WEEK(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600))) >= 52 AND MONTH(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600))) = 1 THEN (YEAR(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600)))-1)*100 + WEEK(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600)))
                                WHEN WEEK(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600))) = 1 AND MONTH(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600))) = 12 THEN (YEAR(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600)))+1)*100 + WEEK(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600)))
                            ELSE YEAR(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600)))*100 + WEEK(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600))) END as year_week
                            ,ad_ns.partner_type
                            ,'Now Ship' as source
                            ,'NS Food Merchant' as sub_source
                            ,0 as collect_from_customer
                            ,ebt.distance*1.00/1000 as distance
                            ,ebt.status
                            ,0 as user_bwf
                            ,dot.delivery_cost*1.00000000/100 as total_shipping_fee  -- ok

                            ,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) >= date('2021-02-01') then GREATEST(13500,coalesce(dotet.unit_fee,4050)*(ebt.distance*1.00/1000))
                                else GREATEST(15000,coalesce(dotet.unit_fee,5000)*(ebt.distance*1.00/1000))
                                end as total_shipping_fee_basic

                            ,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) >=  date('2021-02-01') then
                                    GREATEST(dot.delivery_cost*1.00000000/100 - GREATEST(13500,coalesce(dotet.unit_fee,4050)*(ebt.distance*1.00/1000))  ,0)
                                else GREATEST(dot.delivery_cost*1.00000000/100 - GREATEST(15000,coalesce(dotet.unit_fee,5000)*(ebt.distance*1.00/1000))   ,0)
                                end as total_shipping_fee_surge

                            ,0 as bad_weather_cost_driver_new
                            ,case when ebt.status in (14,19) then cast(json_extract(ebt.extra_data, '$.shipping_fee.return_fee') as DOUBLE) -- returned
                                    else 0 end as total_return_fee
                            ,case when ebt.status in (14,19) then GREATEST(15000,5000*(ebt.distance*1.00/1000))*1.00/2
                                    else 0 end as total_return_fee_basic
                            ,case when ebt.status in (14,19) then GREATEST(coalesce(cast(json_extract(ebt.extra_data, '$.shipping_fee.return_fee') as DOUBLE),0) - coalesce(GREATEST(15000,5000*(ebt.distance*1.00/1000))*1.00/2,0),0)
                                    else 0 end as total_return_fee_surge

                            -- revenue calculation
                            ,eojd.delivery_cost_amount as rev_shipping_fee
                            ,case when prm.code LIKE 'NOW%' then eojd.foody_discount_amount
								  when prm.code LIKE '%NOWSHIP%' then eojd.foody_discount_amount
                                  else 0 end as prm_cost

                            ,case when cast(json_extract(ebt.extra_data, '$.other_fees[0].other_fee_type') as DOUBLE) = 6 then cast(json_extract(ebt.extra_data, '$.other_fees[0].value') as DOUBLE)
                                    else 0 end as rev_cod_fee
                            ,case when ebt.status = 14 then eojd.shipping_return_fee else 0 end as rev_return_fee

                            -- hub order
                            ,coalesce(dotet.driver_payment_policy,1) as driver_payment_policy  -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)


                        from shopeefood.foody_accountant_db__order_now_ship_merchant_tab__reg_daily_s0_live ad_ns
                        Left join shopeefood.foody_express_db__booking_tab__reg_continuous_s0_live ebt on ebt.id = ad_ns.order_id and ebt.create_time > 1609439493
                        left join
                                 (SELECT id,create_timestamp,delivery_cost_amount,foody_discount_amount,shipping_return_fee
                                  FROM shopeefood.foody_mart__fact_express_order_join_detail

                                  WHERE grass_region = 'VN'
                                 )eojd on eojd.id = ebt.id and eojd.create_timestamp > 1609439493
                        left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on ebt.id = dot.ref_order_id and dot.ref_order_category = 5 and dot.submitted_time > 1609439493

                        left join (SELECT order_id
                                            ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
                                            ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)

                                        from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet

                                        )dotet on dot.id = dotet.order_id

                        left join shopeefood.foody_express_db__promotion_tab__reg_daily_s0_live prm on ebt.promotion_code_id = prm.id

                        where cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) >= date('2020-12-31') -- date(current_date) - interval '75' day
                        and cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) <= date(current_date)
                        and ad_ns.partner_id > 0

                        union all

                    --    EXPLAIN ANALYZE
                        -- NS Shopee
                        select  distinct ad_nss.order_id,ad_nss.partner_id
                            ,case when ad_nss.city_id = 217 then 'HCM'
                                  when ad_nss.city_id = 218 then 'HN'
                                  when ad_nss.city_id = 219 then 'DN'
                                  when ad_nss.city_id = 220 then 'HP'
                                  else 'OTH' end as city_name
                            ,ad_nss.city_id
                            ,cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600) as date) as date_
                            ,CASE
                                WHEN WEEK(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600))) >= 52 AND MONTH(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600))) = 1 THEN (YEAR(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600)))-1)*100 + WEEK(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600)))
                                WHEN WEEK(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600))) = 1 AND MONTH(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600))) = 12 THEN (YEAR(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600)))+1)*100 + WEEK(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600)))
                            ELSE YEAR(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600)))*100 + WEEK(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600))) END as year_week
                            ,ad_nss.partner_type
                            ,'Now Ship Shopee' as source
                            ,'Now Ship Shopee' as sub_source
                            ,0 as collect_from_customer
                            ,esbt.distance*1.00/1000 as distance
                            ,esbt.status
                            ,0 as user_bwf
                            ,dot.delivery_cost*1.00000000/100 as total_shipping_fee  -- ok

                            ,case when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600) as date) >= date('2021-02-01') then GREATEST(13500,coalesce(dotet.unit_fee,4050)*(esbt.distance*1.00/1000))
                                else GREATEST(15000,coalesce(dotet.unit_fee,5000)*(esbt.distance*1.00/1000))
                                end as total_shipping_fee_basic

                            ,case when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600) as date) >=  date('2021-02-01') then
                                    GREATEST(dot.delivery_cost*1.00000000/100 - GREATEST(13500,coalesce(dotet.unit_fee,4050)*(esbt.distance*1.00/1000))  ,0)
                                else GREATEST(dot.delivery_cost*1.00000000/100 - GREATEST(15000,coalesce(dotet.unit_fee,5000)*(esbt.distance*1.00/1000))   ,0)
                                end as total_shipping_fee_surge

                            ,0 as bad_weather_cost_driver_new
                            ,case when esbt.status in (14,19) then cast(json_extract(esbt.extra_data, '$.shipping_fee.return_fee') as DOUBLE)  -- returned
                                    else 0 end as total_return_fee
                            ,case when esbt.status in (14,19) then GREATEST(15000,5000*(esbt.distance*1.00/1000))*1.00/2
                                    else 0 end as total_return_fee_basic
                            ,case when esbt.status in (14,19) then GREATEST(coalesce(cast(json_extract(esbt.extra_data, '$.shipping_fee.return_fee') as DOUBLE),0) - coalesce(GREATEST(15000,5000*(esbt.distance*1.00/1000))*1.00/2,0),0)
                                    else 0 end as total_return_fee_surge

                            -- revenue calculation
                            ,case
                                    when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >= date('2022-02-04') then
                                        case when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 18654.84
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 26508.6
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 6 then 29945.16
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 20 then 29945.16+ (ceiling(esbt.distance *1.000 / 1000) -6 )*4418.28
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) >20 then 91801.08 + (ceiling(esbt.distance *1.000 / 1000) -20)*7854.84
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 18654.84*1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 26508.6 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 6 then 29945.16*1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 20 then (29945.16 + (ceiling(esbt.distance *1.000 / 1000) -6)*4418.28) *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) >20 then (91801.08 + (ceiling(esbt.distance *1.000 / 1000) -20)*7854.84) *1.5
                                            else null
                                        end

                                    when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >= date('2022-01-30') then
                                        case when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 29454.84
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 37308.6
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 6 then 40745.16
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 20 then 40745.16 + (ceiling(esbt.distance *1.000 / 1000) -6 )*4418.28
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) >20 then 102601.08 + (ceiling(esbt.distance *1.000 / 1000) -20)*7854.84
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 29454.84 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 37308.6 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 6 then 40745.16*1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 20 then (40745.16 + (ceiling(esbt.distance *1.000 / 1000) -6)*4418.28) *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) >20 then (102601.08 + (ceiling(esbt.distance *1.000 / 1000) -20)*7854.84) *1.5
                                            else null
                                        end

                                    when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >= date('2021-10-13') then
                                        case when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 19000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 27000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 6 then 30500
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 20 then 30500 + (ceiling(esbt.distance *1.000 / 1000) -6 )*4500
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) >20 then 93500 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 19000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 27000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 6 then 30500*1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 20 then (30500 + (ceiling(esbt.distance *1.000 / 1000) -6)*4500) *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) >20 then (93500 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000) *1.5
                                            else null
                                        end

                                    when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >= date('2021-10-01') and ad_nss.city_id = 217 then
                                        case when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 28000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 28000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 6 then 37000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 20 then 37000 + (ceiling(esbt.distance *1.000 / 1000) -6 )*4500
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) >20 then 100000 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 28000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 28000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 6 then 37000*1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 20 then (37000 + (ceiling(esbt.distance *1.000 / 1000) -6)*4500) *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) >20 then (100000 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000) *1.5
                                            else null
                                        end

                                    when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >= date('2021-09-30') and ad_nss.city_id != 217 then
                                        case when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 26000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 26000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 20 then 26000 + (ceiling(esbt.distance *1.000 / 1000) -5 )*4500
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) >20 then 93500 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 26000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 26000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) > 5 then (26000 + (ceiling(esbt.distance *1.000 / 1000) -5)*4500) *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) >20 then (93500 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000) *1.5
                                            else null
                                        end

                                    when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >= date('2021-09-29') and ad_nss.city_id = 218 then
                                        case when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 26000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 26000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 20 then 26000 + (ceiling(esbt.distance *1.000 / 1000) -5 )*4500
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) >20 then 93500 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 26000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 26000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) > 5 then (26000 + (ceiling(esbt.distance *1.000 / 1000) -5)*4500) *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) >20 then (93500 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000) *1.5
                                            else null
                                        end

                                    when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >= date('2021-09-01') then
                                        case when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 35000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 35000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 20 then 41500 + (ceiling(esbt.distance *1.000 / 1000) -5 )*4500
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) >20 then 109000 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 35000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 35000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) > 5 then (41500 + (ceiling(esbt.distance *1.000 / 1000) -5)*4500) *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) >20 then (109000 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000) *1.5
                                            else null
                                        end

                                    when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >= date('2021-08-19') then
                                        case when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 27000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 27000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 20 then 33500 + (ceiling(esbt.distance *1.000 / 1000) -5 )*4500
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) >20 then 101000 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 27000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 27000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) > 5 then (33500 + (ceiling(esbt.distance *1.000 / 1000) -5)*4500) *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) >20 then (101000 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000) *1.5
                                            else null
                                        end

                                    when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >= date('2021-08-18') then
                                        case when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 20000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 28000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 20 then 26000 + (ceiling(esbt.distance *1.000 / 1000) -5 )*4500
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) >20 then 93000 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 20000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 28000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) > 5 then (26000 + (ceiling(esbt.distance *1.000 / 1000) -5)*4500) *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) >20 then (93000 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000) *1.5
                                            else null
                                        end

                                    when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >= date('2021-07-15') then
                                        case when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 18000
                                                                when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 26000
                                                                when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 20 then 26000 + (ceiling(esbt.distance *1.000 / 1000) -5 )*4500
                                                                when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) >20 then 93000 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000
                                                                when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 18000 *1.5
                                                                when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 26000 *1.5
                                                                when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) > 5 then (26000 + (ceiling(esbt.distance *1.000 / 1000) -5)*4500) *1.5
                                                                when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) >20 then (93000 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000) *1.5
                                                                else null
                                        end

                                       when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >= date('2021-03-01') then
                                                            case when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 17500
                                                                when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 25500
                                                                when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 20 then 25500 + (ceiling(esbt.distance *1.000 / 1000) -5 )*4500
                                                                when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) >20 then 93000 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000
                                                                when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 17500 *1.5
                                                                when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 25500 *1.5
                                                                when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) > 5 then (25500 + (ceiling(esbt.distance *1.000 / 1000) -5)*4500) *1.5
                                                                when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) >20 then (93000 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000) *1.5
                                                                else null
                                                            end
                                                        -- before 2021-03-01 - change NSS ratecard
                                                        when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 15000
                                                        when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 25000
                                                        when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) > 5 then 25000 + (ceiling(esbt.distance *1.000 / 1000) -5)*4500
                                                        when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) >20 then 92500 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000
                                                        when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 15000 *1.5
                                                        when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 25000 *1.5
                                                        when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) > 5 then (25000 + (ceiling(esbt.distance *1.000 / 1000) -5)*4500) *1.5
                                                        when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) >20 then (92500 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000) *1.5
                                                        else null end as rev_shipping_fee
                            ,0 as prm_cost
                            ,0 as rev_cod_fee
                            ,0 as rev_return_fee

                            -- hub order
                            ,coalesce(dotet.driver_payment_policy,1) as driver_payment_policy  -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)


                        from shopeefood.foody_accountant_db__order_now_ship_shopee_tab__reg_daily_s0_live ad_nss
                        Left join shopeefood.foody_express_db__shopee_booking_tab__reg_daily_s0_live esbt on esbt.id = ad_nss.order_id and esbt.create_time > 1609439493
                        left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on esbt.id = dot.ref_order_id and dot.ref_order_category = 6 and dot.submitted_time > 1609439493

                        left join (SELECT order_id
                                            ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
                                            ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)

                                        from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet

                                        )dotet on dot.id = dotet.order_id


                        where cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600) as date) >=  date('2020-12-31') -- ate(current_date) - interval '75' day
                        and cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600) as date) <= date(current_date)
                        and ad_nss.partner_id > 0
                        and booking_type = 4

                    UNION all
                       --    EXPLAIN ANALYZE
                        -- SPX Portal
                        select  distinct ad_nss.order_id,ad_nss.partner_id
                            ,case when ad_nss.city_id = 217 then 'HCM'
                                  when ad_nss.city_id = 218 then 'HN'
                                  when ad_nss.city_id = 219 then 'DN'
                                  when ad_nss.city_id = 220 then 'HP'
                                  else 'OTH' end as city_name
                            ,ad_nss.city_id
                            ,cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) as date_
                            ,CASE
                                WHEN WEEK(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600))) >= 52 AND MONTH(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600))) = 1 THEN (YEAR(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600)))-1)*100 + WEEK(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600)))
                                WHEN WEEK(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600))) = 1 AND MONTH(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600))) = 12 THEN (YEAR(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600)))+1)*100 + WEEK(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600)))
                            ELSE YEAR(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600)))*100 + WEEK(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600))) END as year_week
                            ,ad_nss.partner_type
                            ,'Now Ship' as source
                            ,'SPX Portal' as sub_source
                            ,0 as collect_from_customer
                            ,esbt.distance*1.00/1000 as distance
                            ,esbt.status
                            ,0 as user_bwf
                            ,dot.delivery_cost*1.00000000/100 as total_shipping_fee  -- ok

                            ,case when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >= date('2021-02-01') then GREATEST(13500,coalesce(dotet.unit_fee,4050)*(esbt.distance*1.00/1000))
                                else GREATEST(15000,coalesce(dotet.unit_fee,5000)*(esbt.distance*1.00/1000))
                                end as total_shipping_fee_basic

                            ,case when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >=  date('2021-02-01') then
                                    GREATEST(dot.delivery_cost*1.00000000/100 - GREATEST(13500,coalesce(dotet.unit_fee,4050)*(esbt.distance*1.00/1000))  ,0)
                                else GREATEST(dot.delivery_cost*1.00000000/100 - GREATEST(15000,coalesce(dotet.unit_fee,5000)*(esbt.distance*1.00/1000))   ,0)
                                end as total_shipping_fee_surge

                            ,0 as bad_weather_cost_driver_new
                            ,case when esbt.status in (14,19) then cast(json_extract(esbt.extra_data, '$.shipping_fee.return_fee') as DOUBLE)  -- returned
                                    else 0 end as total_return_fee
                            ,case when esbt.status in (14,19) then GREATEST(15000,5000*(esbt.distance*1.00/1000))*1.00/2
                                    else 0 end as total_return_fee_basic
                            ,case when esbt.status in (14,19) then GREATEST(coalesce(cast(json_extract(esbt.extra_data, '$.shipping_fee.return_fee') as DOUBLE),0) - coalesce(GREATEST(15000,5000*(esbt.distance*1.00/1000))*1.00/2,0),0)
                                    else 0 end as total_return_fee_surge

                            -- revenue calculation
                            ,cast(json_extract(esbt.extra_data, '$.shipping_fee.shipping_fee_origin') as DOUBLE) as rev_shipping_fee
                            ,0 as prm_cost
                            ,0 as rev_cod_fee
                            ,case when status in (14,15,22) then cast(json_extract(esbt.extra_data, '$.shipping_fee.return_fee') as DOUBLE) else 0 end as rev_return_fee

                            -- hub order
                            ,coalesce(dotet.driver_payment_policy,1) as driver_payment_policy  -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)


                        from shopeefood.foody_accountant_db__order_now_ship_shopee_tab__reg_daily_s0_live ad_nss
                        Left join shopeefood.foody_express_db__shopee_booking_tab__reg_daily_s0_live esbt on esbt.id = ad_nss.order_id and esbt.create_time > 1609439493
                        left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on esbt.id = dot.ref_order_id and dot.ref_order_category = 6 and dot.submitted_time > 1609439493

                        left join (SELECT order_id
                                            ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
                                            ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)

                                        from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet

                                        )dotet on dot.id = dotet.order_id


                        where cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >=  date('2020-12-31') -- ate(current_date) - interval '75' day
                        and cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) <= date(current_date)
                        and ad_nss.partner_id > 0
                        and booking_type = 5

                    UNION all

                --    EXPLAIN ANALYZE
                    -- NS Same Day
                        SELECT distinct ad_ns.order_id,ad_ns.partner_id
                            ,case when ad_ns.city_id = 217 then 'HCM'
                                  when ad_ns.city_id = 218 then 'HN'
                                  when ad_ns.city_id = 219 then 'DN'
                                  when ad_ns.city_id = 220 then 'HP'
                                  else 'OTH' end as city_name
                            ,ad_ns.city_id
                            ,cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) as date_
                            ,CASE
                                WHEN WEEK(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600))) >= 52 AND MONTH(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600))) = 1 THEN (YEAR(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600)))-1)*100 + WEEK(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600)))
                                WHEN WEEK(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600))) = 1 AND MONTH(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600))) = 12 THEN (YEAR(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600)))+1)*100 + WEEK(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600)))
                            ELSE YEAR(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600)))*100 + WEEK(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600))) END as year_week
                            ,ad_ns.partner_type
                            ,'Now Ship' as source
                            ,'NS Sameday' as sub_source
                            ,0 as collect_from_customer
                            ,ebt.distance*1.00/1000 as distance
                            ,ebt.status
                            ,0 as user_bwf
                            ,dot.delivery_cost*1.00000000/100 as total_shipping_fee  -- ok

                            ,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) >= date('2021-02-01') then GREATEST(13500,coalesce(dotet.unit_fee,4050)*(ebt.distance*1.00/1000))
                                else GREATEST(15000,coalesce(dotet.unit_fee,5000)*(ebt.distance*1.00/1000))
                                end as total_shipping_fee_basic

                            ,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) >=  date('2021-02-01') then
                                    GREATEST(dot.delivery_cost*1.00000000/100 - GREATEST(13500,coalesce(dotet.unit_fee,4050)*(ebt.distance*1.00/1000))  ,0)
                                else GREATEST(dot.delivery_cost*1.00000000/100 - GREATEST(15000,coalesce(dotet.unit_fee,5000)*(ebt.distance*1.00/1000))   ,0)
                                end as total_shipping_fee_surge

                            ,0 as bad_weather_cost_driver_new
                            ,case when ebt.status in (14,19) then cast(json_extract(ebt.extra_data, '$.shipping_fee.return_fee') as DOUBLE) -- returned
                                    else 0 end as total_return_fee
                            ,case when ebt.status in (14,19) then GREATEST(15000,5000*(ebt.distance*1.00/1000))*1.00/2
                                    else 0 end as total_return_fee_basic
                            ,case when ebt.status in (14,19) then GREATEST(coalesce(cast(json_extract(ebt.extra_data, '$.shipping_fee.return_fee') as DOUBLE),0) - coalesce(GREATEST(15000,5000*(ebt.distance*1.00/1000))*1.00/2,0),0)
                                    else 0 end as total_return_fee_surge

                            -- revenue calculation
                            ,eojd.delivery_cost_amount as rev_shipping_fee
                            ,case when prm.code LIKE 'NOW%' then eojd.foody_discount_amount
								  when prm.code LIKE '%NOWSHIP%' then eojd.foody_discount_amount
                                  else 0 end as prm_cost

                            ,case when cast(json_extract(ebt.extra_data, '$.other_fees[0].other_fee_type') as DOUBLE) = 6 then cast(json_extract(ebt.extra_data, '$.other_fees[0].value') as DOUBLE)
                                    else 0 end as rev_cod_fee
                            ,case when ebt.status = 14 then eojd.shipping_return_fee else 0 end as rev_return_fee

                            -- hub order
                            ,coalesce(dotet.driver_payment_policy,1) as driver_payment_policy  -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)


                        from shopeefood.foody_accountant_db__order_now_ship_sameday_tab__reg_daily_s0_live ad_ns
                        Left join shopeefood.foody_express_db__booking_tab__reg_continuous_s0_live ebt on ebt.id = ad_ns.order_id and ebt.create_time > 1609439493
                        left join
                                 (SELECT id,create_timestamp,delivery_cost_amount,foody_discount_amount,shipping_return_fee
                                  FROM shopeefood.foody_mart__fact_express_order_join_detail

                                  WHERE grass_region = 'VN'
                                 )eojd on eojd.id = ebt.id and eojd.create_timestamp > 1609439493
                        left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on ebt.id = dot.ref_order_id and dot.ref_order_category = 7 and dot.submitted_time > 1609439493

                        left join (SELECT order_id
                                            ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
                                            ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)

                                        from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet

                                        )dotet on dot.id = dotet.order_id

                        left join shopeefood.foody_express_db__promotion_tab__reg_daily_s0_live prm on ebt.promotion_code_id = prm.id

                        where cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) >= date('2020-12-31') -- date(current_date) - interval '75' day
                        and cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) <= date(current_date)
                        and ad_ns.partner_id > 0

                    --    limit 100

					UNION all

                    -- NS Multi Drop

                    select  distinct ad_ns.order_id,ad_ns.partner_id -- ,dot.ref_order_code,ebt.id  as ebt_id, eojd.id  as eojd_id
						,case when ad_ns.city_id = 217 then 'HCM'
							  when ad_ns.city_id = 218 then 'HN'
							  when ad_ns.city_id = 219 then 'DN'
							  when ad_ns.city_id = 220 then 'HP'
							  else 'OTH' end as city_name
						,ad_ns.city_id
						,cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) as date_
						,CASE
                            WHEN WEEK(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600))) >= 52 AND MONTH(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600))) = 1 THEN (YEAR(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600)))-1)*100 + WEEK(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600)))
                            WHEN WEEK(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600))) = 1 AND MONTH(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600))) = 12 THEN (YEAR(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600)))+1)*100 + WEEK(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600)))
                        ELSE YEAR(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600)))*100 + WEEK(DATE(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600))) END as year_week
						,ad_ns.partner_type
						,'Now Ship' as source
						,'NS Instant' as sub_source
						,0 as collect_from_customer
						,ebt.distance*1.00/1000 as distance
						,ebt.status
						,0 as user_bwf
						,dot.delivery_cost*1.00000000/100 as total_shipping_fee  -- ok

						,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) >= date('2021-02-01') then GREATEST(13500,coalesce(dotet.unit_fee,4050)*(ebt.distance*1.00/1000))
							else GREATEST(15000,coalesce(dotet.unit_fee,5000)*(ebt.distance*1.00/1000))
							end as total_shipping_fee_basic

						,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) >=  date('2021-02-01') then
								GREATEST(dot.delivery_cost*1.00000000/100 - GREATEST(13500,coalesce(dotet.unit_fee,4050)*(ebt.distance*1.00/1000))  ,0)
							else GREATEST(dot.delivery_cost*1.00000000/100 - GREATEST(15000,coalesce(dotet.unit_fee,5000)*(ebt.distance*1.00/1000))   ,0)
							end as total_shipping_fee_surge

						,0 as bad_weather_cost_driver_new
						,case when ebt.status in (14,19) then cast(json_extract(ebt.extra_data, '$.shipping_fee.return_fee') as DOUBLE) -- returned
								else 0 end as total_return_fee
						,case when ebt.status in (14,19) then GREATEST(15000,5000*(ebt.distance*1.00/1000))*1.00/2
								else 0 end as total_return_fee_basic
						,case when ebt.status in (14,19) then GREATEST(coalesce(cast(json_extract(ebt.extra_data, '$.shipping_fee.return_fee') as DOUBLE),0) - coalesce(GREATEST(15000,5000*(ebt.distance*1.00/1000))*1.00/2,0),0)
								else 0 end as total_return_fee_surge
					   -- ,dot.ref_order_id
						--,dot.ref_order_code

						-- revenue calculation
					   -- ,eojd.delivery_cost_amount as rev_shipping_fee
						,coalesce(cast(json_extract(ebt.extra_data, '$.shipping_fee.shipping_fee_origin') as DOUBLE),0)
						  + coalesce(case
								when cast(json_extract(ebt.extra_data, '$.other_fees[0].other_fee_type') as DOUBLE) = 10 then cast(json_extract(ebt.extra_data, '$.other_fees[0].value') as DOUBLE)
								when cast(json_extract(ebt.extra_data, '$.other_fees[1].other_fee_type') as DOUBLE) = 10 then cast(json_extract(ebt.extra_data, '$.other_fees[1].value') as DOUBLE)
								when cast(json_extract(ebt.extra_data, '$.other_fees[2].other_fee_type') as DOUBLE) = 10 then cast(json_extract(ebt.extra_data, '$.other_fees[2].value') as DOUBLE)
								else 0 end,0)-- as rev_drop_fee
							as rev_shipping_fee


						,case when prm.code LIKE '%NOW%' then ebt.discount_amount
                              when prm.code LIKE '%NOWSHIP%' then ebt.discount_amount
                              when prm.code LIKE '%SPXINSTANT%' then ebt.discount_amount
                              else 0 end as prm_cost
					   -- ,case when prm.code like '%NOW%' then ebt.discount_amount else 0 end as prm_cost


					   -- , case when prm.code LIKE 'NOW%' and cast(json_extract(prm.conditions, '$.promotion_type') as DOUBLE) = 2 then 'ns_prm'
						--       when prm.code LIKE 'NS%' and cast(json_extract(prm.conditions, '$.promotion_type') as DOUBLE) = 1 then 'e_voucher'
						--       else null end as prm_type
					--    , case when ebt.promotion_code_id = 0 then 'no promotion'
					  --          when prm.code LIKE 'NOW%'  then 'ns_prm'
						--       when prm.code LIKE 'NS%'  then 'e_voucher'
						 --      else null end as prm_type_test

						 ,case
							when cast(json_extract(ebt.extra_data, '$.other_fees[0].other_fee_type') as DOUBLE) = 6 then cast(json_extract(ebt.extra_data, '$.other_fees[0].value') as DOUBLE)
							when cast(json_extract(ebt.extra_data, '$.other_fees[1].other_fee_type') as DOUBLE) = 6 then cast(json_extract(ebt.extra_data, '$.other_fees[1].value') as DOUBLE)
							when cast(json_extract(ebt.extra_data, '$.other_fees[2].other_fee_type') as DOUBLE) = 6 then cast(json_extract(ebt.extra_data, '$.other_fees[2].value') as DOUBLE)
							else 0 end as rev_cod_fee
					  --  ,case when cast(json_extract(ebt.extra_data, '$.other_fees[0].other_fee_type') as DOUBLE) = 6 then cast(json_extract(ebt.extra_data, '$.other_fees[0].value') as DOUBLE)
					--            else 0 end as rev_cod_fee
					--    ,case when ebt.status = 14 then eojd.shipping_return_fee else 0 end as rev_return_fee
						,case when ebt.status = 14 then cast(json_extract(ebt.extra_data, '$.shipping_fee.return_fee') as DOUBLE) else 0 end as rev_return_fee

						-- hub order
						,coalesce(dotet.driver_payment_policy,1) as driver_payment_policy  -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)


					from shopeefood.foody_accountant_db__order_now_ship_multi_drop_tab__reg_daily_s0_live ad_ns
					Left join shopeefood.foody_express_db__booking_tab__reg_continuous_s0_live ebt on ebt.id = ad_ns.order_id and ebt.create_time > 1609439493

					left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on ebt.id = dot.ref_order_id and dot.ref_order_category = 8 and dot.submitted_time > 1609439493

					left join (SELECT order_id
										,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
										,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
										,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
										,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
										,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
										,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)

									from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet

									)dotet on dot.id = dotet.order_id

					left join shopeefood.foody_express_db__promotion_tab__reg_daily_s0_live prm on ebt.promotion_code_id = prm.id

					where cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) >= date('2021-01-01') -- date(current_date) - interval '75' day
					and cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) <= date(current_date)
					and ad_ns.partner_id > 0
					-- and dot.ref_order_code = '210709SE2955'

					--limit 1000

                    )o

                    -- take drivers' total point / tier of that day
                LEFT JOIN (SELECT cast(from_unixtime(bonus.report_date - 3600) as date) as report_date
                                ,bonus.uid as shipper_id
                               -- ,case when bonus.total_point <= 450 then 'T1'
                                --    when bonus.total_point <= 1300 then 'T2'
                                --    when bonus.total_point <= 2950 then 'T3'
                                --    when bonus.total_point <= 4400 then 'T4'
                                --    when bonus.total_point > 4400 then 'T5'
                                --    else null end as current_driver_tier
                                ,case when bonus.total_point <= 1800 then 'T1'
                                    when bonus.total_point <= 3600 then 'T2'
                                    when bonus.total_point <= 5400 then 'T3'
                                    when bonus.total_point <= 8400 then 'T4'
                                    when bonus.total_point > 8400 then 'T5'
                                    -- when bonus.total_point > 9600 then 'T6'

                                    else null end as new_driver_tier

                                ,case when bonus.tier in (1,6,11) then 'T1' -- as current_driver_tier
                                    when bonus.tier in (2,7,12) then 'T2'
                                    when bonus.tier in (3,8,13) then 'T3'
                                    when bonus.tier in (4,9,14) then 'T4'
                                    when bonus.tier in (5,10,15) then 'T5'
                                    else null end as current_driver_tier
                                ,bonus.total_point

                            FROM shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus

                        )bonus on o.date_ = bonus.report_date and o.partner_id = bonus.shipper_id

            --    Where  o.date_ between date('2020-06-15') and date('2020-06-21') -- date('2019-12-01')
            --    and o.city_name in ('HCM','HN')
            --    and bonus.current_driver_tier is null
             --   where date_ = date'2022-01-20' and partner_id = 16408221
            --        limit 100

                  --  limit 1000
                  --  LEFT JOIN shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct on o.order_id = oct.id

                )temp



    -- limit 1000
            )raw

    -- city name full
    left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = raw.city_id and city.country_id = 86

    -- transaction tbl --> calculate fee
    left join (SELECT reference_id
                    ,txn_type
                    ,balance
                    ,deposit
                    ,CASE
                        WHEN WEEK(DATE(from_unixtime(create_time,7,0))) >= 52 AND MONTH(DATE(from_unixtime(create_time,7,0))) = 1 THEN (YEAR(DATE(from_unixtime(create_time,7,0)))-1)*100 + WEEK(DATE(from_unixtime(create_time,7,0)))
                        WHEN WEEK(DATE(from_unixtime(create_time,7,0))) = 1 AND MONTH(DATE(from_unixtime(create_time,7,0))) = 12 THEN (YEAR(DATE(from_unixtime(create_time,7,0)))+1)*100 + WEEK(DATE(from_unixtime(create_time,7,0)))
                    ELSE YEAR(DATE(from_unixtime(create_time,7,0)))*100 + WEEK(DATE(from_unixtime(create_time,7,0))) END as year_week
                    ,date(from_unixtime(create_time - 3600)) as created_date
                    ,user_id

                from shopeefood.foody_accountant_db__partner_transaction_tab__reg_daily_s0_live

                where create_time > 1609439493
                -- and cast(from_unixtime(create_time,7,0) as date) >= date('2019-12-01') -- date(current_date) - interval '78' day
                -- and cast(from_unixtime(create_time,7,0) as date) <= date(current_date)

                and txn_type in (-- TYPE: BONUS, RECEIVED SHIPPING FEE, ADDITIONAL BONUS, OTHER PAYABLES (parking fee), RETURN FEE SHARED
                                          200,201,204,203,202, -- Now Ship User
                                          300,301,304,303,302, -- Now Ship Merchant
                                          400,401,404,403,402, -- Now Moto

                                          101,104,105,106,129,131,133,135,110,      -- Delivery Service, consider 105 DELIVERY_ADD_BONUS_MANUAL, 129:DELIVERY_ADD_HAND_DELIVERY_FEE_PASSTHROUGH, 131: DELIVERY_ADD_PARKING_FEE_PASSTHROUGH
                                                                                                                                        --133: DELIVERY_ADD_MERCHANT_PARKING_FEE_PASSTHROUGH, 135: DELIVERY_ADD_TIP_FEE_PASSTHROUGH
                                                                                                                                        --110: DELIVERY_ADD_AFTER_DELIVERY_TIP_FEE

                                          1006,1000,1003,1001,  -- Now Ship Shopee: 1000: recevied shipping fee, 1001: return fee shared, 1003: bonus from CS, 1006: bonus for FT driver
                                          2000,2001,2004,2003,2002,2005,2006,2007, -- Sameday
										  2100,2101,2104,2105,2106,2102, -- multidrop
                                          112,115, -- bad weather fee
                                          119, -- late night fee
                                          117 -- holiday fee
                                )
            --    and reference_id = 182853798 -- 183461946

                )trx on trx.reference_id = raw.order_id
                    and trx.user_id = raw.partner_id -- user_id = partner_id = shipper_id
                    and trx.created_date >= raw.date_ - interval '2' day and trx.created_date <= raw.date_ + interval '2' day      -- map by order Id --> more details than shipper_id



    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29

    --        limit 1000
    )raw2
 --   where raw2.order_id = 123030544
 --    and raw2.city_name = 'HCM'
 --   and raw2.source = 'now_ship_user'
   -- and raw2.status in (9)
-- limit 1000
)bill_fee
-- where bill_fee.partner_id in (3914704,14471485,12619522,3643307,14547020,14331935,7036587,14288958,4494740,4067405)

GROUP BY bill_fee.partner_id
,bill_fee.date_
,bill_fee.year_week
,bill_fee.city_name
,bill_fee.city_name_full
,bill_fee.shipper_type
,bill_fee.is_new_policy
,bill_fee.current_driver_tier
,bill_fee.new_driver_tier

)bill_fee_2
    -- Weekly Bonus: chốt ngày thứ 2 đầu tuần tiếp theo (grass_date)
    LEFT JOIN (select 
                user_id
                ,case when (cast(from_unixtime(create_time,7,0) as date) - interval '1' day) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
                        when (cast(from_unixtime(create_time,7,0) as date) - interval '1' day) between DATE('2020-12-28') and DATE('2021-01-03') then 202053
                        when (cast(from_unixtime(create_time,7,0) as date) - interval '1' day) between DATE('2022-01-01') and DATE('2022-01-02') then 202152
                                else YEAR((cast(from_unixtime(create_time,7,0) as date) - interval '1' day))*100 + WEEK((cast(from_unixtime(create_time,7,0) as date) - interval '1' day)) end as year_week
                ,SUM(case when txn_type in (701) then balance*1.0 else null end)/100 as weekly_bonus
                
                from shopeefood.foody_accountant_db__partner_transaction_tab__reg_continuous_s0_live 
                where create_time > 1609439493 
                -- and cast(from_unixtime(create_time,7,0) as date) >= date('2020-12-01') -- date(current_date) - interval '78' day
                --    and cast(from_unixtime(create_time,7,0) as date) <= date(current_date)
                    and txn_type in (701) 
            
                group by 1,2
            )bonus_week on bonus_week.user_id = bill_fee_2.partner_id and bonus_week.year_week = bill_fee_2.year_week and bill_fee_2.city_name in ('HCM','HN','DN') -- and bill_fee.is_order_city_same_as_shipper_city = 1 -- weekly: currently in HCM HN DN
            
    -- Daily Bonus: chốt vào ngày tiếp theo (grass_date cộng tới 1)        
    LEFT JOIN (select 
                user_id
                ,(cast(from_unixtime(create_time,7,0) as date) - interval '1' day) as date_
                ,case when (cast(from_unixtime(create_time,7,0) as date) - interval '1' day) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
                        when (cast(from_unixtime(create_time,7,0) as date) - interval '1' day) between DATE('2020-12-28') and DATE('2021-01-03') then 202053
                        when (cast(from_unixtime(create_time,7,0) as date) - interval '1' day) between DATE('2022-01-01') and DATE('2022-01-02') then 202152
                                else YEAR((cast(from_unixtime(create_time,7,0) as date) - interval '1' day))*100 + WEEK((cast(from_unixtime(create_time,7,0) as date) - interval '1' day)) end as year_week
                
                ,SUM(case when txn_type in (512,560,900,901) then balance*1.0 else null end)/100 as daily_bonus
                
                from shopeefood.foody_accountant_db__partner_transaction_tab__reg_continuous_s0_live 
                where create_time > 1609439493  
                -- and cast(from_unixtime(create_time,7,0) as date) >= date('2020-12-01') -- date(current_date) - interval '78' day
                -- and cast(from_unixtime(create_time,7,0) as date) <= date(current_date)
                and txn_type in (512,560,900,901)
                and coalesce(note,'na') NOT LIKE '%HUB_MODEL%' 
                and coalesce(note,'na') NOT LIKE '%Thuong chuong trinh tiep suc tai xe%' 
                and coalesce(note,'na') NOT LIKE '%Thuong Ngay Dai Tiec%'
                and coalesce(note,'na') NOT LIKE '%Thuong Mung Ngay Tro Lai%'
                and coalesce(note,'na') NOT LIKE '%Thuong mung tai xe moi%'
                group by 1,2,3
            )bonus_day on bonus_day.user_id = bill_fee_2.partner_id 
                        and bonus_day.year_week = bill_fee_2.year_week
                        and bonus_day.date_ = bill_fee_2.date_
                        and bill_fee_2.city_name in ('HCM','HN') and bill_fee_2.is_new_policy = 1 -- and bill_fee.is_order_city_same_as_shipper_city = 1 -- daily: currently in HCM
    
    -- bonus adjustment - scheme: 10k/order extra for hub drivers during lockdown in HCM / HN 
    LEFT JOIN (select 
                user_id
                ,case when note = 'HUB_MODEL_DAILYBONUS_09/07-11/07' then date('2021-07-09')
                      when note = 'HUB_MODEL_DAILYBONUS_19/07-22/07/2021' then date('2021-07-19')
                      when note = 'HUB_MODEL_DAILYBONUS_23.08.2021' then date('2021-08-23')
                      when note = 'HUB_MODEL_DAILYBONUS_24.08.2021' then date('2021-08-24')
                      when note = 'HUB_MODEL_DAILYBONUS_25.08.2021' then date('2021-08-25')
                      when note = 'HUB_MODEL_DAILYBONUS_26.08.2021' then date('2021-08-26') 
                                       else date_parse(substr(note,22), '%d/%m/%Y')
                      end as date_
             --   ,balance*1.000/100 as balance
             --   ,note
                
                ,SUM(case when txn_type in (512) then balance*1.0 else null end)/100 as daily_bonus
                
                from shopeefood.foody_accountant_db__partner_transaction_tab__reg_continuous_s0_live 
                where create_time > 1609439493  
                -- and cast(from_unixtime(create_time,7,0) as date) >= date('2020-12-01') -- date(current_date) - interval '78' day
                -- and cast(from_unixtime(create_time,7,0) as date) <= date(current_date)
                and txn_type in (512)
                and coalesce(note,'na') != 'HUB_MODEL_DAILYBONUS_11/11/2021'
                and coalesce(note,'na') != 'HUB_MODEL_DAILYBONUS_20/11/2021'
                and coalesce(note,'na') LIKE '%HUB_MODEL_DAILYBONUS%'           
                and (cast(from_unixtime(create_time,7,0) as date) - interval '1' day) >= date('2021-07-01') 
                and (cast(from_unixtime(create_time,7,0) as date) - interval '1' day) <= date('2021-11-01') 
                and cast(id as bigint) not in (351910689,349884120,349884121)
             --   and note not in ('HUB_MODEL_DAILYBONUS_19/07-22/07/2021','HUB_MODEL_DAILYBONUS_09/07-11/07')
                
                group by 1,2
    
                )bonus_day_adjust on bonus_day_adjust.user_id = bill_fee_2.partner_id
                                  and bonus_day_adjust.date_ = bill_fee_2.date_
                        
    -- bonus adjustment - new scheme: for covid support in HCM
    LEFT JOIN
    
                (
                select 
                         user_id
                        ,date_parse(substr(note,37), '%d/%m/%Y')as date_
                        ,SUM(case when txn_type in (512) then balance*1.0 else null end)/100 as daily_bonus
                        
                from shopeefood.foody_accountant_db__partner_transaction_tab__reg_continuous_s0_live 
                where create_time > 1609439493  
                
                and txn_type in (512)
                and coalesce(note,'na') LIKE '%Thuong chuong trinh tiep suc tai xe%' 
            --    and (cast(from_unixtime(create_time,7,0) as date) - interval '1' day) >= date('2021-07-01')
                
                group by 1,2
                
                )bonus_day_extra on bonus_day_extra.user_id = bill_fee_2.partner_id
                                  and bonus_day_extra.date_ = bill_fee_2.date_   

    -- bonus adjustment - 9.9 NS Scheme HCM + 11.11 NS Scheme + 12.12 + thuong gio vang
    LEFT JOIN
                (
                select 
                         user_id
                        ,date_parse(substr(note,-10), '%d/%m/%Y')as date_
                        ,SUM(case when txn_type in (512,519) then balance*1.0 else null end)/100 as daily_bonus
                        
                from shopeefood.foody_accountant_db__partner_transaction_tab__reg_continuous_s0_live 
                where create_time > 1609439493  
                
                and txn_type in (512,519) 
                and (coalesce(note,'na') LIKE '%Thuong Ngay Dai Tiec%' or coalesce(note,'na') LIKE '%Thuong Gio Vang%') 
             --   and (cast(from_unixtime(create_time,7,0) as date) - interval '1' day) >= date('2021-07-01')
                
                group by 1,2
                )bonus_day_cp on bonus_day_cp.user_id = bill_fee_2.partner_id
                                  and bonus_day_cp.date_ = bill_fee_2.date_                


    -- bonus adjustment - new scheme: weekly scheme 1,2 for HCM 16-29/09/ Thuong Li xi Tet 2022 week 04/02 - 10/02
    LEFT JOIN
    
                (
                select 
                         user_id
                        ,case when coalesce(note,'na') LIKE '%16-22/09%' then date('2021-09-16') 
                              when coalesce(note,'na') LIKE '%24-30/09%' then date('2021-09-24')  
                              when coalesce(note,'na') LIKE '%04/02-10/02%' then date('2022-02-04') 
                              else null end as start_date 
                        ,case when coalesce(note,'na') LIKE '%16-22/09%' then date('2021-09-22') 
                              when coalesce(note,'na') LIKE '%24-30/09%' then date('2021-09-30')  
                              when coalesce(note,'na') LIKE '%04/02-10/02%' then date('2022-02-10') 
                              else null end as end_date                               
                        ,SUM(case when txn_type in (505,519) then balance*1.0 else null end)/100 as weekly_bonus
                        
                from shopeefood.foody_accountant_db__partner_transaction_tab__reg_continuous_s0_live 
                where cast(from_unixtime(create_time,7,0) as date) >= date('2021-09-24') 
                
                and txn_type in (505,519)
                and (coalesce(note,'na') LIKE '%Thuong Mung Ngay Tro Lai%' or coalesce(note,'na') LIKE '%Thuong tai xe chuong trinh Li Xi Tet%')
                
                group by 1,2,3
                
                )bonus_week_extra on bonus_week_extra.user_id = bill_fee_2.partner_id
                                  and bill_fee_2.date_  between bonus_week_extra.start_date and bonus_week_extra.end_date                
    -- Hub cost - autopayment
    LEFT JOIN (select 
                    user_id
                    ,(cast(from_unixtime(create_time,7,0) as date) - interval '1' day) as date_
                    ,case when (cast(from_unixtime(create_time,7,0) as date) - interval '1' day) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
                            when (cast(from_unixtime(create_time,7,0) as date) - interval '1' day) between DATE('2020-12-28') and DATE('2021-01-03') then 202053
                            when (cast(from_unixtime(create_time,7,0) as date) - interval '1' day) between DATE('2022-01-01') and DATE('2022-01-02') then 202152
                                    else YEAR((cast(from_unixtime(create_time,7,0) as date) - interval '1' day))*100 + WEEK((cast(from_unixtime(create_time,7,0) as date) - interval '1' day)) end as year_week
                    
                    ,SUM(case when txn_type in (906,907) then balance*1.0000 + deposit*1.0000 else null end)/100 as hub_cost_auto
                    ,SUM(case when txn_type in (906) then balance*1.0000 + deposit*1.0000 else null end)/100 as hub_cost_auto_shipping_fee
                    ,SUM(case when txn_type in (907) then balance*1.0000 + deposit*1.0000 else null end)/100 as hub_cost_auto_daily_bonus
                    
                    from shopeefood.foody_accountant_db__partner_transaction_tab__reg_continuous_s0_live 
                    where (cast(from_unixtime(create_time,7,0) as date) - interval '1' day) > date('2021-04-17')  
                    -- and cast(from_unixtime(create_time,7,0) as date) >= date('2020-12-01') -- date(current_date) - interval '78' day
                    -- and cast(from_unixtime(create_time,7,0) as date) <= date(current_date)
                    and txn_type in (906,907)
                    
                    group by 1,2,3
    
                )hub_auto on hub_auto.user_id = bill_fee_2.partner_id
                          and hub_auto.year_week = bill_fee_2.year_week
                          and hub_auto.date_ = bill_fee_2.date_
                       --   and bill_fee_2.city_name in ('HCM','HN')
                          
    -- hub cost - adjustment for campaign / holiday
    LEFT JOIN (SELECT case when trim(note) in ('HUB_MODEL_SHIP_30/04') then date('2021-04-30')
                            when trim(note) in ('HUB_MODEL_SHIP_05/05') then date('2021-05-05')
                            when trim(note) LIKE '%HUB_MODEL_DAILYBONUS%' then date(date_parse(substr(trim(note),22,10),'%d/%m/%Y'))
                            when trim(note) LIKE '%HUB_MODEL_EXTRASHIP%' then date(date_parse(substr(trim(note),21,10),'%d/%m/%Y'))
                            when trim(note) LIKE '%ADJUSTMENT_SHIPPING FEE_HUB%' then date(date_parse(substr(note,-10,10),'%d/%m/%Y')) --- for Holiday Tet 2022
                            when trim(note) LIKE '%Thuong mung tai xe moi%' then date(date_parse(substr(trim(note),22,10),'%d/%m/%Y')) -- for new onboarding hub scheme
                      else null end as date_
                    
                        ,user_id
                        ,sum(trx.balance + trx.deposit)*1.0/(100*1.0) as hub_cost_bonus_adjustment
                           
                            
                from (select * from shopeefood.foody_accountant_db__partner_transaction_tab__reg_continuous_s0_live trx
                      where(trx.note not LIKE '%HUB_MODEL_EXTRASHIP_Bù thu nhập do lỗi hệ thống%' and trx.note not LIKE '%HUB_MODEL_EXTRASHIP_Chưa nhận auto pay do sup hub điều chỉnh ca trong shift%'
                      and trx.note not LIKE '%Lỗi sai thu nhập do Work Schedules%' and trx.note not LIKE '%HUB_MODEL_EXTRASHIP_Điều chỉnh thu nhập do miss config%')
                      and cast(trx.id as bigint) not in (390114868,390114871,390114871,390114869,390114870,390114867,399878797,
                                         399878783,399878786,399878789,399878805,399878777,399878768,399878769,399878747,399878814,399878791,399878821,399878782,
                                         399878818,399878801,399878785,399878796,399878767,399878802,399878817,399878798,399878813,399878772,399878795,399878820,399878819,399878784,399878770)
                     ) trx
                
                
                where 1=1 
                and trx.txn_type in (501,518,512,519,560,900,901)
                 and date(from_unixtime(trx.create_time - 60*60)) >= date('2020-11-01')
                and (trim(trx.note) in ('HUB_MODEL_SHIP_30/04','HUB_MODEL_SHIP_05/05') or (date(from_unixtime(create_time - 60*60))  > date('2021-11-11') and (trim(note) LIKE 'HUB_MODEL_EXTRASHIP%' or trim(note) LIKE '%HUB_MODEL_DAILYBONUS%')) or (trim(note) LIKE '%ADJUSTMENT_SHIPPING FEE_HUB%')
                     or (trim(note) LIKE '%Thuong mung tai xe moi%'))
                
                
                GROUP BY 1,2
                -- ORDER BY 1 ASC

                )hub_adj on hub_adj.user_id = bill_fee_2.partner_id
                          and hub_adj.date_ = bill_fee_2.date_
                         -- and bill_fee_2.city_name in ('HCM','HN') 
    
    -- hub cost - weekly bonus
    LEFT JOIN (select user_id
                --    ,(date(from_unixtime(create_time - 60*60)) - interval '7' day) as created_date
                    ,case when (date(from_unixtime(create_time - 60*60)) - interval '7' day) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
                                                  when (date(from_unixtime(create_time - 60*60)) - interval '7' day) between DATE('2020-12-28') and DATE('2021-01-03') then 202053
                                                  when (date(from_unixtime(create_time - 60*60)) - interval '7' day) between DATE('2022-01-01') and DATE('2022-01-02') then 202152
                                                    else YEAR((date(from_unixtime(create_time - 60*60)) - interval '7' day))*100 + WEEK((date(from_unixtime(create_time - 60*60)) - interval '7' day)) end as year_week_text
                                            
                                    
                        --            ,case when note in ('HUB_MODEL_WEEKLYBONUS_24/05-30/05') then 202121
                        --                  when note in ('HUB_MODEL_WEEKLYBONUS_31/05-06/06') then 202122
                        --                  when note in ('HUB_MODEL_WEEKLYBONUS_07/06-13/06','HUB_MODEL_WEEKLY BONUS_07/06-13/06') then 202123
                        --                  when note in ('HUB_MODEL_WEEKLYBONUS_14/06-20/06','HUB_MODEL_WEEKLY BONUS_14/06-20/06') then 202123
                        --                  when note in ('HUB_MODEL_WEEKLYBONUS_21/06-27/06','HUB_MODEL_WEEKLY BONUS_21/06-27/06') then 202123
                                          
                        --                  else null end as year_week_text
                            --        note      
                                    ,sum(balance + deposit)*1.0/(100*1.0) as total_weekly_bonus_hub                
                                                    
                                    from shopeefood.foody_accountant_db__partner_transaction_tab__reg_continuous_s0_live 
                                    where create_time > 1609439493 
                                        and date(from_unixtime(create_time - 60*60)) between date('2021-05-29') and date('2021-11-01')
                                        and txn_type = 505
                                        and note LIKE '%HUB_MODEL%' 
                                                 
                                        group by 1,2--,3 
    
                )hub_weekly on hub_weekly.user_id = bill_fee_2.partner_id  and hub_weekly.year_week_text = bill_fee_2.year_week

    LEFT JOIN (select user_id
                    ,case 
                        when month(date(from_unixtime(create_time - 60*60))) = 1 and substr(trim(note),length(replace(trim(note),' '))-7,2) = '12'
                        then date_parse(concat(substr(replace(trim(note),' '),length(replace(trim(note),' '))-10,5),'/',cast(year(date(from_unixtime(create_time - 60*60))) -1 as varchar)),'%d/%c/%Y')
                        else date_parse(concat(substr(replace(trim(note),' '),length(replace(trim(note),' '))-10,5),'/',cast(year(date(from_unixtime(create_time - 60*60)))as varchar)),'%d/%c/%Y')
                    end as start_date
                    ,case 
                        when month(date(from_unixtime(create_time - 60*60))) = 1 and substr(trim(note),length(trim(note))-1,2) = '12'
                        then date_parse(concat(substr(replace(trim(note),' '),length(replace(trim(note),' '))-4,5),'/',cast(year(date(from_unixtime(create_time - 60*60))) -1 as varchar)),'%d/%c/%Y')
                        else date_parse(concat(substr(replace(trim(note),' '),length(replace(trim(note),' '))-4,5),'/',cast(year(date(from_unixtime(create_time - 60*60)))as varchar)),'%d/%c/%Y')
                    end as end_date
                    ,sum(balance + deposit)*1.0/(100*1.0) as total_weekly_bonus_hub 
                                    
                    from shopeefood.foody_accountant_db__partner_transaction_tab__reg_continuous_s0_live 
                    where create_time > 1609439493 
                        and date(from_unixtime(create_time - 60*60))  > date('2021-11-01')
                        and txn_type = 505
                        and note LIKE '%HUB_MODEL%' 
                                  
                        group by 1,2,3
    
                )hub_weekly_2 on hub_weekly_2.user_id = bill_fee_2.partner_id  and bill_fee_2.date_ between hub_weekly_2.start_date and hub_weekly_2.end_date

    
    -- Adjustment for 2021.02.04        
    LEFT JOIN (select user_id
                    ,case when trim(note) = 'ADJUST_SHIPPING FEE_ 04.02' then date('2021-02-04') 
                          when trim(note) = 'ADJUSTMENT_SHIPPING FEE_11/11/2021' then date('2021-11-11')
                          when trim(note) LIKE '%ADJUSTMENT_SHIPPING FEE_12/12/2021%' then date('2021-12-12')
                          when trim(note) LIKE '%ADJUSTMENT_SHIPPING FEE_PT16%' then date(date_parse(substr(trim(note),-10,10),'%d/%m/%Y'))
                          else null end as date_
                    -- adjustment rate basic / surge: adjust based on fixed basic / order
                    ,SUM(case when txn_type in (565,518) then (balance*1.0000 + deposit*1.0000) else null end)/100 as adjustment
                    ,SUM(case when txn_type in (565,518) and trim(note) = 'ADJUST_SHIPPING FEE_ 04.02' then (balance*1.0000 + deposit*1.0000)*0.8462/100 else 0 end) as adjustment_basic
                    ,SUM(case when txn_type in (565,518) and trim(note) = 'ADJUST_SHIPPING FEE_ 04.02' then (balance*1.0000 + deposit*1.0000)*0.1538/100 
                              else (balance*1.0000 + deposit*1.0000)/100 end) as adjustment_surge
              
                    ,SUM(case when txn_type in (565,518) and trim(note) = 'ADJUST_SHIPPING FEE_ 04.02' and user_id <> 14604057 then (balance*1.0000 + deposit*1.0000)/100 
                              when txn_type in (565,518) and trim(note) = 'ADJUSTMENT_SHIPPING FEE_11/11/2021' then (balance*1.0000 + deposit*1.0000)*0.982587884233491/100
                              when txn_type in (565,518) and trim(note) LIKE '%ADJUSTMENT_SHIPPING FEE_12/12/2021%' then (balance*1.0000 + deposit*1.0000)*0.964658029579302/100  
                              when txn_type in (565,518) and trim(note) LIKE '%ADJUSTMENT_SHIPPING FEE_PT16_Food%' then (balance*1.0000 + deposit*1.0000)/100
                              else 0 end) as adjustment_food

                    ,SUM(case when txn_type in (565,518) and user_id <> 14604057 and trim(note) = 'ADJUST_SHIPPING FEE_ 04.02' then (balance*1.0000 + deposit*1.0000)*0.8462/100 else 0 end) as adjustment_food_basic

                    ,SUM(case when txn_type in (565,518) and user_id <> 14604057 and trim(note) = 'ADJUST_SHIPPING FEE_ 04.02' then (balance*1.0000 + deposit*1.0000)*0.1538/100
                              when txn_type in (565,518) and trim(note) = 'ADJUSTMENT_SHIPPING FEE_11/11/2021' then (balance*1.0000 + deposit*1.0000)*0.982587884233491/100
                              when txn_type in (565,518) and trim(note) LIKE '%ADJUSTMENT_SHIPPING FEE_12/12/2021%' then (balance*1.0000 + deposit*1.0000)*0.964658029579302/100  
                              when txn_type in (565,518) and trim(note) LIKE '%ADJUSTMENT_SHIPPING FEE_PT16_Food%' then (balance*1.0000 + deposit*1.0000)/100
                              else 0 end) as adjustment_food_surge
                    
                    ,SUM(case when txn_type in (565,518) and trim(note) = 'ADJUST_SHIPPING FEE_ 04.02' and user_id = 14604057 then (balance*1.0000 + deposit*1.0000)/100 
                              when txn_type in (565,518) and trim(note) = 'ADJUSTMENT_SHIPPING FEE_11/11/2021' then (balance*1.0000 + deposit*1.0000)*0.017412115766509/100
                              when txn_type in (565,518) and trim(note) LIKE '%ADJUSTMENT_SHIPPING FEE_12/12/2021%' then (balance*1.0000 + deposit*1.0000)*0.035341970420698/100  
                              when txn_type in (565,518) and trim(note) LIKE '%ADJUSTMENT_SHIPPING FEE_PT16_Market%' then (balance*1.0000 + deposit*1.0000)/100
                              else 0 end) as adjustment_market 

                    ,SUM(case when txn_type in (565,518) and user_id = 14604057 and trim(note) = 'ADJUST_SHIPPING FEE_ 04.02' then (balance*1.0000 + deposit*1.0000)*0.8449/100 else 0 end) as adjustment_market_basic

                    ,SUM(case when txn_type in (565,518) and user_id = 14604057 and trim(note) = 'ADJUST_SHIPPING FEE_ 04.02' then (balance*1.0000 + deposit*1.0000)*0.1551/100
                              when txn_type in (565,518) and trim(note) = 'ADJUSTMENT_SHIPPING FEE_11/11/2021' then (balance*1.0000 + deposit*1.0000)*0.017412115766509/100
                              when txn_type in (565,518) and trim(note) LIKE '%ADJUSTMENT_SHIPPING FEE_12/12/2021%' then (balance*1.0000 + deposit*1.0000)*0.035341970420698/100  
                              when txn_type in (565,518) and trim(note) LIKE '%ADJUSTMENT_SHIPPING FEE_PT16_Market%' then (balance*1.0000 + deposit*1.0000)/100               
                              else 0 end) as adjustment_market_surge
                    
                    from shopeefood.foody_accountant_db__partner_transaction_tab__reg_continuous_s0_live 
                    
                    where 1=1  
                    and txn_type in (565,518)
                    and (trim(note) = 'ADJUST_SHIPPING FEE_ 04.02'
                         or trim(note) = 'ADJUSTMENT_SHIPPING FEE_11/11/2021'
                         or trim(note) LIKE '%ADJUSTMENT_SHIPPING FEE_12/12/2021%'
                         or trim(note) LIKE '%ADJUSTMENT_SHIPPING FEE_PT16%'
                        )
                    and user_id not in (8938858) -- admin account Dong Nai
                    
                    group by 1,2
            )adj on adj.user_id = bill_fee_2.partner_id 
                        and adj.date_ = bill_fee_2.date_ and coalesce(bill_fee_2.total_bill_hub,0) = 0

)all

where 1=1
and all.date_ >= date('2021-06-01')
and all.date_ < date(current_date)
--and city_name = 'HCM'
)

where 1=1