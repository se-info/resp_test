with driver_performance as 
(select 
         uid 
        ,date(from_unixtime(report_date - 3600)) as date_ 
        ,total_online_seconds/cast(3600 as double) as online_hour 
        ,total_work_seconds/cast(3600 as double) as working_hour
        ,total_work_distance/cast(1000 as double) as working_distance 



from shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live
order by report_date desc
)
,driver_order as 
(
SELECT 
  date(from_unixtime(dot.real_drop_time - 3600)) as report_date
, dot.uid
-- ,dot.ref_order_code
-- ,delivery_cost/100
, count(distinct dot.ref_order_code) as total_order
, count(distinct case when ref_order_category = 0 then dot.ref_order_code else null end) as total_order_delivery
, count(distinct case when ref_order_category <> 0 then dot.ref_order_code else null end) as total_order_spxi
, sum(delivery_cost)/cast(100 as double) as total_shipping_fee

FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
where dot.order_status = 400
and date(from_unixtime(dot.real_drop_time - 3600)) >= date((current_date) - interval '180' day)

group by 1,2
)

select 
        do.report_date
       ,do.uid as shipper_id
       ,pf.shopee_uid
       ,tier.current_driver_tier as tier_
       ,date(from_unixtime(pf.create_time - 3600)) as onboard_date
       ,dp.online_hour
       ,dp.working_hour
       ,dp.working_distance
       ,do.total_order
       ,do.total_order_delivery
       ,do.total_order_spxi
       ,do.total_shipping_fee
       ,tier.daily_bonus
    --    ,ic.income as total_income


from driver_order do

LEFT JOIN shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live pf on pf.uid = do.uid 

LEFT JOIN
(
SELECT date(from_unixtime(bonus.report_date - 60*60)) as report_date
,bonus.uid as shipper_id
,case when hub.shipper_type_id = 12 then 'Hub'
when bonus.tier in (1,6,11) then 'T1' when bonus.tier in (2,7,12) then 'T2'
when bonus.tier in (3,8,13) then 'T3'
when bonus.tier in (4,9,14) then 'T4'
when bonus.tier in (5,10,15) then 'T5'
else null end as current_driver_tier
,bonus.total_point
,bonus.daily_point
,bonus.bonus_value/cast(100 as double) as daily_bonus

FROM shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus

LEFT JOIN shopeefood.foody_mart__profile_shipper_master hub on hub.shipper_id = bonus.uid and try_cast(hub.grass_date as date) = date(from_unixtime(bonus.report_date - 60*60))
order by report_date desc 
) tier on tier.shipper_id = do.uid and tier.report_date = do.report_date

-- left join dev_vnfdbi_opsndrivers.shopeefood_vn_food_accountant_driver_order_daily_income_tab ic on ic.driver_id = do.uid and ic.grass_date = do.report_date

left join driver_performance dp on dp.uid = do.uid and dp.date_ = do.report_date


where 1 = 1 
and 
    (
    do.report_date = date'2022-09-09'
     or 
    do.report_date = date'2022-08-08'   
    )
