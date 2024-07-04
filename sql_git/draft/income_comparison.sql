with base as 
(select 
        dot.uid
       ,dot.ref_order_code
       ,dot.delivery_distance*1.00/1000 as distance 
       ,date(from_unixtime(dot.real_drop_time - 3600)) as report_date 
       ,extract(hour from from_unixtime(dot.real_drop_time - 3600)) as hour_ 
       ,sm.city_name
       ,sm.shipper_name
       ,current.current_driver_tier
       ,case when sm.shipper_type_id = 12 then 'Hub' else 'Non Hub' end as shipper_type
       ,case when doet.driver_payment_policy = 2 then 1 else 0 end as is_inshift 

from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 

--Check hub
left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = dot.uid and try_cast(sm.grass_date as date) = date(from_unixtime(dot.real_drop_time - 3600))

--Order policy
left join 
    (SELECT order_id
        ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
        ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)
        -- ,order_data
    from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet

    )doet on dot.id = doet.order_id

--- Driver tier
left join 
        (SELECT cast(from_unixtime(bonus.report_date - 60*60) as date) as report_date
        ,bonus.uid as shipper_id
        ,case when hub.shipper_type_id = 12 then 'Hub'
        when bonus.tier in (1,6,11) then 'T1' when bonus.tier in (2,7,12) then 'T2'
        when bonus.tier in (3,8,13) then 'T3'
        when bonus.tier in (4,9,14) then 'T4'
        when bonus.tier in (5,10,15) then 'T5'
        else null end as current_driver_tier
        ,bonus.total_point
        ,bonus.daily_point

        FROM shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus
        LEFT JOIN
        (SELECT shipper_id
        ,shipper_type_id
        ,case when grass_date = 'current' then date(current_date)
        else cast(grass_date as date) end as report_date

        from shopeefood.foody_mart__profile_shipper_master

        where 1=1
        and (grass_date = 'current' OR cast(grass_date as date) >= date('2019-01-01'))
        GROUP BY 1,2,3
        )hub on hub.shipper_id = bonus.uid and hub.report_date = cast(from_unixtime(bonus.report_date - 60*60) as date)

        -- where cast(from_unixtime(bonus.report_date - 60*60) as date) = date(current_date) - interval '1' day
        )current on current.shipper_id = dot.uid and current.report_date = date(from_unixtime(dot.real_drop_time - 3600))

where date(from_unixtime(dot.real_drop_time - 3600)) between current_date - interval '45' day and current_date - interval '1' day

and order_status = 400

)

,base2 as 
(select
       a.report_date
       ,year(a.report_date)*100+ week(a.report_date) as week_
    --   ,hour_  
      ,a.uid
      ,a.shipper_name 
      ,coalesce(a.current_driver_tier,a.shipper_type) as tier 
    --   ,case when a.city_name in ('HCM City','Ha Noi City') then a.current_driver_tier else a.shipper_type end as tier
      ,a.shipper_type
      ,a.city_name
      ,case when slot.uid is not null and a.shipper_type = 'Hub' and slot.registration_status != 2 
            then concat(cast((slot.end_time - slot.start_time)/3600 as varchar),'-','hour shift') 
                 else 'part-time' end as shift_hour
      ,total_earning_before_tax
      ,total_earning_hub
      ,total_earning_non_hub
      ,case when a.shipper_type = 'Hub' and slot.uid is not null and slot.registration_status != 2 then hub.in_shift_online_time
            else pt.total_online_time end as online_time 
      ,case when a.shipper_type = 'Hub' and slot.uid is not null and slot.registration_status != 2 then hub.in_shift_work_time
            else pt.total_working_time end as working_time 
      ,count(distinct ref_order_code) as total_order
      ,count(distinct case when is_inshift = 0 then a.ref_order_code else null end) as total_non_hub_order
      ,count(distinct case when is_inshift = 1 then a.ref_order_code else null end) as total_hub_order
      ,sum(a.distance) as total_distance 
      ,sum(case when is_inshift = 0 then a.distance else null end ) as toal_distance_non_hub_order
      ,sum(case when is_inshift = 1 then a.distance else null end ) as toal_distance_hub_order
      


from base a

left join shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live slot on slot.uid = a.uid and date(from_unixtime(slot.date_ts - 3600)) = a.report_date

left join (select date_
                 ,partner_id
                 ,sum(total_earning_before_tax) as total_earning_before_tax 
                 ,sum(total_earning_hub) as total_earning_hub
                 ,sum(total_earning_non_hub) as total_earning_non_hub
                 
                 
                 from vnfdbi_opsndrivers.snp_foody_shipper_income_tab  
group by 1,2

) inc on inc.partner_id = a.uid and inc.date_ = a.report_date

left join vnfdbi_opsndrivers.snp_foody_hub_driver_report_tab hub on hub.shipper_id = a.uid and hub.report_date = a.report_date

left join vnfdbi_opsndrivers.snp_foody_shipper_daily_report pt on pt.shipper_id = a.uid and pt.report_date = a.report_date


where 1 = 1 
-- and a.report_date between current_date - interval '1' day and current_date - interval '1' day


group by 1,2,3,4,5,6,7,8,9,10,11,12,13

)
-- ,week_view as 
-- (select  week_ 
--         ,city_name 
--         ,tier 
--         ,shift_hour
--         ,sum(working_day)*1.00/count(distinct uid) as avg_workingday
    
--     from
--     (select week_ 
--        ,city_name 
--        ,tier
--        ,shift_hour
--        ,uid
--        ,count(distinct report_date) as working_day
--        from base2 
--        group by 1,2,3,4,5)
-- group by 1,2,3,4



-- )

select   
-- * from week_view where city_name in ('HCM City','Ha Noi City')
         report_date
        --  week_ 
        ,city_name
        ,tier
        ,shift_hour
        -- driver performance
        ,sum(online_time)*1.00/count(distinct uid) as online_time
        ,sum(working_time)*1.00/count(distinct uid) as working_time
        ,sum(total_order)*1.00/count(distinct uid) as total_order 
        ,sum(total_non_hub_order)*1.00/count(distinct uid) as total_non_hub_order 
        ,sum(total_hub_order)*1.00/count(distinct uid) as total_hub_order
        -- driver distance
        ,sum(total_distance)*1.00 as total_distance 
        ,sum(toal_distance_hub_order)*1.00 as total_distance_hub
        ,sum(toal_distance_non_hub_order)*1.00 as total_distance_non_hub 
        -- driver income
        ,sum(total_earning_before_tax)*1.00/count(distinct uid) as total_earning_before_tax
        ,sum(total_earning_hub)*1.00/count(distinct uid) as total_earning_hub
        ,sum(total_earning_non_hub)*1.00/count(distinct uid) as total_earning_non_hub
        ---distance per orders
        ,sum(total_distance)*1.00/sum(total_order) as avg_distance_order
        ,sum(toal_distance_hub_order)*1.00/sum(total_hub_order) as avg_distance_order_hub
        ,sum(toal_distance_non_hub_order)*1.00/sum(total_non_hub_order) as avg_distance_order_non_hub
        -- total driver 
        ,count(distinct uid ) as total_driver
        -- ,count(distinct case when shipper_type != 'part-time' then uid else null end) as total_driver_hub 
        -- ,count(distinct case when shipper_type = 'part-time' then uid else null end) as total_driver_non_hub 
        -- working_day 
        -- ,count(distinct report_date)*1.00


       






from base2 


where 1 = 1 

and report_date between current_date - interval '30' day and current_date - interval '1' day

and city_name in ('HCM City','Ha Noi City')

-- and current_driver_tier is null 

group by 1,2,3,4

order by report_date desc 














