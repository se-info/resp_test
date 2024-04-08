WITH bill_fee AS 
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
    ---- add distance range
    ,CASE when distance <= 1 then '0-1 km'
        when distance <= 2 then '1-2 km'
        when distance <= 3 then '2-3 km'
        when distance <= 4 then '3-4 km'
        when distance <= 5 then '4-5 km'
        when distance <= 6 then '5-6 km'
        when distance <= 7 then '6-7 km'
        when distance <= 8 then '7-8 km'
        when distance <= 9 then '8-9 km'
        when distance <= 10 then '9-10 km'
        when distance <= 11 then '10-11 km'
        when distance <= 12 then '11-12 km'
        when distance <= 13 then '12-13 km'
        when distance <= 14 then '13-14 km'
        when distance <= 15 then '14-15 km'
        ELSE '15++ km' end as distance_range
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
    ,raw2.is_qualified_hub
    ,raw2.delivered_by
    ,coalesce(raw2.total_bill,0) as total_bill
    ,coalesce(raw2.total_bill_hub,0) as total_bill_hub
    ,coalesce(raw2.total_shipping_fee_collected_from_customer,0) as total_shipping_fee_collected_from_customer
    ,coalesce(raw2.shipping_fee_share,0) + coalesce(raw2.bad_weather_fee_temp_non_hub,0) + coalesce(raw2.late_night_fee_temp_non_hub,0) + coalesce(raw2.holiday_fee_temp_non_hub,0)  as shipping_fee_share -- add bad weather fee
    ,coalesce(raw2.return_fee_share,0) as return_fee_share
    ,coalesce(raw2.additional_bonus,0) as additional_bonus
    ,coalesce(raw2.order_completed_bonus,0) as order_completed_bonus
    ,coalesce(raw2.other_payables,0) as other_payables
    ,case when raw2.total_shipping_fee = 0 then 0
        else (coalesce(raw2.shipping_fee_share,0)*1.000000/raw2.total_shipping_fee)*raw2.total_shipping_fee_basic end as shipping_fee_share_basic 
    ,case when raw2.total_shipping_fee = 0 then 0
        else (coalesce(raw2.shipping_fee_share,0)*1.000000/raw2.total_shipping_fee) *raw2.total_shipping_fee_surge + coalesce(raw2.bad_weather_fee_temp,0) + coalesce(raw2.late_night_fee_temp_non_hub,0) +  coalesce(raw2.holiday_fee_temp_non_hub,0)  end as shipping_fee_share_surge  -- add bad weather fee/late night/ holiday fee into surge
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
    ,city.city_name as city_name_full
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
    ,raw.is_qualified_hub
    ,case when raw.partner_type = 12 and coalesce(raw.driver_payment_policy,0) <> 3 then 'hub' else 'non-hub' end as delivered_by
    ,count(DISTINCT raw.order_id) as total_bill
    ,count(distinct case when raw.partner_type = 12 and coalesce(raw.driver_payment_policy,0) <> 3 then raw.order_id else null end) as total_bill_hub
    ,sum(raw.total_shipping_fee_collected_from_customer) as total_shipping_fee_collected_from_customer
    ,SUM(case when trx.txn_type in (201,301,401,104,1000,2001,2101,3000) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as shipping_fee_share
    ,SUM(case when trx.txn_type in (202,302,402,1001,2002,2102,3001) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as return_fee_share
    ,SUM(case when trx.txn_type in (204,304,404,105,1003,2004,2106,3003) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as additional_bonus
    ,SUM(case when trx.txn_type in (200,300,400,101,1006,2000,2100,3006) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as order_completed_bonus
    ,SUM(case when trx.txn_type in (203,303,403,106,2003,2005,2006,2007,2105,2104,3002,3005,3007) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as other_payables
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
                ,case when o.hub_id > 0 or (o.partner_type = 12 and coalesce(o.driver_payment_policy,0) <> 3) then 1 else 0 end as is_qualified_hub

                from        
                        (--EXPLAIN ANALYZE
                        -- Food / Market
                        select  distinct ad_odt.order_id,ad_odt.partner_id
                            ,case when ad_odt.city_id = 217 then 'HCM'
                                  when ad_odt.city_id = 218 then 'HN'
                                  when ad_odt.city_id = 219 then 'DN'
                                  else 'OTH' end as city_name
                            ,ad_odt.city_id
                            ,cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time)  - 60*60) as date) as date_
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
                                  when cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time)  - 60*60) as date) >= date('2021-02-01') then GREATEST(13500,coalesce(dotet.unit_fee,4500)*oct.distance)  -- change setting    
                                  else GREATEST(15000,coalesce(dotet.unit_fee,5000)*oct.distance) 
                                  end as total_shipping_fee_basic
                            
                                  
                            ,case when oct.status = 9 then 0
                                when cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time)  - 60*60) as date) >= date('2021-02-01') then GREATEST( dotet.total_shipping_fee - coalesce(GREATEST(13500,coalesce(dotet.unit_fee,4500)*oct.distance),0) ,0)
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
                            ,COALESCE(dotet.hub_id, 0) as hub_id

                        from shopeefood.foody_accountant_db__order_delivery_tab__reg_daily_s0_live ad_odt
                        left join (SELECT id,submit_time,foody_service_id,distance,status,total_shipping_fee,extra_data
                                        ,coalesce(cast(json_extract(oct.extra_data,'$.bad_weather_fee.user_pay_amount') as decimal),0) as user_bwf
                        
                                    from shopeefood.foody_order_db__order_completed_tab__reg_daily_s0_live oct
                                    where submit_time > 1609439493
                                    )oct on oct.id = ad_odt.order_id and oct.submit_time > 1609439493
                        
                                    
                            left JOIN shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot on dot.ref_order_id = oct.id and dot.ref_order_category = 0 and dot.submitted_time > 1609439493
                            left join (SELECT order_id
                                            ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
                                            ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)
                                            ,cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ) as hub_id
                                        from shopeefood.foody_partner_db__driver_order_extra_tab__reg_daily_s0_live dotet
                                        
                                        )dotet on dot.id = dotet.order_id
                        
                        where cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time)  - 60*60) as date) >= date('2020-12-31')  -- date(current_date) - interval '75' day
                        and cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time)  - 60*60) as date) <= date(current_date)
                        and ad_odt.partner_id > 0 
                        
                        union all
                        
                      --  EXPLAIN ANALYZE
                        -- NS User = NS Instant
                        select  distinct ad_ns.order_id,ad_ns.partner_id --,dot.ref_order_code
                            ,case when ad_ns.city_id = 217 then 'HCM'
                                  when ad_ns.city_id = 218 then 'HN'
                                  when ad_ns.city_id = 219 then 'DN'
                                  else 'OTH' end as city_name
                            ,ad_ns.city_id  
                            ,cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 60*60) as date) as date_
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
                            
                            ,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 60*60) as date) >= date('2021-02-01') then GREATEST(13500,coalesce(dotet.unit_fee,4050)*(ebt.distance*1.00/1000))
                                else GREATEST(15000,coalesce(dotet.unit_fee,5000)*(ebt.distance*1.00/1000))
                                end as total_shipping_fee_basic
                                
                            ,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 60*60) as date) >=  date('2021-02-01') then 
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
                                  when prm.code LIKE '%SPXINSTANT%' then eojd.foody_discount_amount      
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
                            ,COALESCE(dotet.hub_id, 0) as hub_id
                                    
                        from shopeefood.foody_accountant_db__order_now_ship_user_tab__reg_daily_s0_live ad_ns
                        Left join shopeefood.foody_express_db__booking_tab__reg_daily_s0_live ebt on ebt.id = ad_ns.order_id and ebt.create_time > 1609439493 
                        left join 
                                 (SELECT id,create_timestamp,delivery_cost_amount,foody_discount_amount,shipping_return_fee
                                  FROM shopeefood.foody_mart__fact_express_order_join_detail
                                  
                                  WHERE grass_region = 'VN'
                                 )eojd on eojd.id = ebt.id and eojd.create_timestamp > 1609439493 
                        left join shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot on ebt.id = dot.ref_order_id and dot.ref_order_category = 4 and dot.submitted_time > 1609439493
                        
                        left join (SELECT order_id
                                            ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
                                            ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)
                                            ,cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ) as hub_id
                                        from shopeefood.foody_partner_db__driver_order_extra_tab__reg_daily_s0_live dotet
                                        
                                        )dotet on dot.id = dotet.order_id
                                        
                        left join shopeefood.foody_express_db__promotion_tab__reg_daily_s0_live prm on ebt.promotion_code_id = prm.id
                        
                        where cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 60*60) as date) >= date('2020-12-31') -- date(current_date) - interval '75' day
                        and cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 60*60) as date) <= date(current_date)
                        and ad_ns.partner_id > 0
                        
                        union all
                        
                    --    EXPLAIN ANALYZE
                        -- NS Merchant = NS Food Merchant
                        select  distinct ad_ns.order_id,ad_ns.partner_id
                            ,case when ad_ns.city_id = 217 then 'HCM'
                                  when ad_ns.city_id = 218 then 'HN'
                                  when ad_ns.city_id = 219 then 'DN'
                                  else 'OTH' end as city_name
                            ,ad_ns.city_id    
                            ,cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 60*60) as date) as date_
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
                            
                            ,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 60*60) as date) >= date('2021-02-01') then GREATEST(13500,coalesce(dotet.unit_fee,4050)*(ebt.distance*1.00/1000))
                                else GREATEST(15000,coalesce(dotet.unit_fee,5000)*(ebt.distance*1.00/1000))
                                end as total_shipping_fee_basic
                                
                            ,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 60*60) as date) >=  date('2021-02-01') then 
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
                            ,COALESCE(dotet.hub_id, 0) as hub_id
                                    
                        from shopeefood.foody_accountant_db__order_now_ship_merchant_tab__reg_daily_s0_live ad_ns
                        Left join shopeefood.foody_express_db__booking_tab__reg_daily_s0_live ebt on ebt.id = ad_ns.order_id and ebt.create_time > 1609439493
                        left join 
                                 (SELECT id,create_timestamp,delivery_cost_amount,foody_discount_amount,shipping_return_fee
                                  FROM shopeefood.foody_mart__fact_express_order_join_detail
                                  
                                  WHERE grass_region = 'VN'
                                 )eojd on eojd.id = ebt.id and eojd.create_timestamp > 1609439493
                        left join shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot on ebt.id = dot.ref_order_id and dot.ref_order_category = 5 and dot.submitted_time > 1609439493
                        
                        left join (SELECT order_id
                                            ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
                                            ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)
                                            ,cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ) as hub_id
                                        from shopeefood.foody_partner_db__driver_order_extra_tab__reg_daily_s0_live dotet
                                        
                                        )dotet on dot.id = dotet.order_id
                                        
                        left join shopeefood.foody_express_db__promotion_tab__reg_daily_s0_live prm on ebt.promotion_code_id = prm.id
                        
                        where cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 60*60) as date) >= date('2020-12-31') -- date(current_date) - interval '75' day
                        and cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 60*60) as date) <= date(current_date)
                        and ad_ns.partner_id > 0
                        
                        union all
                        
                    --    EXPLAIN ANALYZE
                        -- NS Shopee
                        select  distinct ad_nss.order_id,ad_nss.partner_id
                            ,case when ad_nss.city_id = 217 then 'HCM'
                                  when ad_nss.city_id = 218 then 'HN'
                                  when ad_nss.city_id = 219 then 'DN'
                                  else 'OTH' end as city_name
                            ,ad_nss.city_id      
                            ,cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) as date_
                            ,CASE WHEN WEEK(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60)) >= 52 AND MONTH(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60)) = 1 THEN (YEAR(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60))-1)*100 + WEEK(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60)) 
                                    ELSE CAST(DATE_FORMAT(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60), '%x%v') AS BIGINT) END as year_week
                            ,ad_nss.partner_type
                            ,'Now Ship Shopee' as source
                            ,'Now Ship Shopee' as sub_source
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

                            , CASE 
                                -- exclusive case, apply on city level
                                WHEN cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) BETWEEN date('2021-10-01') AND date('2021-10-12') and ad_nss.city_id = 217 then 
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

                                WHEN cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) BETWEEN date('2021-09-30') AND date('2021-10-12') and ad_nss.city_id != 217 then 
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

                                WHEN cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) BETWEEN date('2021-09-29') AND date('2021-10-12') and ad_nss.city_id = 218 then 
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
                                -- else, apply nationwide
                                WHEN esbt.status = 11 THEN sp_unit.fix_price + sp_unit.unit_fee*(ceiling(esbt.distance*1.000/1000) - sp_unit.distance_subtract)
                                WHEN esbt.status = 14 THEN (sp_unit.fix_price + sp_unit.unit_fee*(ceiling(esbt.distance*1.000/1000) - sp_unit.distance_subtract))*1.5 ELSE NULL 
                                END AS rev_shipping_fee
                            -- ,sp_unit.fix_price
                            -- ,sp_unit.distance_subtract
                            -- ,sp_unit.unit_fee
                            ,0 as prm_cost
                            ,0 as rev_cod_fee
                            ,0 as rev_return_fee
                            
                            -- hub order 
                            ,coalesce(dotet.driver_payment_policy,1) as driver_payment_policy  -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)
                            ,COALESCE(dotet.hub_id, 0) as hub_id

                        from shopeefood.foody_accountant_db__order_now_ship_shopee_tab__reg_daily_s0_live ad_nss
                        Left join shopeefood.foody_express_db__shopee_booking_tab__reg_daily_s0_live esbt on esbt.id = ad_nss.order_id and esbt.create_time > 1609439493
                        left join shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot on esbt.id = dot.ref_order_id and dot.ref_order_category = 6 and dot.submitted_time > 1609439493

                        left join (SELECT order_id
                                            ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
                                            ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)
                                            ,cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ) as hub_id
                                        from shopeefood.foody_partner_db__driver_order_extra_tab__reg_daily_s0_live dotet
                                        
                                        )dotet on dot.id = dotet.order_id

                        LEFT JOIN (
                                    SELECT CAST(distance_grp AS INT) distance_grp, 
                                        CAST(distance AS INT) distance,
                                        DATE(start_date) start_date,
                                        DATE(end_date) end_date,
                                        CAST(unit_fee as DOUBLE) unit_fee,
                                        CAST(fix_price AS DOUBLE) fix_price,
                                        CAST(distance_subtract AS DOUBLE) distance_subtract
                                    FROM vnfdbi_opsndrivers.bpn_shipping_fee_unit_ingest
                                )sp_unit ON cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) BETWEEN sp_unit.start_date AND sp_unit.end_date
                                AND (ceiling(esbt.distance *1.000 / 1000) = sp_unit.distance OR (CASE WHEN ceiling(esbt.distance *1.000 / 1000) >= 21 THEN 20 ELSE NULL END) = sp_unit.distance_grp)

                        where cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >= DATE'2021-01-01'  -- ate(current_date) - interval '75' day
                        and cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) <= date(current_date)
                        and ad_nss.partner_id > 0
                        and booking_type = 4

                    UNION all  
                       --    EXPLAIN ANALYZE
                        -- SPX Portal
                        select  distinct ad_nss.order_id,ad_nss.partner_id
                            ,case when ad_nss.city_id = 217 then 'HCM'
                                  when ad_nss.city_id = 218 then 'HN'
                                  when ad_nss.city_id = 219 then 'DN'
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
                            ,COALESCE(dotet.hub_id, 0) as hub_id
                                
                        from shopeefood.foody_accountant_db__order_now_ship_shopee_tab__reg_daily_s0_live ad_nss
                        Left join shopeefood.foody_express_db__shopee_booking_tab__reg_daily_s0_live esbt on esbt.id = ad_nss.order_id and esbt.create_time > 1609439493
                        left join shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot on esbt.id = dot.ref_order_id and dot.ref_order_category = 6 and dot.submitted_time > 1609439493
                        
                        left join (SELECT order_id
                                            ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
                                            ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)
                                            ,cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ) as hub_id
                                        from shopeefood.foody_partner_db__driver_order_extra_tab__reg_daily_s0_live dotet
                                        
                                        )dotet on dot.id = dotet.order_id
                        
                        
                        where cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >=  date('2020-12-31') -- ate(current_date) - interval '75' day
                        and cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) <= date(current_date)
                        and ad_nss.partner_id > 0
                        and booking_type = 5
            
        UNION ALL
                --    EXPLAIN ANALYZE
                    -- NS Same Day 
                        SELECT distinct ad_ns.order_id,ad_ns.partner_id
                            ,case when ad_ns.city_id = 217 then 'HCM'
                                  when ad_ns.city_id = 218 then 'HN'
                                  when ad_ns.city_id = 219 then 'DN'
                                  else 'OTH' end as city_name
                            ,ad_ns.city_id
                            ,cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 60*60) as date) as date_
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
                            
                            ,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 60*60) as date) >= date('2021-02-01') then GREATEST(13500,coalesce(dotet.unit_fee,4050)*(ebt.distance*1.00/1000))
                                else GREATEST(15000,coalesce(dotet.unit_fee,5000)*(ebt.distance*1.00/1000))
                                end as total_shipping_fee_basic
                                
                            ,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 60*60) as date) >=  date('2021-02-01') then 
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
                            ,COALESCE(dotet.hub_id, 0) as hub_id
                    
                        from shopeefood.foody_accountant_db__order_now_ship_sameday_tab__reg_daily_s0_live ad_ns
                        Left join shopeefood.foody_express_db__booking_tab__reg_daily_s0_live ebt on ebt.id = ad_ns.order_id and ebt.create_time > 1609439493
                        left join 
                                 (SELECT id,create_timestamp,delivery_cost_amount,foody_discount_amount,shipping_return_fee
                                  FROM shopeefood.foody_mart__fact_express_order_join_detail
                                  
                                  WHERE grass_region = 'VN'
                                 )eojd on eojd.id = ebt.id and eojd.create_timestamp > 1609439493
                        left join shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot on ebt.id = dot.ref_order_id and dot.ref_order_category = 7 and dot.submitted_time > 1609439493
                        
                        left join (SELECT order_id
                                            ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
                                            ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)
                                            ,cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ) as hub_id
                                        from shopeefood.foody_partner_db__driver_order_extra_tab__reg_daily_s0_live dotet
                                        
                                        )dotet on dot.id = dotet.order_id
                                        
                        left join shopeefood.foody_express_db__promotion_tab__reg_daily_s0_live prm on ebt.promotion_code_id = prm.id
                        
                        where cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 60*60) as date) >= date('2020-12-31') -- date(current_date) - interval '75' day
                        and cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 60*60) as date) <= date(current_date)
                        and ad_ns.partner_id > 0
                     
                    --    limit 100
                        
                    UNION all   
                        
                    -- NS Multi Drop 
                    
                    select  distinct ad_ns.order_id,ad_ns.partner_id -- ,dot.ref_order_code,ebt.id  as ebt_id, eojd.id  as eojd_id
                        ,case when ad_ns.city_id = 217 then 'HCM'
                              when ad_ns.city_id = 218 then 'HN'
                              when ad_ns.city_id = 219 then 'DN'
                              else 'OTH' end as city_name
                        ,ad_ns.city_id  
                        ,cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 60*60) as date) as date_
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
                        
                        ,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 60*60) as date) >= date('2021-02-01') then GREATEST(13500,coalesce(dotet.unit_fee,4050)*(ebt.distance*1.00/1000))
                            else GREATEST(15000,coalesce(dotet.unit_fee,5000)*(ebt.distance*1.00/1000))
                            end as total_shipping_fee_basic
                            
                        ,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 60*60) as date) >=  date('2021-02-01') then 
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
                        ,COALESCE(dotet.hub_id, 0) as hub_id
                                
                    from shopeefood.foody_accountant_db__order_now_ship_multi_drop_tab__reg_daily_s0_live ad_ns
                    Left join shopeefood.foody_express_db__booking_tab__reg_daily_s0_live ebt on ebt.id = ad_ns.order_id and ebt.create_time > 1609439493 
                    
                    left join shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot on ebt.id = dot.ref_order_id and dot.ref_order_category = 8 and dot.submitted_time > 1609439493

                    left join (SELECT order_id
                                        ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
                                        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
                                        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
                                        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
                                        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
                                        ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)
                                        ,cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ) as hub_id
                                    from shopeefood.foody_partner_db__driver_order_extra_tab__reg_daily_s0_live dotet
                                    
                                    )dotet on dot.id = dotet.order_id
                                    
                    left join shopeefood.foody_express_db__promotion_tab__reg_daily_s0_live prm on ebt.promotion_code_id = prm.id

                    where cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 60*60) as date) >= date('2021-01-01') -- date(current_date) - interval '75' day
                    and cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 60*60) as date) <= date(current_date)
                    and ad_ns.partner_id > 0
                    -- and dot.ref_order_code = '210709SE2955'

                    --limit 1000
                    
                    )o
                    
                    -- take drivers' total point / tier of that day
                LEFT JOIN (SELECT cast(from_unixtime(bonus.report_date - 60*60) as date) as report_date
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
                    
            --        limit 100
                    
                  --  limit 1000
                  --  LEFT JOIN foody.foody_order_db__order_completed_tab oct on o.order_id = oct.id
                    
                )temp
                 

                
    -- limit 1000
            )raw
            
    -- city name full        
    left join (SELECT city_id
                    ,city_name
                    
                    from shopeefood.foody_mart__fact_gross_order_join_detail
                    where from_unixtime(create_timestamp) between date(cast(now() - interval '30' day as TIMESTAMP)) and date(cast(now() - interval '1' hour as TIMESTAMP)) 
                    and grass_region = 'VN'
                    GROUP BY city_id
                    ,city_name
                   )city on city.city_id = raw.city_id

    -- transaction tbl --> calculate fee
    left join (SELECT reference_id
                    ,txn_type
                    ,balance
                    ,deposit
                    ,case when cast(from_unixtime(create_time,7,0) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
                            when cast(from_unixtime(create_time,7,0) as date) between DATE('2020-12-28') and DATE('2021-01-03') then 202053
                            when cast(from_unixtime(create_time,7,0) as date) between DATE('2022-01-01') and DATE('2022-01-02') then 202152
                            else YEAR(cast(from_unixtime(create_time,7,0) as date))*100 + WEEK(cast(from_unixtime(create_time,7,0) as date)) end as year_week
                    ,date(from_unixtime(create_time - 60*60)) as created_date        
                    ,user_id
    
                from shopeefood.foody_accountant_db__partner_transaction_tab__reg_daily_s0_live
                
                where create_time > 1609439493 
                -- and cast(from_unixtime(create_time,7,0) as date) >= date('2019-12-01') -- date(current_date) - interval '78' day
                -- and cast(from_unixtime(create_time,7,0) as date) <= date(current_date)
                
                and txn_type in (-- TYPE: BONUS, RECEIVED SHIPPING FEE, ADDITIONAL BONUS, OTHER PAYABLES (parking fee), RETURN FEE SHARED
                                          200,201,204,203,202, -- Now Ship User
                                          300,301,304,303,302, -- Now Ship Merchant
                                          400,401,404,403,402, -- Now Moto    
                                          
                                          101,104,105,106,      -- Delivery Service, consider 105 DELIVERY_ADD_BONUS_MANUAL
                                          1006,1000,1003,1001,  -- Now Ship Shopee: 1000: recevied shipping fee, 1001: return fee shared, 1003: bonus from CS, 1006: bonus for FT driver
                                          2000,2001,2004,2003,2002,2005,2006,2007, -- Sameday
                                          2100,2101,2104,2105,2106,2102, -- multidrop
                                          3000,3001,3004,3003,3002,3005,3006,3007, -- SPX Portal
                                          112,115, -- bad weather fee 
                                          117, -- holiday fee passthrough
                                          119 -- late night fee passthrough  

                                )   
            --    and reference_id = 182853798 -- 183461946                 
                
                )trx on trx.reference_id = raw.order_id 
                    and trx.user_id = raw.partner_id -- user_id = partner_id = shipper_id
                    and trx.created_date >= raw.date_ - interval '2' day and trx.created_date <= raw.date_ + interval '2' day      -- map by order Id --> more details than shipper_id
                                
        
   
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31

    --        limit 1000
    )raw2
-- WHERE date_ >= DATE'2022-01-01' -- input date
WHERE date_ >= DATE_TRUNC('month', current_date ) - INTERVAL'1' month -- change here
AND date_ < current_date 
)

,bonus_instant as (
Select distinct 
    -- date_, 
    DATE_FORMAT(date_, '%Y-%m') month_,
    AVG(exchange_rate) exchange_rate,
    (
case 
    when sum(total_bill_non_hub_now_ship_instant) = 0 then 0
    when sum(total_bill_non_hub) = 0 then 0
    else
    case when sum(case
    when city_name = 'HCM' then total_bill_non_hub
    else 0 end
    ) = 0 then 0 else
    (sum(case
    when city_name = 'HCM' then total_bonus_before_tax_combined * 1.000
    else 0 end
    ) * 1.000/sum(case
    when city_name = 'HCM' then total_bill_non_hub
    else 0 end
    ) * (-1)) * (sum(case when city_name = 'HCM' then total_bill_non_hub_now_ship_instant else null end) * 1.000/sum(total_bill_non_hub_now_ship_instant))
    end
    +
    case when sum(case
    when city_name = 'HN' then total_bill_non_hub
    else 0 end
    ) = 0 then 0 else
    (sum(case
    when city_name = 'HN' then total_bonus_before_tax_combined * 1.000
    else 0 end
    ) * 1.000/sum(case
    when city_name = 'HN' then total_bill_non_hub
    else 0 end
    ) * (-1)) * (sum(case when city_name = 'HN' then total_bill_non_hub_now_ship_instant else null end) * 1.000/sum(total_bill_non_hub_now_ship_instant))
    end
    +
    case when sum(case
    when city_name = 'DN' then total_bill_non_hub
    else 0 end
    ) = 0 then 0 else
    (sum(case
    when city_name = 'DN' then total_bonus_before_tax_combined * 1.000
    else 0 end
    ) * 1.000/sum(case
    when city_name = 'DN' then total_bill_non_hub
    else 0 end
    ) * (-1)) * (sum(case when city_name = 'DN' then total_bill_non_hub_now_ship_instant else null end) * 1.000/sum(total_bill_non_hub_now_ship_instant))
    end
    +
    case when sum(case
    when city_name = 'OTH' then total_bill_non_hub
    else 0 end
    ) = 0 then 0 else
    (sum(case
    when city_name = 'OTH' then total_bonus_before_tax_combined * 1.000
    else 0 end
    ) * 1.000/sum(case
    when city_name = 'OTH' then total_bill_non_hub
    else 0 end
    ) * (-1)) * (sum(case when city_name = 'OTH' then total_bill_non_hub_now_ship_instant else null end) * 1.000/sum(total_bill_non_hub_now_ship_instant))
    end
    end
    ) * sum(total_bill_non_hub_now_ship_instant) * 1.000/sum(total_bill_now_ship_instant)
    +
    (
    case when sum(total_bill_hub_now_ship_instant) = 0 then 0
    when sum(total_bill_hub) = 0 then 0
    else
    case when sum(case when city_name = 'HCM' then total_bill_hub else 0 end) = 0 then 0 else
    (
    sum(case when city_name = 'HCM' then (hub_cost_auto_daily_bonus + hub_weekly_bonus) * 1.000 else 0 end) * 1.000/sum(case when city_name = 'HCM' then total_bill_hub else 0 end) * (-1)
    ) * (sum(case when city_name = 'HCM' then total_bill_hub_now_ship_instant else null end) * 1.000/sum(total_bill_hub_now_ship_instant))
    end
    +
    case when sum(case when city_name = 'HN' then total_bill_hub else 0 end) = 0 then 0 else
    (
    sum(case when city_name = 'HN' then (hub_cost_auto_daily_bonus + hub_weekly_bonus) * 1.000 else 0 end) * 1.000/sum(case when city_name = 'HN' then total_bill_hub else 0 end) * (-1)
    ) * (sum(case when city_name = 'HN' then total_bill_hub_now_ship_instant else null end) * 1.000/sum(total_bill_hub_now_ship_instant))
    end
    +
    case when sum(case when city_name = 'DN' then total_bill_hub else 0 end) = 0 then 0 else
    (
    sum(case when city_name = 'DN' then (hub_cost_auto_daily_bonus + hub_weekly_bonus) * 1.000 else 0 end) * 1.000/sum(case when city_name = 'DN' then total_bill_hub else 0 end) * (-1)
    ) * (sum(case when city_name = 'DN' then total_bill_hub_now_ship_instant else null end) * 1.000/sum(total_bill_hub_now_ship_instant))
    end
    +
    case when sum(case when city_name = 'OTH' then total_bill_hub else 0 end) = 0 then 0 else
    (
    sum(case when city_name = 'OTH' then (hub_cost_auto_daily_bonus + hub_weekly_bonus) * 1.000 else 0 end) * 1.000/sum(case when city_name = 'OTH' then total_bill_hub else 0 end) * (-1)
    ) * (sum(case when city_name = 'OTH' then total_bill_hub_now_ship_instant else null end) * 1.000/sum(total_bill_hub_now_ship_instant))
    end
    end
    ) * sum(total_bill_hub_now_ship_instant) * 1.000/sum(total_bill_now_ship_instant)
    as total_bonus
from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_drivers_cpo_daily_tab
group by 1
),


bonus_sp as (
Select distinct 
    date_,
    exchange_rate 
 
    ,case when sum(total_bill_now_ship_shopee) = 0 then 0
        when sum(total_bill_non_hub) = 0 then 0
        else
        
        (case when sum(case when city_name = 'HCM' then total_bill_non_hub else 0 end) = 0 then 0
        else
        
        (sum(case
        when city_name = 'HCM' then total_bonus_before_tax_combined*1.0000/exchange_rate
        else 0 end
        )*1.0000/sum(case
        when city_name = 'HCM' then total_bill_non_hub
        else 0 end
        )*(-1))*(sum(case when city_name = 'HCM' then total_bill_non_hub_now_ship_shopee else null end)*1.0000/sum(total_bill_non_hub_now_ship_shopee))
        
        end
        )
        
        +
        
        (case when sum(case when city_name = 'HN' then total_bill_non_hub else 0 end) = 0 then 0
        else
        
        (sum(case
        when city_name = 'HN' then total_bonus_before_tax_combined*1.0000/exchange_rate
        else 0 end
        )*1.0000/sum(case
        when city_name = 'HN' then total_bill_non_hub
        else 0 end
        )*(-1))*(sum(case when city_name = 'HN' then total_bill_non_hub_now_ship_shopee else null end)*1.0000/sum(total_bill_non_hub_now_ship_shopee))
        
        end
        )
        
        +
        
        (case when sum(case when city_name = 'DN' then total_bill_non_hub else 0 end) = 0 then 0
        else
        
        (sum(case
        when city_name = 'DN' then total_bonus_before_tax_combined*1.0000/exchange_rate
        else 0 end
        )*1.0000/sum(case
        when city_name = 'DN' then total_bill_non_hub
        else 0 end
        )*(-1))*(sum(case when city_name = 'DN' then total_bill_non_hub_now_ship_shopee else null end)*1.0000/sum(total_bill_non_hub_now_ship_shopee))
        
        end
        )
        
        +
        
        (case when sum(case when city_name = 'OTH' then total_bill_non_hub else 0 end) = 0 then 0
        else
        
        (sum(case
        when city_name = 'OTH' then total_bonus_before_tax_combined*1.0000/exchange_rate
        else 0 end
        )*1.0000/sum(case
        when city_name = 'OTH' then total_bill_non_hub
        else 0 end
        )*(-1))*(sum(case when city_name = 'OTH' then total_bill_non_hub_now_ship_shopee else null end)*1.0000/sum(total_bill_non_hub_now_ship_shopee))
        
        end
        )
        
        end  total_bonus_non_hub 
        ,case when sum(total_bill_now_ship_shopee) = 0 then 0
            else
            
            (
            
            
            case when sum(total_bill_hub_now_ship_shopee) = 0 then 0
            when sum(total_bill_hub) = 0 then 0
            else
            case when sum(case when city_name = 'HCM' then total_bill_hub else 0 end) = 0 then 0 else
            (
            sum(case when city_name = 'HCM' then (hub_cost_auto_daily_bonus + hub_weekly_bonus) * 1.0000/exchange_rate else 0 end) * 1.00/sum(case when city_name = 'HCM' then total_bill_hub else 0 end) * (-1)
            ) * (sum(case when city_name = 'HCM' then total_bill_hub_now_ship_shopee else null end) * 1.0000/sum(total_bill_hub_now_ship_shopee))
            end
            +
            case when sum(case when city_name = 'HN' then total_bill_hub else 0 end) = 0 then 0 else
            (
            sum(case when city_name = 'HN' then (hub_cost_auto_daily_bonus + hub_weekly_bonus) * 1.0000/exchange_rate else 0 end) * 1.00/sum(case when city_name = 'HN' then total_bill_hub else 0 end) * (-1)
            ) * (sum(case when city_name = 'HN' then total_bill_hub_now_ship_shopee else null end) * 1.0000/sum(total_bill_hub_now_ship_shopee))
            end
            +
            case when sum(case when city_name = 'DN' then total_bill_hub else 0 end) = 0 then 0 else
            (
            sum(case when city_name = 'DN' then (hub_cost_auto_daily_bonus + hub_weekly_bonus) * 1.0000/exchange_rate else 0 end) * 1.00/sum(case when city_name = 'DN' then total_bill_hub else 0 end) * (-1)
            ) * (sum(case when city_name = 'DN' then total_bill_hub_now_ship_shopee else null end) * 1.0000/sum(total_bill_hub_now_ship_shopee))
            end
            +
            case when sum(case when city_name = 'OTH' then total_bill_hub else 0 end) = 0 then 0 else
            (
            sum(case when city_name = 'OTH' then (hub_cost_auto_daily_bonus + hub_weekly_bonus) * 1.0000/exchange_rate else 0 end) * 1.00/sum(case when city_name = 'OTH' then total_bill_hub else 0 end) * (-1)
            ) * (sum(case when city_name = 'OTH' then total_bill_hub_now_ship_shopee else null end) * 1.0000/sum(total_bill_hub_now_ship_shopee))
            end
            end
            
            
            )
        
          end as total_bonus_hub 
        ,  case when sum(total_bill_hub_now_ship_shopee) = 0 then 0
                when sum(total_bill_hub) = 0 then 0
                else
                case when sum(case when city_name = 'HCM' then total_bill_hub else 0 end) = 0 then 0 else
                        (
                        sum(case when city_name = 'HCM' then hub_cost_auto_shipping_fee  else 0 end) * 1.00/sum(case when city_name = 'HCM' then total_bill_hub else 0 end) * (-1)
                        ) * (sum(case when city_name = 'HCM' then total_bill_hub_now_ship_shopee else null end) * 1.0000/sum(total_bill_hub_now_ship_shopee))
                        end
                +
                case when sum(case when city_name = 'HN' then total_bill_hub else 0 end) = 0 then 0 else
                        (
                        sum(case when city_name = 'HN' then hub_cost_auto_shipping_fee  else 0 end) * 1.00/sum(case when city_name = 'HN' then total_bill_hub else 0 end) * (-1)
                        ) * (sum(case when city_name = 'HN' then total_bill_hub_now_ship_shopee else null end) * 1.0000/sum(total_bill_hub_now_ship_shopee))
                         end
                +
                case when sum(case when city_name = 'DN' then total_bill_hub else 0 end) = 0 then 0 else
                        (
                        sum(case when city_name = 'DN' then hub_cost_auto_shipping_fee  else 0 end) * 1.00/sum(case when city_name = 'DN' then total_bill_hub else 0 end) * (-1)
                        ) * (sum(case when city_name = 'DN' then total_bill_hub_now_ship_shopee else null end) * 1.0000/sum(total_bill_hub_now_ship_shopee))
                        end
                +
                case when sum(case when city_name = 'OTH' then total_bill_hub else 0 end) = 0 then 0 else
                        (
                        sum(case when city_name = 'OTH' then hub_cost_auto_shipping_fee  else 0 end) * 1.00/sum(case when city_name = 'OTH' then total_bill_hub else 0 end) * (-1)
                        ) * (sum(case when city_name = 'OTH' then total_bill_hub_now_ship_shopee else null end) * 1.0000/sum(total_bill_hub_now_ship_shopee))
                        end
                end as driver_cost_basic_nss_hub       
from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_drivers_cpo_daily_tab
group by 1,2
),

Final AS (
SELECT DISTINCT 
    bill_fee.status,
    bill_fee.source,
    bill_fee.date_,
    bill_fee.is_qualified_hub,
    bill_fee.delivered_by,
    CASE WHEN bill_fee.distance = 0 THEN 1 ELSE bill_fee.distance END as distance,
    DATE_FORMAT(bill_fee.date_, '%Y%m') created_year_month,
    CASE WHEN MONTH(bill_fee.date_) = 1 AND WEEK(bill_fee.date_) >= 53 THEN
    YEAR(bill_fee.date_)*100-1 + WEEK(bill_fee.date_) ELSE CAST(DATE_FORMAT(bill_fee.date_, '%x%v') AS INT) END as year_week,
    bill_fee.order_id, 
    bill_fee.total_shipping_fee, -- before stack
    bill_fee.distance_range, 
    case when bill_fee.delivered_by = 'hub' then coalesce(bonus.driver_cost_basic_nss_hub,0)
         else 1.000000*(bill_fee.shipping_fee_share+bill_fee.return_fee_share)*(-1) end as total_driver_cost,
    case when bill_fee.delivered_by = 'hub' then 1.000000*0
         else 1.000000*(bill_fee.shipping_fee_share+bill_fee.return_fee_share - bill_fee.shipping_fee_share_basic - bill_fee.return_fee_share_basic)*(-1) end as total_driver_cost_surge,
    bill_fee.rev_shipping_fee + bill_fee.rev_return_fee rev_shipping_fee,
    bill_fee.rev_cod_fee,
    case when bill_fee.delivered_by = 'hub' then bonus.total_bonus_hub else bonus.total_bonus_non_hub end as total_bonus,
    bill_fee.prm_cost

FROM bill_fee 
left join bonus_sp bonus on bill_fee.date_ = bonus.date_ -- change here
WHERE 1=1 
-- AND bill_fee.status IN (11, 14)
AND bill_fee.status = 11
AND source = 'Now Ship Shopee' -- change here
-- AND sub_source = 'NS Instant'
AND bill_fee.date_  BETWEEN DATE'2022-04-12' AND DATE'2022-04-19'
),

base AS (
    SELECT
            ns.grass_date,
            ns.uid, 
            ns.id,
            ns.group_id,
            ogi.distance/100000 distance_grp,
            CASE WHEN ns.distance = 0 THEN 1 ELSE ns.distance END as distance,
            total_driver_cost

        FROM dev_vnfdbi_opsndrivers.ns_performance_tab ns
        LEFT JOIN shopeefood.foody_partner_db__order_group_info_tab__reg_daily_s0_live ogi on ogi.id = ns.group_id

        LEFT JOIN Final ON ns.id = Final.order_id 
        WHERE 1=1
         AND ns.grass_date between DATE'2022-04-01' AND DATE'2022-04-19'
         AND ns.source = 'now_ship_shopee'
        -- AND source IN ('now_ship_user', 'now_ship_multi_drop')
        AND order_status IN ('Delivered')
)

, base_2 AS (
    SELECT 
        t0.date_, t0.order_id,
        t0.is_qualified_hub, t0.delivered_by,
        t1.uid, t1.id, t1.group_id, t0.distance,
        CASE 
            when t0.distance <= 1 then '0-1 km'
            when t0.distance <= 2 then '1-2 km'
            when t0.distance <= 3 then '2-3 km'
            when t0.distance <= 4 then '3-4 km'
            when t0.distance <= 5 then '4-5 km'
            when t0.distance <= 6 then '5-6 km'
            when t0.distance <= 7 then '6-7 km'
            when t0.distance <= 8 then '7-8 km'
            when t0.distance <= 9 then '8-9 km'
            when t0.distance <= 10 then '9-10 km'
            when t0.distance <= 11 then '10-11 km'
            when t0.distance <= 12 then '11-12 km'
            when t0.distance <= 13 then '12-13 km'
            when t0.distance <= 14 then '13-14 km'
            when t0.distance <= 15 then '14-15 km'
            -- when t0.distance <= 16 then '15-16 km'
            -- when t0.distance <= 17 then '16-17 km'
            -- when t0.distance <= 18 then '17-18 km'
            -- when t0.distance <= 19 then '18-19 km'
            -- when t0.distance <= 20 then '19-20 km'
            -- when t0.distance <= 21 then '20-21 km'
            -- when t0.distance <= 22 then '21-22 km'
            -- when t0.distance <= 23 then '22-23 km'
            -- when t0.distance <= 24 then '23-24 km'
        ELSE '15++ km' end as distance_range,
        exchange_rate,
        COALESCE(t2.distance_all, t0.distance) distance_all,
        COALESCE(t2.distance_grp, t0.distance) distance_grp,
        1.000*t0.distance/COALESCE(t2.distance_all, t0.distance) distance_dist,
        t0.total_shipping_fee/ xrate.exchange_rate  total_shipping_fee_bf_stack,
        case when t0.delivered_by = 'hub' then 1.000*t0.total_driver_cost/ xrate.exchange_rate
             else 1.000*t0.distance/COALESCE(t2.distance_all, t0.distance) * COALESCE(t2.total_driver_cost, t0.total_driver_cost)/ xrate.exchange_rate end as total_driver_cost,
        case when t0.delivered_by = 'hub' then 0
             else 1.000*t0.distance/COALESCE(t2.distance_all, t0.distance) * t0.total_driver_cost_surge/ xrate.exchange_rate end as total_driver_cost_surge,             
        1.000*t0.prm_cost/ xrate.exchange_rate prm_cost,
        1.000*t0.rev_shipping_fee/ xrate.exchange_rate rev_shipping_fee,
        1.000*t0.rev_cod_fee/ xrate.exchange_rate rev_cod_fee,
        t0.total_bonus bonus
        
    FROM  Final t0
    LEFT JOIN base t1 ON t0.order_id = t1.id

    LEFT JOIN (
        SELECT group_id, distance_grp,
            SUM(distance) distance_all, 
            SUM(total_driver_cost) total_driver_cost
        FROM base
        WHERE group_id > 0
        GROUP BY 1, 2
        HAVING COUNT(DISTINCT id) > 1
        ) t2 ON t1.group_id = t2.group_id

    LEFT JOIN (
            SELECT distinct
                grass_date, exchange_rate
            FROM mp_order.dim_exchange_rate__reg_s0_live 
            WHERE currency='VND' AND grass_date >= date('2020-12-28')
        ) xrate on xrate.grass_date = t1.grass_date
-- ORDER BY group_id desc 
-- WHERE t0.status = 11 -- get deli only
)

SELECT 
   delivered_by, 
   is_qualified_hub,
    date_, 
    exchange_rate ,
    AVG(bonus) bonus_usd,
    AVG(distance) distance,
    COUNT(DISTINCT uid) orders,
    --1.00000*SUM(rev_shipping_fee)/COUNT(DISTINCT uid) rev_shipping_fee,
    --1.00000*SUM(rev_cod_fee)/COUNT(DISTINCT uid) rev_cod_fee,
    --1.00000*SUM(prm_cost)*(-1)/COUNT(DISTINCT uid) prm,
    1.00000*SUM(total_driver_cost)/COUNT(DISTINCT uid) total_driver_cost,
    (1.00000*SUM(total_driver_cost)/COUNT(DISTINCT uid)) - (1.00000*SUM(total_driver_cost_surge)/COUNT(DISTINCT uid)) as base_fee,
    1.00000*SUM(total_driver_cost_surge)/COUNT(DISTINCT uid) total_driver_cost_surge,
    1.00000*SUM(total_shipping_fee_bf_stack)/COUNT(DISTINCT uid) total_driver_cost_bf_stack

FROM base_2  --where is_qualified_hub = 1
where  date_ between current_date -interval '30' day and current_date - interval '1' day
GROUP BY 1,2,3,4

--UNION 

--SELECT 
  --  is_qualified_hub, delivered_by,
   -- 'All' as distance_range, 
  --  AVG(exchange_rate) exchange_rate,
 --   AVG(bonus)/AVG(exchange_rate) bonus_usd,
 --   AVG(distance) distance,
 --   COUNT(DISTINCT uid) orders,
 ---   1.00000*SUM(rev_shipping_fee)/COUNT(DISTINCT uid) rev_shipping_fee,
 --  1.00000*SUM(rev_cod_fee)/COUNT(DISTINCT uid) rev_cod_fee,
  --  1.00000*SUM(prm_cost)*(-1)/COUNT(DISTINCT uid) prm,
 --   1.00000*SUM(total_driver_cost)/COUNT(DISTINCT uid) total_driver_cost,
 --   1.00000*SUM(total_shipping_fee_bf_stack)/COUNT(DISTINCT uid) total_driver_cost_bf_stack
--FROM base_2
--GROUP BY 1,2,3
--ORDER BY 1
