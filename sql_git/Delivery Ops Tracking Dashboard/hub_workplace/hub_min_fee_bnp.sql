drop table if exists dev_vnfdbi_opsndrivers.shopeefood_vn_tet_holiday_min_fee_tab_adhoc_sunday;
create table if not exists dev_vnfdbi_opsndrivers.shopeefood_vn_tet_holiday_min_fee_tab_adhoc_sunday

as

select 
    *
from
(SELECT 
        base2.order_id
        ,base2.partner_id
        ,name.full_name
        ,prov.name_en as city_name
        ,base2.city_name as city_group
        ,base2.city_group_mapping
        ,base2.inflow_date
        ,base2.delivered_date
        ,base2.year_week
        ,base2.partner_type
        ,base2.source
        ,base2.distance
        ,base2.status
        ,base2.driver_payment_policy
        ,base2.inflow_timestamp
        ,base2.inflow_hour
        ,base2.is_group_stack
        ,base2.dot_delivery_cost
        ,base2.is_hub_in_shift
        ,base2.shipping_fee_share
        ,base2.late_night_fee
        ,base2.holiday_fee
        ,base2.total_shipping_fee_driver_received
        ,base2.hour_range
        ,base2.expected_min_fee
        ,base2.is_hub_order
        ,base2.autopay_date
       
      , case when expected_min_fee - total_shipping_fee_driver_received <= 100 then 0 
             else expected_min_fee - total_shipping_fee_driver_received end as diff 
      , case when expected_min_fee - total_shipping_fee_driver_received > 100 then 1 else 0 end as is_need_adjust_shipping_fee

FROM 
(
SELECT base1.*
      ,case when coalesce(cast(hm.is_min_fee_guarantee as bigint),0) = 1 or (source = 'Now Ship Shopee' and is_hub_in_shift = 1) then 1 else 0 end as is_min_fee_guarantee
      ,case when coalesce(cast(hm.is_min_fee_guarantee as bigint),0) = 1 then coalesce(cast(hm.min_fee as double),0) 
        --     when source = 'Now Ship Shopee' and is_hub_in_shift = 1 then 15000
            end as expected_min_fee
        ,hm.hour_range
    ,hsf.is_hub_order
    ,hsf.autopay_date
FROM 
(
SELECT    order_id
        , partner_id
        , city_name
        , city_group_mapping
        , city_id
        , date(inflow_timestamp) as inflow_date
        , date_ as delivered_date
        , year_week
        , partner_type
        , source
        , distance
        , status
        , driver_payment_policy
        , inflow_timestamp
        , is_group_stack
        , dot_delivery_cost
        , is_hub_in_shift
        , case when is_hub_in_shift = 1 then 13500 else shipping_fee_share end as shipping_fee_share
        , late_night_fee
        , holiday_fee 
        , case when is_hub_in_shift = 1 then 13500 else shipping_fee_share end  + late_night_fee + holiday_fee as total_shipping_fee_driver_received 
        -- , hour_range
        ,inflow_hour
    

FROM 
(

select   raw.order_id
        ,raw.partner_id
        ,raw.city_name
        ,raw.city_group_mapping
        ,raw.city_id
        ,raw.date_
        ,raw.year_week
        ,raw.partner_type
        ,raw.source
        ,raw.distance
        ,raw.status
        ,raw.driver_payment_policy
        ,raw.inflow_timestamp
        -- ,case 
        --         -- when HOUR(raw.inflow_timestamp)*100+ MINUTE(raw.inflow_timestamp) >= 630 AND HOUR(raw.inflow_timestamp)*100+ MINUTE(raw.inflow_timestamp) < 1000 then '06am_10am'
        --         when HOUR(raw.inflow_timestamp)*100+ MINUTE(raw.inflow_timestamp) >= 1000 AND HOUR(raw.inflow_timestamp)*100+ MINUTE(raw.inflow_timestamp) < 1100 then '10am_11am'
        --         when HOUR(raw.inflow_timestamp)*100+ MINUTE(raw.inflow_timestamp) >= 1100 AND HOUR(raw.inflow_timestamp)*100+ MINUTE(raw.inflow_timestamp) < 1230 then '11am_1230pm'
        --         when HOUR(raw.inflow_timestamp)*100+ MINUTE(raw.inflow_timestamp) >= 1700 AND HOUR(raw.inflow_timestamp)*100+ MINUTE(raw.inflow_timestamp) < 2000 then '17pm_20pm'
        --         when HOUR(raw.inflow_timestamp)*100+ MINUTE(raw.inflow_timestamp) >= 2000 AND HOUR(raw.inflow_timestamp)*100+ MINUTE(raw.inflow_timestamp) < 2100 then '20pm_21pm'
        -- --       when HOUR(raw.inflow_timestamp)*100+ MINUTE(raw.inflow_timestamp) >= 1500 AND HOUR(raw.inflow_timestamp)*100+ MINUTE(raw.inflow_timestamp) < 1700 then '15pm_17pm'
        -- --        when HOUR(raw.inflow_timestamp)*100+ MINUTE(raw.inflow_timestamp) >= 2100 AND HOUR(raw.inflow_timestamp)*100+ MINUTE(raw.inflow_timestamp) < 2200 then '21pm_22pm'
        -- --       when HOUR(raw.inflow_timestamp)*100+ MINUTE(raw.inflow_timestamp) >= 2000 AND HOUR(raw.inflow_timestamp)*100+ MINUTE(raw.inflow_timestamp) < 2200 then '20pm_22pm'
        --        else null end as hour_range
        ,HOUR(raw.inflow_timestamp)*100+ MINUTE(raw.inflow_timestamp) as inflow_hour
        ,raw.is_group_stack
        ,raw.dot_delivery_cost
        ,case when raw.partner_type = 12 and coalesce(raw.driver_payment_policy,0) = 2 then 1 else 0 end as is_hub_in_shift
        ,SUM(case when trx.txn_type in (201,301,401,104,1000,2001,2101,3000) then trx.balance + trx.deposit else 0 end)*1.0/(100*1.0) as shipping_fee_share
        ,SUM(case when trx.txn_type in (119) then trx.balance + trx.deposit else 0 end)*1.0/(100*1.0) as late_night_fee
        ,SUM(case when trx.txn_type in (117) then trx.balance + trx.deposit else 0 end)*1.0/(100*1.0) as holiday_fee
from 
        (
        SELECT   o.order_id
                ,o.partner_id
                ,o.city_name
                ,o.city_group_mapping
                ,o.city_id
                ,o.date_
                ,o.year_week
                ,o.partner_type
                ,o.source
                ,coalesce(o.distance,0) as distance
                ,o.status
                ,o.driver_payment_policy
                ,o.inflow_timestamp
                ,case when o.partner_type = 12 and coalesce(o.driver_payment_policy,0) = 2 then 0 
                      when o.is_group_stack = 1 then 1 else 0 end as is_group_stack
                ,o.dot_delivery_cost
        from        
                (--EXPLAIN ANALYZE
                -- Food / Market
                select  distinct ad_odt.order_id,ad_odt.partner_id
                    ,case when ad_odt.city_id = 217 then 'HCM'
                          when ad_odt.city_id = 218 then 'HN'
                          when ad_odt.city_id = 219 then 'DN'
                          else 'OTH' end as city_name

                        ,case when ad_odt.city_id in (217) then 'HCM'
                            when ad_odt.city_id in (218) then 'HN'
                          when ad_odt.city_id = 220 then 'HP'
                          else 'na' end as city_group_mapping
                    ,ad_odt.city_id
                    ,cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time)  - 60*60) as date) as date_
                    ,case when cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time) - 60*60) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
                          when cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time) - 60*60) as date) between DATE('2020-12-28') and DATE('2021-01-03') then 202053
                          when cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time) - 60*60) as date) between DATE('2022-01-01') and DATE('2022-01-02') then 202152
                            else YEAR(cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time) - 60*60) as date))*100 + WEEK(cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time) - 60*60) as date)) end as year_week
                    ,ad_odt.partner_type
                    ,case when oct.foody_service_id = 1 then 'Food'
                            else 'Market' end as source
                    ,oct.distance
                    ,oct.status
                    ,coalesce(fa.last_auto_assign_timestamp, from_unixtime(dot.submitted_time - 3600)) as inflow_timestamp
                    ,case when dot.group_id > 0 then 1 else 0 end as is_group_stack
                    -- hub order 
                    ,coalesce(dotet.driver_payment_policy,1) as driver_payment_policy  -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)
                    ,dot.delivery_cost*1.0000/100 as dot_delivery_cost
                from shopeefood.foody_accountant_db__order_delivery_tab__reg_daily_s0_live ad_odt
                left join (SELECT id,submit_time,foody_service_id,distance,status,total_shipping_fee,extra_data, is_asap
                                ,coalesce(cast(json_extract(oct.extra_data,'$.bad_weather_fee.user_pay_amount') as decimal),0) as user_bwf
                
                            from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
                            where submit_time > 1609439493
                            )oct on oct.id = ad_odt.order_id and oct.submit_time > 1609439493
                LEFT JOIN
                        (
                        SELECT   order_id , 0 as order_type
                                ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
                                ,max(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_auto_assign_timestamp
                                ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
                                ,max(case when status = 6 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp
                                from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
                                where 1=1
                                -- and grass_schema = 'foody_order_db'
                                group by 1,2

                        ) fa on fa.order_id = oct.id               
                            
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

        where cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time)  - 60*60) as date) BETWEEN date('2024-05-12') AND current_date - interval '1' day  -- date(current_date) - interval '75' day
        and ad_odt.partner_id > 0 
        and oct.status = 7
      --  and ad_odt.city_id in (21)
--         UNION ALL
--     --------------
--   --    EXPLAIN ANALYZE
--             -- NS Shopee
--                 select  distinct ad_nss.order_id,ad_nss.partner_id
--                     ,case when ad_nss.city_id = 217 then 'HCM'
--                             when ad_nss.city_id = 218 then 'HN'
--                             when ad_nss.city_id = 219 then 'DN'
--                             else 'OTH' end as city_name

--                         ,case when ad_nss.city_id in (217,218) then 'HCM_HN'
--                           when ad_nss.city_id = 220 then 'HP'
--                           else 'na' end as city_group_mapping
--                     ,ad_nss.city_id      
--                     ,cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) as date_
--                     ,CASE WHEN WEEK(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60)) >= 52 AND MONTH(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60)) = 1 THEN (YEAR(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60))-1)*100 + WEEK(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60)) 
--                             ELSE CAST(DATE_FORMAT(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60), '%x%v') AS BIGINT) END as year_week
--                     ,ad_nss.partner_type
--                     ,'Now Ship Shopee' as source
--                     ,esbt.distance*1.00/1000 as distance
--                     ,esbt.status 
--                     ,from_unixtime(esbt.create_time - 3600) as inflow_timestamp
--                     ,case when dot.group_id > 0 then 1 else 0 end as is_group_stack
--                     -- hub order 
--                     ,coalesce(dotet.driver_payment_policy,1) as driver_payment_policy  -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)
--                     ,dot.delivery_cost*1.0000/100 as dot_delivery_cost

--                 from shopeefood.foody_accountant_db__order_now_ship_shopee_tab__reg_daily_s0_live ad_nss
--                 Left join shopeefood.foody_express_db__shopee_booking_tab__reg_daily_s0_live esbt on esbt.id = ad_nss.order_id and esbt.create_time > 1609439493
--                 left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on esbt.id = dot.ref_order_id and dot.ref_order_category = 6 and dot.submitted_time > 1609439493

--                 left join (SELECT order_id
--                                     ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
--                                     ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
--                                     ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
--                                     ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
--                                     ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
--                                     ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)
                                
--                                 from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet
                                
--                                 )dotet on dot.id = dotet.order_id

--                 where cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) BETWEEN date('2023-01-15') AND date('2023-01-15')  -- ate(current_date) - interval '75' day
--                 and ad_nss.partner_id > 0
--                 and esbt.status = 11
--                 and booking_type in (4,5)
        )o 
where 1=1 
and (case when o.partner_type = 12 and coalesce(o.driver_payment_policy,0) = 2 then 0 
         when o.is_group_stack = 1 then 1 else 0 end) = 0
)raw

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
                
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) base

WHERE 1=1 
and (partner_type not in (1,3)) -- to ignore order by fulltime driver
and date(inflow_timestamp) BETWEEN date('2024-05-12') AND current_date - interval '1' day
--AND dot_delivery_cost = shipping_fee_share 
--AND is_hub_in_shift = 0
) base1

inner JOIN vnfdbi_opsndrivers.shopeefood_vn_bnp_drivers_holiday_min_fee hm on date(hm.date_) = date(base1.inflow_timestamp) and base1.source in ('Food', 'Market','Now Ship Shopee' ) and base1.city_group_mapping = hm.city_group and  base1.inflow_hour >= cast(hm.from_ as bigint) and base1.inflow_hour < cast(hm.to_ as bigint)
left join vnfdbi_opsndrivers.shopeefood_vn_bnp_hub_shipping_fee_order_level hsf on base1.partner_id = hsf.shipper_id and base1.order_id = hsf.ref_order_id and hsf.ref_order_category = 0 

WHERE 1=1 
and (coalesce(cast(hm.is_min_fee_guarantee as bigint),0) = 1 or (source = 'Now Ship Shopee' and is_hub_in_shift = 1))
--and order_id = 246464453

--AND is_hub_in_shift = 0
)base2
left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live prov on prov.country_id = 86 and base2.city_id = prov.id
left join shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live name on base2.partner_id = name.uid

)
where 1=1


-- (case 
--     when  city_name in ('HCM City','Ha Noi City')
--     and source in ('Food', 'Market') and hour_range in ('10am_11am','11am_1230pm','17pm_20pm','20pm_21pm') then 1
    
-- --     when  city_name in ('HCM City','Ha Noi City')
-- --     and source in ('Food', 'Market') and hour_range in ('10am_1230pm') then 1
-- --     when  city_name in ('HCM City','Ha Noi City')
-- --     and source in ('Food', 'Market') and hour_range in ('17pm_20pm') then 1
-- --     when  city_name in ('HCM City','Ha Noi City')
-- --     and source in ('Food', 'Market') and hour_range in ('20pm_22pm') then 1
-- -- when  city_name in ('HCM City','Ha Noi City')
-- --     and source in ('Now Ship Shopee') and hour_range in ('07am_10am') then 1

-- else 0 end) = 1
/*
add city_name
add shipper name
*/


select 
        delivered_date
        ,hour_range
        ,city_group
        ,source
        ,sum(diff) as total_bonus
from dev_vnfdbi_opsndrivers.shopeefood_vn_tet_holiday_min_fee_tab_adhoc_sunday
-- where city_group_mapping = 'HP'
where 1=1
and delivered_date between date '2024-05-12' and date '2024-05-12'
and is_hub_order = 1
and autopay_date = delivered_date
and is_need_adjust_shipping_fee = 1
and source in ('Food','Market')
group by 1,2,3,4



