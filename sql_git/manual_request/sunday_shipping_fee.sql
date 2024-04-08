
select 

        *

from

(SELECT base1.*

      ,coalesce(cast(hm.is_min_fee_guarantee as bigint),0) as is_min_fee_guarantee

      ,coalesce(cast(hm.min_fee as double),0) as expected_min_fee

    --   ,case

    --         when coalesce(cast(hm.min_fee as double),0) - total_shipping_fee_driver_received <= 100 then 0

    --         else coalesce(cast(hm.min_fee as double),0) - total_shipping_fee_driver_received end as diff --ignore diff <100 d

    --   ,case when coalesce(cast(hm.min_fee as double),0) - total_shipping_fee_driver_received > 100 then 1 else 0 end as is_need_adjust_shipping_fee

      ,case 

            when coalesce(cast(hm.min_fee as double),0) - shipping_fee_share <= 100 then 0

            else coalesce(cast(hm.min_fee as double),0) - shipping_fee_share end as diff

    ,case when coalesce(cast(hm.min_fee as double),0) - shipping_fee_share > 100 then 1 else 0 end as is_need_adjust_shipping_fee

FROM

(

SELECT    order_id

        , order_code

        , partner_id

        , city_name

        , district_name

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

        , hour_range



FROM

(


select   raw.order_id

        ,raw.order_code

        ,raw.partner_id

        ,raw.city_name

        ,raw.city_id

        ,raw.date_

        ,raw.year_week

        ,raw.partner_type

        ,raw.source

        ,raw.distance

        ,raw.status

        ,raw.driver_payment_policy

        ,raw.inflow_timestamp

        ,case 

        -- when EXTRACT(HOUR from raw.inflow_timestamp)*100+ EXTRACT(MINUTE from raw.inflow_timestamp) >= 0600 AND EXTRACT(HOUR from raw.inflow_timestamp)*100+ EXTRACT(MINUTE from raw.inflow_timestamp) < 1030 then '06am_1030am'

        -- when EXTRACT(HOUR from raw.inflow_timestamp)*100+ EXTRACT(MINUTE from raw.inflow_timestamp) >= 1030 AND EXTRACT(HOUR from raw.inflow_timestamp)*100+ EXTRACT(MINUTE from raw.inflow_timestamp) < 1230 then '1030am_1230pm'

        -- when EXTRACT(HOUR from raw.inflow_timestamp)*100+ EXTRACT(MINUTE from raw.inflow_timestamp) >= 1300 AND EXTRACT(HOUR from raw.inflow_timestamp)*100+ EXTRACT(MINUTE from raw.inflow_timestamp) < 1330 then '1300pm_1330pm'

        when EXTRACT(HOUR from raw.inflow_timestamp)*100+ EXTRACT(MINUTE from raw.inflow_timestamp) >= 1700 AND EXTRACT(HOUR from raw.inflow_timestamp)*100+ EXTRACT(MINUTE from raw.inflow_timestamp) < 2100 then '17pm_21pm'       

        else null end as hour_range

        ,raw.is_group_stack

        ,raw.dot_delivery_cost

        ,raw.district_name

        ,case when raw.partner_type = 12 and coalesce(raw.driver_payment_policy,0) <> 3 then 1 else 0 end as is_hub_in_shift

        ,SUM(case when trx.txn_type in (201,301,401,104,1000,2001,2101,3000) then trx.balance + trx.deposit else 0 end)*1.0/(100*1.0) as shipping_fee_share

        ,SUM(case when trx.txn_type in (119) then trx.balance + trx.deposit else 0 end)*1.0/(100*1.0) as late_night_fee

        ,SUM(case when trx.txn_type in (117) then trx.balance + trx.deposit else 0 end)*1.0/(100*1.0) as holiday_fee

from

        (

        SELECT   o.order_id

                ,o.order_code

                ,o.partner_id

                ,o.city_name

                ,o.city_id

                ,o.date_

                ,o.year_week

                ,o.partner_type

                ,o.source

                ,coalesce(o.distance,0) as distance

                ,o.status

                ,o.driver_payment_policy

                ,o.inflow_timestamp

                ,case when o.partner_type = 12 and coalesce(o.driver_payment_policy,0) <> 3 then 0 -- inshift hub orders

                      when o.is_group_stack = 1 then 1 else 0 end as is_group_stack

                ,o.dot_delivery_cost

                ,o.district_name

        from

                (--EXPLAIN ANALYZE

                -- Food / Market

                select  distinct ad_odt.order_id,ad_odt.partner_id,ad_odt.order_code

                    -- ,case when ad_odt.city_id = 217 then 'HCM'

                    --       when ad_odt.city_id = 218 then 'HN'

                    --       when ad_odt.city_id = 219 then 'DN'

                    --       else 'OTH' end as city_name

                    ,city.name_en as city_name

                    ,ad_odt.city_id

                    ,cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time)  - 60*60) as date) as date_

                    ,case when cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time) - 60*60) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001

                          when cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time) - 60*60) as date) between DATE('2020-12-28') and DATE('2021-01-03') then 202053

                          when cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time) - 60*60) as date) between DATE('2022-01-01') and DATE('2022-01-02') then 202152

                            else YEAR(cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time) - 60*60) as date))*100 + WEEK(cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time) - 60*60) as date)) end as year_week

                    ,ad_odt.partner_type

                    ,case when oct.foody_service_id = 1 then 'Food'

                        when oct.foody_service_id = 5 then 'Market - Fresh'

                            else 'Market - Non-Fresh' end as source

                    ,oct.distance

                    ,oct.status

                    ,coalesce(fa.last_auto_assign_timestamp, from_unixtime(dot.submitted_time - 3600)) as inflow_timestamp

                    ,case when dot.group_id > 0 then 1 else 0 end as is_group_stack

                    -- hub order

                    ,coalesce(dotet.driver_payment_policy,1) as driver_payment_policy  -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)

                    ,dot.delivery_cost*1.0000/100 as dot_delivery_cost

                    ,oct.district_id

                    ,district.name_en as district_name

                from shopeefood.foody_accountant_db__order_delivery_tab__reg_daily_s0_live ad_odt

                left join (SELECT id,submit_time,foody_service_id,distance,status,total_shipping_fee,extra_data, is_asap, district_id

                                ,coalesce(cast(json_extract(oct.extra_data,'$.bad_weather_fee.user_pay_amount') as decimal),0) as user_bwf

                            from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct

                            where submit_time > 1609439493

                            ) oct on oct.id = ad_odt.order_id and oct.submit_time > 1609439493

                LEFT JOIN

                        (SELECT   order_id , 0 as order_type

                                ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp

                                ,max(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_auto_assign_timestamp

                                ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp

                                ,max(case when status = 6 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp

                                from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live

                                where 1=1

                                and grass_schema = 'foody_order_db'

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

                    LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on ad_odt.city_id = city.id and city.country_id = 86

                    left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live district on oct.district_id = district.id 


        where cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time)  - 60*60) as date) BETWEEN date '${campaign_date}' - interval '7' day AND date '${campaign_date}'

        and ad_odt.partner_id > 0

        and oct.status = 7 -- delivered

        -- and ad_odt.city_id in (217,218,219) -- apply HCM,HN,DN

        and ad_odt.city_id in (218) -- HN only

        -- and ad_odt.city_id in (218) -- apply only HN

        -- and ad_odt.city_id not in (0,238,468,469,470,471,472) -- -- applied all cities excluded test city

        )o

where 1=1

and (case when o.partner_type = 12 and coalesce(o.driver_payment_policy,0) <> 3 then 0

         when o.is_group_stack = 1 then 1 else 0 end) = 0 -- exclude group/stack orders from non-hub

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

            )trx on trx.reference_id = raw.order_id

                and trx.user_id = raw.partner_id -- user_id = partner_id = shipper_id

                and trx.created_date >= raw.date_ - interval '2' day and trx.created_date <= raw.date_ + interval '2' day      -- map by order Id --> more details than shipper_id

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17

) base


WHERE 1=1

and (is_hub_in_shift = 1 or shipping_fee_share >= 13500) -- to ignore order by fulltime driver

and date(inflow_timestamp) between date '${campaign_date}' and date '${campaign_date}'

-- and source in ('Food','Fresh')

) base1


LEFT JOIN vnfdbi_opsndrivers.shopeefood_vn_bnp_drivers_holiday_min_fee hm on date(hm.date_) = date(base1.inflow_timestamp) and hm.hour_range = base1.hour_range

)

WHERE 1=1

and coalesce(cast(is_min_fee_guarantee as bigint),0) = 1

and (case 

    when hour_range = '17pm_21pm' and city_name in ('Ha Noi City') then 1 

    else 0 end) = 1

-- and city_name = 'HN'

-- and is_hub_in_shift = 1 -- is_hub_in_shift >> filter driver Hub , driver hub outshift > Non-Hub


/*

-- Conditions:

-- apply: online 10h30-1330 > HN | 1300-1330: HCM

-- NowFood

-- Hub Food:

-- 07/07: 10h30-1330 > HN: 15k | 1300-1330: HCM,HN: 15k |17h-20h :HCM +HN 15k

*/

