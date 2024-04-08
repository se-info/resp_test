SELECT 
-- base1.inflow_date
'30May_12Jun' as date_range
-- ,base1.source
-- ,base1.order_source
,base1.source_v2
,base1.city_group
,base1.inflow_hour
,count(distinct base1.uid)/7.0 as net_orders
,cast(count(distinct case when peak_mode_name = 'Peak 1 Mode' then base1.uid else null end) as double) / count(distinct base1.uid) net_peak1_pct
,cast(count(distinct case when peak_mode_name = 'Peak 2 Mode' then base1.uid else null end) as double) / count(distinct base1.uid) net_peak2_pct
,cast(count(distinct case when peak_mode_name = 'Peak 3 Mode' then base1.uid else null end) as double) / count(distinct base1.uid) net_peak3_pct
,cast(count(distinct case when peak_mode_name = 'Normal' then base1.uid else null end) as double) / count(distinct base1.uid) net_normal_pct
,cast(count(distinct case when distance_range = '1. 0-3km' then base1.uid else null end) as double) / count(distinct base1.uid) net_under_3km_pct
,cast(count(distinct case when distance_range = '2. 3km+' then base1.uid else null end) as double) / count(distinct base1.uid) net_above_3km_pct
,cast(sum(case when distance_range = '2. 3km+' then base1.distance else null end) as double) / cast(count(distinct case when distance_range = '2. 3km+' then base1.uid else null end) as double) as avg_above_3km_distance
,cast(count(distinct case when peak_mode_name = 'Peak 1 Mode' then base1.uid else null end) as double)/7.0 as net_peak1_orders
,cast(count(distinct case when peak_mode_name = 'Peak 2 Mode' then base1.uid else null end) as double)/7.0 as net_peak2_orders
,cast(count(distinct case when peak_mode_name = 'Peak 3 Mode' then base1.uid else null end) as double)/7.0 as net_peak3_orders
,cast(count(distinct case when peak_mode_name = 'Normal' then base1.uid else null end) as double)/7.0 as net_normal_orders
,cast(count(distinct case when distance_range = '1. 0-3km' then base1.uid else null end) as double)/7.0 as net_under_3km_orders
,cast(count(distinct case when distance_range = '2. 3km+' then base1.uid else null end) as double)/7.0 as net_above_3km_orders
,cast(sum(case when distance_range = '1. 0-3km' then base1.distance else null end) as double)/7.0 as under_3km_distance
,cast(sum(case when distance_range = '2. 3km+' then base1.distance else null end) as double)/7.0 as above_3km_distance
,count(distinct case when is_hub_order = 0 then base1.uid else null end)/7.0 total_non_hub_order
,count(distinct case when is_hub_order = 1 then base1.uid else null end)/7.0 total_hub_order
,cast(count(distinct case when is_hub_order = 0 then base1.uid else null end) as double) / cast(count(distinct base1.uid) as double) as dist_order_non_hub
,cast(count(distinct case when is_hub_order = 0 and peak_mode_name = 'Normal' then base1.uid else null end) as double) / count(distinct case when is_hub_order = 0 then base1.uid else null end) net_normal_non_hub_pct
,cast(count(distinct case when is_hub_order = 0 and peak_mode_name = 'Peak 1 Mode' then base1.uid else null end) as double) / count(distinct case when is_hub_order = 0 then base1.uid else null end) net_peak1_non_hub_pct
,cast(count(distinct case when is_hub_order = 0 and peak_mode_name = 'Peak 2 Mode' then base1.uid else null end) as double)/ count(distinct case when is_hub_order = 0 then base1.uid else null end) net_peak2_non_hub_pct
,cast(count(distinct case when is_hub_order = 0 and peak_mode_name = 'Peak 3 Mode' then base1.uid else null end) as double)/ count(distinct case when is_hub_order = 0 then base1.uid else null end) net_peak3_non_hub_pct
,cast(count(distinct case when distance_range = '1. 0-3km' and is_hub_order = 0 then base1.uid else null end) as double)/7.0 as net_non_hub_under_3km_orders
,cast(count(distinct case when distance_range = '2. 3km+' and is_hub_order = 0 then base1.uid else null end) as double)/7.0 as net_non_hub_above_3km_distance
,cast(count(distinct case when distance_range = '1. 0-3km' and is_hub_order = 0 then base1.uid else null end) as double) / count(distinct case when is_hub_order = 0 then base1.uid else null end) as pct_non_hub_under_3km_orders
,cast(count(distinct case when distance_range = '2. 3km+' and is_hub_order = 0 then base1.uid else null end) as double) / count(distinct case when is_hub_order = 0 then base1.uid else null end) as pct_non_hub_above_3km_distance
,cast(sum(case when distance_range = '2. 3km+' and is_hub_order = 0 then base1.distance else null end) as double) / cast(count(distinct case when distance_range = '2. 3km+' and is_hub_order = 0 then base1.uid else null end) as double) as avg_non_hub_above_3km_distance
-- ,base1.distance_range
-- ,base1.peak_mode_name

-- ,sum(base1.distance) as total_distance

-- ,count(distinct case when is_hub_order = 0 then base1.uid else null end) total_non_hub_order
-- ,sum(case when is_hub_order = 0 then base1.distance else 0 end) total_distance_non_hub
--base1.peak_mode_name

from
(SELECT all.created_date
,date(inflow_timestamp) inflow_date
,all.source
,all.order_source
,all.source_v2
,all.id
,all.uid
,all.created_timestamp
,all.created_year_week
,all.created_year_month
,all.created_hour
,all.order_status
,all.city_id
,all.district_id
,all.city_group
,mode.start_time
,mode.end_time
,case when mode.peak_mode_name in ('Peak 1 Mode','Peak 2 Mode','Peak 3 Mode') then mode.peak_mode_name
        else 'Normal' end as peak_mode_name 
,all.distance_range
,all.distance
,case when ad_odt.partner_type = 12 and coalesce(dotet.driver_payment_policy,0) = 2 then 1 else 0 end as is_hub_order
,Extract(HOUR from inflow_timestamp) inflow_hour
from
(-- ********** order_delivery
SELECT oct.id
,concat('order_delivery_',cast(oct.id as VARCHAR)) as uid
,case when oct.foody_service_id = 1 then 'food'
      when oct.foody_service_id in (4,5) then 'fresh'
      else 'other market' end as source
,case when oct.foody_service_id is not null then 'Food'
      else 'Ship' end as source_v2

,oct.shipper_uid as shipper_id
-- order distance
,oct.distance
,case when oct.distance <= 3 then '1. 0-3km'
--    when oct.distance <= 5 then '2. 3-5km'
--    when oct.distance <= 7 then '3. 5-7km'
--    when oct.distance <= 10 then '4. 7-10km'
--    when oct.distance > 10 then '5. 10km+'
    when oct.distance > 3 then '2. 3km+'
    else null end as distance_range

-- time
,from_unixtime(oct.submit_time - 60*60) as created_timestamp
,cast(from_unixtime(oct.submit_time - 60*60) as date) as created_date
,format_datetime(cast(from_unixtime(oct.submit_time - 60*60) as date),'EE') as created_day_of_week
,case when cast(from_unixtime(oct.submit_time - 60*60) as date) between DATE('2018-12-31') and DATE('2018-12-31') then 201901
    when cast(from_unixtime(oct.submit_time - 60*60) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
    else YEAR(cast(from_unixtime(oct.submit_time - 60*60) as date))*100 + WEEK(cast(from_unixtime(oct.submit_time - 60*60) as date)) end as created_year_week
,concat(cast(YEAR(from_unixtime(oct.submit_time - 60*60)) as VARCHAR),'-',date_format(from_unixtime(oct.submit_time - 60*60),'%b')) as created_year_month
,Extract(HOUR from from_unixtime(oct.submit_time - 60*60)) as created_hour
,case when Extract(HOUR from from_unixtime(oct.submit_time - 60*60)) <= 5 then '5. 22:00-5:00'
    when Extract(HOUR from from_unixtime(oct.submit_time - 60*60)) <= 10 then '1. 6:00-10:00'
    when Extract(HOUR from from_unixtime(oct.submit_time - 60*60)) <= 13 then '2. 11:00-13:00'
    when Extract(HOUR from from_unixtime(oct.submit_time - 60*60)) <= 17 then '3. 14:00-17:00'
    when Extract(HOUR from from_unixtime(oct.submit_time - 60*60)) <= 21 then '4. 18:00-21:00'
    when Extract(HOUR from from_unixtime(oct.submit_time - 60*60)) <= 23 then '5. 22:00-5:00'
    else null end as created_hour_range
,coalesce(osl.first_auto_assign_timestamp,from_unixtime(oct.submit_time - 60*60)) inflow_timestamp
-- incharge time
,osl.first_auto_assign_timestamp
,osl.last_incharge_timestamp
,date_diff('second',osl.first_auto_assign_timestamp,osl.last_incharge_timestamp) as lt_incharge -- from 1st auto assign to last incharge

-- completion time
,osl.last_delivered_timestamp
,date_diff('second',from_unixtime(oct.submit_time - 60*60),osl.last_delivered_timestamp) as lt_completion

-- order info
,case when oct.status = 7 then 'Delivered'
    when oct.status = 8 then 'Cancelled'
    when oct.status = 9 then 'Quit' end as order_status
,case when oct.foody_service_id = 1 then 'Food'
    -- when oct.foody_service_id = 3 then 'Laundy'
    -- when oct.foody_service_id = 4 then 'Products'
    -- when oct.foody_service_id = 5 then 'Fresh'
    -- when oct.foody_service_id = 6 then 'Flowers'
    -- when oct.foody_service_id = 7 then 'Medicine'
    -- when oct.foody_service_id = 12 then 'Pets'
    -- when oct.foody_service_id = 13 then 'Liquor'
    -- when oct.foody_service_id = 15 then 'Salon'
    else 'Market' end as foody_service

-- location
,case when oct.city_id = 238 THEN 'Dien Bien' else city.city_name end as city_name
,case when oct.city_id = 217 then 'HCM'
    when oct.city_id = 218 then 'HN'
    when oct.city_id = 219 then 'DN'
    ELSE 'OTH' end as city_group
-- ,district.district_name

-- flag
,oct.is_foody_delivery
,oct.is_asap

-- payment
,case when oct.payment_method = 1 then 'Cash'
    when oct.payment_method = 6 then 'Airpay'
    else 'Others' end as payment_method
    
-- location id
,oct.city_id
,oct.district_id
,case when go.app_type_id in (50,51) then 'Shopee'                                                                                             
        when go.app_type_id = 0 then 'Others'                                                                                              
        else 'Now' end as order_source  

from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
LEFT JOIN shopeefood.foody_mart__fact_gross_order_join_detail go on go.id = oct.id
    -- location
    left join (SELECT city_id
                ,city_name
                
                from shopeefood.foody_mart__fact_gross_order_join_detail
                where from_unixtime(create_timestamp) between date(cast(now() - interval '30' day as TIMESTAMP)) and date(cast(now() - interval '1' hour as TIMESTAMP))
                
                GROUP BY city_id
                        ,city_name
                )city on city.city_id = oct.city_id
    
--    left join (SELECT district_id
  --              ,district_name
                
    --            from foody.foody_mart__fact_gross_order_join_detail
      --          where from_unixtime(create_timestamp) between date(cast(now() - interval '30' day as TIMESTAMP)) and date(cast(now() - interval '1' hour as TIMESTAMP))
                
        --        GROUP BY district_id
          --              ,district_name
            --    )district on district.district_id = oct.district_id
    
    -- assign time: request archive log
    left join
            (SELECT order_id
                ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
                ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
                ,max(case when status = 7 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_delivered_timestamp
                
            from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
            where 1=1
            group by order_id
            )osl on osl.order_id = oct.id

)all
    LEFT JOIN
                (SELECT pm.city_id
                    ,pm.district_id
                    ,pm.mode_id
                    ,from_unixtime(pm.start_time - 60*60) as start_time
                    ,from_unixtime(pm.start_time + pm.running_time - 60*60) as end_time
                    ,pm.available_driver
                    ,pm.assigning_order
                    ,pm.driver_availability
                    ,pm_name.name as peak_mode_name
                    
                    from shopeefood.foody_delivery_admin_db__peak_mode_export_activity_tab__reg_daily_s0_live pm 
                        left join shopeefood.foody_delivery_admin_db__peak_mode_tab__reg_daily_s0_live pm_name on pm_name.id = pm.mode_id
                    where pm.mode_id in (7,8,9,10,11)
                )mode on mode.city_id = all.city_id and mode.district_id = all.district_id and all.inflow_timestamp >= mode.start_time and all.inflow_timestamp < mode.end_time 

left join shopeefood.foody_accountant_db__order_delivery_tab__reg_daily_s0_live ad_odt on all.id = ad_odt.order_id
left JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on dot.ref_order_id = all.id and dot.ref_order_category = 0 
left join (SELECT order_id
                ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
                ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
                ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
                ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
                ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
                ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)
            
            from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet
            
            )dotet on dot.id = dotet.order_id

where (date(inflow_timestamp) between date('2022-05-30') and date '2022-06-12')
and all.order_status = 'Delivered'

-- and all.city_group = 'HCM'
-- and all.uid = 'order_delivery_131406469'
-- limit 100
-- and case when ad_odt.partner_type = 12 and coalesce(dotet.driver_payment_policy,0) = 2 then 1 else 0 end = 0
)base1
 --> get only order non hub 
-- where base1.is_hub_order = 0
GROUP BY 1,2,3,4