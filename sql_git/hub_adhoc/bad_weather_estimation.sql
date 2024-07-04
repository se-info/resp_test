with fa as                
(SELECT   
    order_id 
    , 0 as order_type
    ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
    ,max(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_auto_assign_timestamp
    ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
    ,max(case when status = 6 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp
    from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
    where 1=1
    and grass_schema = 'foody_order_db'
    group by 1,2
)
,gross as 

(select id, bad_weather_fee*1.00 as bwf 

from shopeefood.foody_mart__fact_gross_order_join_detail 

where cast(grass_date as date) between current_date - interval '15' day and current_date - interval '1' day
)

,driver_order as 
(select 
        dot.uid as shipper_id
        ,dot.ref_order_id as order_id
        ,dot.ref_order_code as order_code
        ,CAST(dot.ref_order_id AS VARCHAR) || '-' || CAST(dot.ref_order_category AS VARCHAR) AS order_uid
        ,dot.ref_order_category
        ,case when dot.ref_order_category = 0 then 'order_delivery'
            when dot.ref_order_category = 3 then 'now_moto'
            when dot.ref_order_category = 4 then 'now_ship'
            when dot.ref_order_category = 5 then 'now_ship'
            when dot.ref_order_category = 6 then 'now_ship_shopee'
            when dot.ref_order_category = 7 then 'now_ship_sameday'
            else null end source
        ,dot.ref_order_status
        ,dot.order_status
        ,case when dot.order_status = 1 then 'Pending'
            when dot.order_status in (100,101,102) then 'Assigning'
            when dot.order_status in (200,201,202,203,204) then 'Processing'
            when dot.order_status in (300,301) then 'Error'
            when dot.order_status in (400,401,402,403,404,405,406,407) then 'Completed'
            else null end as order_status_group
        ,dot.is_asap
        -- ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60))
        --     when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
        --     else date(from_unixtime(dot.submitted_time- 60*60)) end as report_date
        ,case 
            when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60)) 
            else date(from_unixtime(dot.submitted_time- 60*60)) end as report_date
        -- ,date(from_unixtime(dot.submitted_time- 60*60)) created_date
        ,from_unixtime(dot.submitted_time- 60*60) created_timestamp
        ,case when dot.real_drop_time = 0 then null else from_unixtime(dot.real_drop_time - 60*60) end as last_delivered_timestamp
    --   ,case when dot.pick_city_id = 238 THEN 'Dien Bien' else city.city_name end as city_name
        ,case when dot.pick_city_id = 217 then 'HCM'
            when dot.pick_city_id = 218 then 'HN'
            when dot.pick_city_id = 219 then 'DN'
            ELSE 'OTH' end as city_group
    ,coalesce(fa.last_auto_assign_timestamp, from_unixtime(dot.submitted_time - 3600)) as inflow_timestamp
    ,date(coalesce(fa.last_auto_assign_timestamp, from_unixtime(dot.submitted_time - 3600))) as inflow_date
    ,coalesce(dotet.driver_payment_policy,1) as driver_payment_policy
    ,go.bwf

from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

left join gross go on go.id = dot.ref_order_id
left join shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
    on dot.ref_order_id = oct.id and dot.ref_order_category = 0 and oct.submit_time > 1609439493

left join fa
    on fa.order_id = oct.id

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

    )dotet on dot.id = dotet.order_id

where 1=1
    and dot.order_status = 400 -- delivered order
    and dot.ref_order_category = 0 -- Now Food only 
    and dot.pick_city_id not in (0,238,468,469,470,471,472)
    and (date(from_unixtime(dot.submitted_time-3600)) = date'2022-05-30' or date(from_unixtime(dot.submitted_time-3600)) = date'2022-06-02') 
    and date(from_unixtime(dot.submitted_time-3600)) = date'2022-06-12'
)


select 
        report_date 
       ,shipper_id 
       ,shipper_name
       ,city_name
       ,case when inshift_delivered_order > 0 then 'Inshift'
             else 'Out shift' end as working_type 
       ,total_delivered_order
       ,inshift_delivered_order
       ,total_bw_fee      


from 

(select a.report_date
      ,a.shipper_id
      ,sm.shipper_name 
      ,sm.city_name
      ,case when slot.uid is not null and slot.registration_status != 2 then 'Inshift'
            else 'Out shift' end as working_type
      ,count(distinct order_code) as total_delivered_order
      ,count(distinct case when driver_payment_policy = 2 then order_code else null end) as inshift_delivered_order
      ,count(distinct case when bwf > 0 then order_code else null end) as total_bw_fee




      from driver_order a 


      left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = a.shipper_id and try_cast(sm.grass_date as date) = a.report_date

      left join shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live slot on slot.uid = a.shipper_id 
                                                                                              and date(from_unixtime(slot.date_ts - 3600)) = a.report_date


        where sm.shipper_type_id = 12 


      group by 1,2,3,4,5
)
