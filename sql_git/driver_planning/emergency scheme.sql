with metrics as 
(SELECT
    created_date
    , created_hour
    , city_group
    , shipper_id
    , COUNT(DISTINCT order_uid) AS cnt_total_order_delivered
    -- , COUNT(DISTINCT shipper_id) AS a1_drivers
    , cast(COUNT(DISTINCT order_uid) as double)/COUNT(DISTINCT shipper_id) as productivity
FROM
    (SELECT
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

        ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(FROM_UNIXTIME(dot.real_drop_time - 60*60))
            when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(FROM_UNIXTIME(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
            else date(FROM_UNIXTIME(dot.submitted_time- 60*60)) end as report_date
        ,date(FROM_UNIXTIME(dot.submitted_time- 60*60)) created_date
        ,HOUR(FROM_UNIXTIME(dot.submitted_time- 60*60)) AS created_hour
        ,case when dot.real_drop_time = 0 then null else FROM_UNIXTIME(dot.real_drop_time - 60*60) end as last_delivered_timestamp
                                                                                                    
        ,case when dot.pick_city_id = 217 then 'HCM'
            when dot.pick_city_id = 218 then 'HN'
            when dot.pick_city_id = 219 then 'DN'
            ELSE 'OTH' end as city_group
    FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
    LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id
    -- LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on dot.pick_city_id = city.id and city.country_id = 86
    WHERE dot.order_status = 400
    AND date(FROM_UNIXTIME(dot.submitted_time- 60*60)) IN ( DATE '${campaign_date}')
    )
GROUP BY 1,2,3,4
)

select 
        created_date
        ,created_hour
        ,city_group
        ,count(distinct shipper_id) as a1_drivers
        ,sum(cnt_total_order_delivered) cnt_total_order_delivered
        ,sum(cnt_total_order_delivered)/cast(count(distinct shipper_id) as double) as driver_productivity 
        ,approx_percentile(productivity,0.3) as driver_productivity_pct30
        ,approx_percentile(productivity,0.4) as driver_productivity_pct40
        ,approx_percentile(productivity,0.5) as driver_productivity_pct50
        ,approx_percentile(productivity,0.6) as driver_productivity_pct60
        ,approx_percentile(productivity,0.7) as driver_productivity_pct70
        ,approx_percentile(productivity,0.8) as driver_productivity_pct80                                
        ,approx_percentile(productivity,0.9) as driver_productivity_pct90                                       
        ,approx_percentile(productivity,0.95) as driver_productivity_pct95

from metrics

group by 1,2,3
