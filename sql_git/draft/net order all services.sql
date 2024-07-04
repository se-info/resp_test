WITH snp AS 
(SELECT
    -- report_date
     CAST(DATE_FORMAT(DATE_TRUNC('month', report_date),'%c') as double) AS report_month
    ,city_group
    , COUNT(DISTINCT order_uid)*1.00/count(distinct report_date) AS gross_orders
    , COUNT(DISTINCT IF(order_status = 400, order_uid, NULL))*1.00/count(distinct report_date) AS net_orders
    , COUNT(DISTINCT IF(order_status = 400 AND source = 'order_delivery', order_uid, NULL))*1.00/count(distinct report_date) AS net_food_orders
    , COUNT(DISTINCT IF(order_status = 400 AND source != 'order_delivery', order_uid, NULL))*1.00/count(distinct report_date) AS net_ship_orders
    , count(distinct case when order_status = 400 and source ='order_delivery' and foody_service = 'Food' then order_uid else null end)*1.00/count(distinct report_date) as  net_food_orders_v1
    , count(distinct case when order_status = 400 and source ='order_delivery' and foody_service = 'Fresh' then order_uid else null end)*1.00/count(distinct report_date) as  net_fresh_orders_v1
    , count(distinct case when order_status = 400 and source !='order_delivery' then order_uid else null end)*1.00/count(distinct report_date) as net_ship_orders

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

        ,case when dot.real_drop_time = 0 then null else FROM_UNIXTIME(dot.real_drop_time - 60*60) end as last_delivered_timestamp
        --   ,case when dot.pick_city_id = 238 THEN 'Dien Bien' else city.city_name end as city_name
        ,case when dot.pick_city_id = 217 then 'HCM'
            when dot.pick_city_id = 218 then 'HN'
            when dot.pick_city_id = 219 then 'DN'
            ELSE 'OTH' end as city_group
        ,case when oct.foody_service_id = 1 then 'Food'
                else 'Fresh' end as foody_service
            -- when oct.foody_service_id = 5 then 'Fresh'
            -- else 'Market' end as foody_service
    FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
    LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id
    LEFT JOIN shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct on oct.id = dot.ref_order_id and dot.ref_order_category = 0
    WHERE dot.order_status in (400,401,402,403,404,405,406,407)
    AND case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(FROM_UNIXTIME(dot.real_drop_time - 60*60))
            when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(FROM_UNIXTIME(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
            else date(FROM_UNIXTIME(dot.submitted_time- 60*60)) end BETWEEN DATE'2022-01-01' AND current_date - interval '1' day
    )
GROUP BY 1,2

)


select 
    *

from snp

where report_month between 4 and 6 

-- SELECT * 
-- FROM 
--     (SELECT 
--         report_month
--         , report_date
--         , gross_orders
--         , net_orders
--         , net_food_orders
--         , net_ship_orders
--         , RANK() OVER (PARTITION BY report_month ORDER BY net_orders DESC) AS ranking
--     FROM snp)
-- WHERE ranking IN (1,2,3)