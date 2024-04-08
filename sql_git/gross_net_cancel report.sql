WITH raw as
(SELECT raw.*
       ,case when raw.source = 'now_ship_shopee' then case when raw.order_status = 'Assigning Timeout' then 1 else 0 end
             when raw.source in ('now_ship_user','now_ship_merchant') then case when raw.last_incharge_timestamp is null 
                and sa.assigning_count > 0 
                and raw.cancel_reason in ('No Reason','Wait for assigning driver too long','Unable to contact driver','Shipper denied Auto Accept order','Mistaken accept') then 1 else 0 end
            when raw.source in ('now_ship_same_day') then case when raw.last_incharge_timestamp is null 
                and raw.cancel_reason in ('No Reason','Wait for assigning driver too long','Unable to contact driver','Shipper denied Auto Accept order','Mistaken accept') then 1 else 0 end
            when raw.source in ('order_food','order_fresh','order_market') then case when raw.cancel_reason = 'No driver' then 1 else 0 end 
             else 0 end as is_no_driver 
           


FROM dev_vnfdbi_opsndrivers.phong_raw_order_v2 raw

LEFT JOIN 
    (SELECT 
             ref_order_id
            ,order_category
            ,COUNT(ref_order_id) AS assigning_count
     FROM dev_vnfdbi_opsndrivers.phong_raw_assignment_test
     WHERE metrics != 'Denied'
     GROUP BY 1,2
     ) sa 
    on sa.ref_order_id = raw.id 
    and sa.order_category = raw.order_type 
WHERE raw.created_date between date'2023-03-01' and date'2023-03-31'
)
SELECT
      created_date
    , created_hour  
    , YEAR(created_date)*100 + WEEK(created_date) as year_week
    , city_
    , source
    , COUNT(DISTINCT shipper_id) AS active_drivers
    , SUM(gross_orders) AS gross_orders
    , SUM(net_orders) AS net_orders
    , SUM(canceled_orders) AS canceled_orders
    , SUM(cancel_no_driver_orders) AS cancel_no_driver_orders
    , TRY(CAST(SUM(net_orders) AS DOUBLE) / COUNT(DISTINCT shipper_id)) AS driver_ado
    , TRY(CAST(SUM(cancel_no_driver_orders) AS DOUBLE) / SUM(gross_orders)) AS canel_no_driver
FROM
(SELECT
      created_date 
    , hour(created_timestamp) AS created_hour
    , if(city_name in ('HCM City','Ha Noi City','Da Nang City','Hai Phong City' ,'Hue City' ,'Can Tho City','Dong Nai','Binh Duong','Vung Tau'),city_name,'OTH') as city_
    , IF(order_status in ('Delivered'), shipper_id, NULL) AS shipper_id
    , CASE 
          WHEN source = 'now_ship_shopee' THEN 'NowShip On Shopee'
          WHEN regexp_like(source,'now_ship') = true THEN 'NowShip Off Shopee' 
          ELSE source END AS source
    , COUNT(DISTINCT uid) as gross_orders
    , COUNT(DISTINCT if(order_status in ('Delivered'), uid, null)) as net_orders
    , COUNT(DISTINCT if(order_status not in ('Delivered','Returned'), uid, null)) as canceled_orders
    , COUNT(DISTINCT if(is_no_driver = 1, uid, null)) as cancel_no_driver_orders

FROM raw
GROUP BY 1,2,3,4,5
)

GROUP BY 1,2,3,4,5