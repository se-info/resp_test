-- gross cancel
with 
params(period_group, period, start_date, end_date, days) AS 
(VALUES 
('M-12','2022-12',date'2022-12-01',date'2022-12-31',31)
)
,raw_cancel as 
(
SELECT grass_date 
      ,id 
      ,merchant_id
      ,case when status = 8 then 
            case when cancel_reason = 'I made duplicate orders' then 'Made duplicate orders'
                 when cancel_reason = 'Make another order' then 'Buyer want to cancel order'
                 when cancel_reason = 'I am busy and cannot receive order' then 'Busy and cannot receive order'
                 when cancel_reason in ('Confirmed the order too late','Affected by quarantine area','Order limit due to Covid','Preorder','Missing reason','Other reason - Buyer','Other reason - Merchant','Other reason - Driver') then 'Others'
                 when cancel_reason is null then 'Others' else coalesce(cancel_reason,'Not Cancel') end
            else 'Not Cancel' end as cancel_reason
      ,case when status = 8 then 
            case when cancel_reason is null then null
                 when cancel_reason in ('No driver') and cancel_actor = 'Buyer' and osl.cancel_time - a.create_time < 60 then '1. Buyer Voluntary Cancellation'
                 when cancel_reason in ('No driver','Other reason - Driver') then '2. Buyer Non-voluntary Cancellation'
                 when cancel_reason in ('Out of stock', 'Shop closed','Shop busy','Shop did not confirm','Shop did not confirm order','Wrong price','Other reason - Merchant') then '2. Buyer Non-voluntary Cancellation'
                 when cancel_reason in ('Pending status from bank') then '2. Buyer Non-voluntary Cancellation'
                 when cancel_reason in ('Payment failed') then '2. Buyer Non-voluntary Cancellation'
                 when cancel_reason in ('Affected by quarantine area','Order limit due to Covid') then '2. Buyer Non-voluntary Cancellation'
                 else '1. Buyer Voluntary Cancellation' end 
            else 'Not Cancel' end as cancel_type      
      ,foody_service_id
      ,if(city_tier = 'T1', city_group, city_tier) city_group
      ,status

FROM vnfdbi_opsndrivers.shopeefood_vn_bnp_ops_order_detail_tab__vn_daily_s0_live a 
left join 
        (
        select order_id 
            ,max(case when status = 8 then create_time  else null end) as cancel_time
        from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
        group by 1
        )osl on osl.order_id = a.id 
)

select 
          p.period_group 
        , p.period
        , p.days AS days
        , coalesce(try(1.00000000/ p.days * sum(ado)),0) as gross_ado_all

        , coalesce(try(1.00000000* sum(if(cancel_reason != 'Not Cancel',ado,0))/sum(ado)),0) as p_cancel_vn
        , coalesce(try(1.00000000* sum(if(city_group = 'HCM' and cancel_reason != 'Not Cancel',ado,0))/sum(if(city_group = 'HCM',ado,0))),0) as p_cancel_hcm
        , coalesce(try(1.00000000* sum(if(city_group = 'HN' and cancel_reason != 'Not Cancel',ado,0))/sum(if(city_group = 'HN',ado,0))),0) as p_cancel_hn 
        , coalesce(try(1.00000000* sum(if(city_group = 'DN' and cancel_reason != 'Not Cancel',ado,0))/sum(if(city_group = 'DN',ado,0))),0) as p_cancel_dn
        , coalesce(try(1.00000000* sum(if(city_group = 'T2' and cancel_reason != 'Not Cancel',ado,0))/sum(if(city_group = 'T2',ado,0))),0) as p_cancel_t2 
        , coalesce(try(1.00000000* sum(if(city_group = 'T3' and cancel_reason != 'Not Cancel',ado,0))/sum(if(city_group = 'T3',ado,0))),0) as p_cancel_t3 

        , coalesce(try(1.00000000* sum(if(service = 'Food' and cancel_reason != 'Not Cancel',ado,0))/sum(if(service = 'Food',ado,0))),0) as p_cancel_food 
        , coalesce(try(1.00000000* sum(if(service = 'Fresh' and cancel_reason != 'Not Cancel',ado,0))/sum(if(service = 'Fresh',ado,0))),0) as p_cancel_fresh

        , coalesce(try(1.00000000* sum(if(cancel_type = '1. Buyer Voluntary Cancellation' and cancel_reason != 'Not Cancel',ado,0))/sum(ado)),0) as p_cancel_buyer_voluntary
        , coalesce(try(1.00000000* sum(if(cancel_type = '1. Buyer Voluntary Cancellation' and cancel_reason = 'No driver',ado,0))/sum(ado)),0) as p_cancel_buyer_voluntary_no_driver
        , coalesce(try(1.00000000* sum(if(cancel_type = '1. Buyer Voluntary Cancellation' and cancel_reason = 'Change item/Merchant',ado,0))/sum(ado)),0) as p_cancel_change_item
        , coalesce(try(1.00000000* sum(if(cancel_type = '1. Buyer Voluntary Cancellation' and cancel_reason = 'Change payment method',ado,0))/sum(ado)),0) as p_cancel_change_payment
        , coalesce(try(1.00000000* sum(if(cancel_type = '1. Buyer Voluntary Cancellation' and cancel_reason = 'Incomplete payment',ado,0))/sum(ado)),0) as p_cancel_incomplete_payment
        , coalesce(try(1.00000000* sum(if(cancel_type = '1. Buyer Voluntary Cancellation' and cancel_reason = 'Made duplicate orders',ado,0))/sum(ado)),0) as p_cancel_duplicate_order
        , coalesce(try(1.00000000* sum(if(cancel_type = '1. Buyer Voluntary Cancellation' and cancel_reason = 'Change delivery time',ado,0))/sum(ado)),0) as p_cancel_change_time
        , coalesce(try(1.00000000* sum(if(cancel_type = '1. Buyer Voluntary Cancellation' and cancel_reason = 'Busy and cannot receive order',ado,0))/sum(ado)),0) as p_cancel_cannot_receive
        , coalesce(try(1.00000000* sum(if(cancel_type = '1. Buyer Voluntary Cancellation' and cancel_reason = 'Forgot inputting discount code',ado,0))/sum(ado)),0) as p_cancel_forget_discount
        , coalesce(try(1.00000000* sum(if(cancel_type = '1. Buyer Voluntary Cancellation' and cancel_reason = 'Buyer want to cancel order',ado,0))/sum(ado)),0) as p_cancel_buyer_want_cancel
        , coalesce(try(1.00000000* sum(if(cancel_type = '1. Buyer Voluntary Cancellation' and cancel_reason = 'Think again on price and fees',ado,0))/sum(ado)),0) as p_cancel_buyer_think_again
        , coalesce(try(1.00000000* sum(if(cancel_type = '1. Buyer Voluntary Cancellation' and cancel_reason = 'Others',ado,0))/sum(ado)),0) as p_cancel_buyer_voluntary_others

        , coalesce(try(1.00000000* sum(if(cancel_type = '2. Buyer Non-voluntary Cancellation' and cancel_reason != 'Not Cancel',ado,0))/sum(ado)),0) as p_cancel_buyer_non_voluntary
        , coalesce(try(1.00000000* sum(if(cancel_type = '2. Buyer Non-voluntary Cancellation' and cancel_reason = 'Payment failed',ado,0))/sum(ado)),0) as p_cancel_payment_failed
        , coalesce(try(1.00000000* sum(if(cancel_type = '2. Buyer Non-voluntary Cancellation' and cancel_reason = 'Out of stock',ado,0))/sum(ado)),0) as p_cancel_oos
        , coalesce(try(1.00000000* sum(if(cancel_type = '2. Buyer Non-voluntary Cancellation' and cancel_reason = 'Shop closed',ado,0))/sum(ado)),0) as p_cancel_shop_closed
        , coalesce(try(1.00000000* sum(if(cancel_type = '2. Buyer Non-voluntary Cancellation' and cancel_reason = 'No driver',ado,0))/sum(ado)),0) as p_cancel_buyer_non_voluntary_no_driver
        , coalesce(try(1.00000000* sum(if(cancel_type = '2. Buyer Non-voluntary Cancellation' and cancel_reason = 'Shop busy',ado,0))/sum(ado)),0) as p_cancel_shop_busy
        , coalesce(try(1.00000000* sum(if(cancel_type = '2. Buyer Non-voluntary Cancellation' and cancel_reason = 'Wrong price',ado,0))/sum(ado)),0) as p_cancel_wrong_price
        , coalesce(try(1.00000000* sum(if(cancel_type = '2. Buyer Non-voluntary Cancellation' and cancel_reason = 'Shop did not confirm order',ado,0))/sum(ado)),0) as p_cancel_shop_not_confim
        , coalesce(try(1.00000000* sum(if(cancel_type = '2. Buyer Non-voluntary Cancellation' and cancel_reason = 'Pending status from bank',ado,0))/sum(ado)),0) as p_cancel_pending_bank
        , coalesce(try(1.00000000* sum(if(cancel_type = '2. Buyer Non-voluntary Cancellation' and cancel_reason = 'Others',ado,0))/sum(ado)),0) as p_cancel_buyer_non_voluntary_others

from 
(
select  grass_date 
      , coalesce(cancel_reason, 'Not Cancel') cancel_reason
      , coalesce(cancel_type, 'Not Cancel') cancel_type
      , case when foody_service_id = 1 then 'Food'
             when foody_service_id = 5 then 'Fresh'
             else 'Mart' end service 
      , city_group 
      , count(distinct id) ado

from raw_cancel 

where 1=1 -- grass_date = date'2023-02-01'
group by 1,2,3,4,5
)a 

INNER JOIN params p ON a.grass_date BETWEEN p.start_date AND p.end_date

group by 1,2,3
;
-- net metrics
WITH params(period, start_date, end_date, days) AS (
    VALUES
    ('M-12',DATE'2022-12-01',DATE'2022-12-31',31)
    )
, base AS (
SELECT
    base.created_date
    , COUNT(DISTINCT base.order_uid) AS gross_orders
    , COUNT(DISTINCT IF(base.city_group = 'HCM', base.order_uid, NULL)) AS hcm_gross_orders
    , COUNT(DISTINCT IF(base.city_group = 'HN', base.order_uid, NULL)) AS hn_gross_orders
    , COUNT(DISTINCT IF(base.city_group = 'DN', base.order_uid, NULL)) AS dn_gross_orders
    , COUNT(DISTINCT IF(base.city_name IN ('Hai Phong City', 'Hue City', 'Can Tho City', 'Dong Nai', 'Binh Duong', 'Vung Tau'), base.order_uid, NULL)) AS t2_gross_orders
    , COUNT(DISTINCT IF(base.city_name NOT IN ('HCM City', 'Ha Noi City', 'Da Nang City', 'Hai Phong City', 'Hue City', 'Can Tho City', 'Dong Nai', 'Binh Duong', 'Vung Tau'), base.order_uid, NULL)) AS t3_gross_orders
    , COUNT(DISTINCT IF(base.status = 7, base.order_uid, NULL)) AS net_orders
    , COUNT(DISTINCT IF(base.city_group = 'HCM' AND base.status = 7, base.order_uid, NULL)) AS hcm_net_orders
    , COUNT(DISTINCT IF(base.city_group = 'HN' AND base.status = 7, base.order_uid, NULL)) AS hn_net_orders
    , COUNT(DISTINCT IF(base.city_group = 'DN' AND base.status = 7, base.order_uid, NULL)) AS dn_net_orders
    , COUNT(DISTINCT IF(base.city_name IN ('Hai Phong City', 'Hue City', 'Can Tho City', 'Dong Nai', 'Binh Duong', 'Vung Tau') AND status = 7, base.order_uid, NULL)) AS t2_net_orders
    , COUNT(DISTINCT IF(base.city_name NOT IN ('HCM City', 'Ha Noi City', 'Da Nang City', 'Hai Phong City', 'Hue City', 'Can Tho City', 'Dong Nai', 'Binh Duong', 'Vung Tau') AND base.status = 7, base.order_uid, NULL)) AS t3_net_orders

    , COUNT(DISTINCT IF(base.distance <= 1, base.order_uid, NULL)) AS gross_orders_0_1km
    , COUNT(DISTINCT IF(base.distance > 1 AND base.distance <= 3, base.order_uid, NULL)) AS gross_orders_1_3km
    , COUNT(DISTINCT IF(base.distance > 3 AND base.distance <= 5, base.order_uid, NULL)) AS gross_orders_3_5km
    , COUNT(DISTINCT IF(base.distance > 5 AND base.distance <= 7, base.order_uid, NULL)) AS gross_orders_5_7km
    , COUNT(DISTINCT IF(base.distance > 7 AND base.distance <= 10, base.order_uid, NULL)) AS gross_orders_7_10km
    , COUNT(DISTINCT IF(base.distance > 10, base.order_uid, NULL)) AS gross_orders_10km
    , COUNT(DISTINCT IF(base.distance <= 1 AND base.status = 7, base.order_uid, NULL)) AS net_orders_0_1km
    , COUNT(DISTINCT IF(base.distance > 1 AND base.distance <= 3 AND base.status = 7, base.order_uid, NULL)) AS net_orders_1_3km
    , COUNT(DISTINCT IF(base.distance > 3 AND base.distance <= 5 AND base.status = 7, base.order_uid, NULL)) AS net_orders_3_5km
    , COUNT(DISTINCT IF(base.distance > 5 AND base.distance <= 7 AND base.status = 7, base.order_uid, NULL)) AS net_orders_5_7km
    , COUNT(DISTINCT IF(base.distance > 7 AND base.distance <= 10 AND base.status = 7, base.order_uid, NULL)) AS net_orders_7_10km
    , COUNT(DISTINCT IF(base.distance > 10 AND base.status = 7, base.order_uid, NULL)) AS net_orders_10km

    , COUNT(DISTINCT IF(base.order_value <= 500000, base.order_uid, NULL)) AS gross_orders_500k
    , COUNT(DISTINCT IF(base.order_value > 500000 AND base.order_value <= 1000000, base.order_uid, NULL)) AS gross_orders_500_1000k
    , COUNT(DISTINCT IF(base.order_value > 1000000 AND base.order_value <= 1500000, base.order_uid, NULL)) AS gross_orders_1000_1500k
    , COUNT(DISTINCT IF(base.order_value > 1500000 AND base.order_value <= 2000000, base.order_uid, NULL)) AS gross_orders_1500_2000k
    , COUNT(DISTINCT IF(base.order_value > 2000000, base.order_uid, NULL)) AS gross_orders_2000k
    , COUNT(DISTINCT IF(base.order_value <= 500000 AND base.status = 7, base.order_uid, NULL)) AS net_orders_500k
    , COUNT(DISTINCT IF(base.order_value > 500000 AND base.order_value <= 1000000 AND base.status = 7, base.order_uid, NULL)) AS net_orders_500_1000k
    , COUNT(DISTINCT IF(base.order_value > 1000000 AND base.order_value <= 1500000 AND base.status = 7, base.order_uid, NULL)) AS net_orders_1000_1500k
    , COUNT(DISTINCT IF(base.order_value > 1500000 AND base.order_value <= 2000000 AND base.status = 7, base.order_uid, NULL)) AS net_orders_1500_2000k
    , COUNT(DISTINCT IF(base.order_value > 2000000 AND base.status = 7, base.order_uid, NULL)) AS net_orders_2000k

    , COUNT(DISTINCT IF(base.foody_service_id = 1 AND base.status = 7, base.order_uid, NULL)) AS net_orders_food 
    , COUNT(DISTINCT IF(base.foody_service_id != 1 AND base.status = 7, base.order_uid, NULL)) AS net_orders_market 
    , COUNT(DISTINCT IF(base.foody_service_id = 1 , base.order_uid, NULL)) AS gross_orders_food 
    , COUNT(DISTINCT IF(base.foody_service_id != 1 , base.order_uid, NULL)) AS gross_orders_market        
FROM
    (SELECT
        oct.shipper_uid as shipper_id
        ,oct.id as order_uid
        ,oct.order_code as order_code
        ,oct.status
        ,date(from_unixtime(oct.submit_time - 3600)) created_date
        ,from_unixtime(oct.submit_time - 3600) created_timestamp
        ,case when oct.city_id = 238 THEN 'Dien Bien' else city.name_en end as city_name
        ,case when oct.city_id = 217 then 'HCM'
            when oct.city_id = 218 then 'HN'
            when oct.city_id = 219 then 'DN'
            ELSE 'OTH' end as city_group
        ,oct.city_id
        ,oct.is_asap
        ,cast(oct.distance as double)  as distance
        ,cast(oct.sub_total as double) / 100 as order_value
        ,foody_service_id

    FROM shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct 
    LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on dot.ref_order_id = oct.id and dot.ref_order_id = 0
    LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id
    LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = oct.city_id and city.country_id = 86

    WHERE 1=1
    and oct.city_id not in (0,238,468,469,470,471,472)

    ) base
WHERE 1=1
and base.created_date BETWEEN DATE'2022-12-01' AND DATE'2022-12-31'

GROUP BY 1
)

SELECT
    p.period
    , p.days AS days
    , SUM(b.gross_orders) / p.days AS vn_gross_ado
    , SUM(b.net_orders) / p.days AS vn_net_ado
    , SUM(b.hcm_gross_orders) / p.days AS hcm_gross_ado
    , SUM(b.hcm_net_orders) / p.days AS hcm_net_ado
    , SUM(b.hn_gross_orders) / p.days AS hn_gross_ado
    , SUM(b.hn_net_orders) / p.days AS hn_net_ado
    , SUM(b.dn_gross_orders) / p.days AS dn_gross_ado
    , SUM(b.dn_net_orders) / p.days AS dn_net_ado
    , SUM(b.t2_gross_orders) / p.days AS t2_gross_ado
    , SUM(b.t2_net_orders) / p.days AS t2_net_ado
    , SUM(b.t3_gross_orders) / p.days AS t3_gross_ado
    , SUM(b.t3_net_orders) / p.days AS t3_net_ado
    , SUM(b.net_orders_0_1km) / p.days AS net_ado_0_1km
    , SUM(b.net_orders_1_3km) / p.days AS net_ado_1_3km
    , SUM(b.net_orders_3_5km) / p.days AS net_ado_3_5km
    , SUM(b.net_orders_5_7km) / p.days AS net_ado_5_7km
    , SUM(b.net_orders_7_10km) / p.days AS net_ado_7_10km
    , SUM(b.net_orders_10km) / p.days AS net_ado_10km
    , SUM(b.gross_orders_0_1km) / p.days AS gross_ado_0_1km
    , SUM(b.gross_orders_1_3km) / p.days AS gross_ado_1_3km
    , SUM(b.gross_orders_3_5km) / p.days AS gross_ado_3_5km
    , SUM(b.gross_orders_5_7km) / p.days AS gross_ado_5_7km
    , SUM(b.gross_orders_7_10km) / p.days AS gross_ado_7_10km
    , SUM(b.gross_orders_10km) / p.days AS gross_ado_10km
     , SUM(b.gross_orders_500k) / p.days AS gross_ado_500k
    , SUM(b.gross_orders_500_1000k) / p.days AS gross_ado_500_1000k
    , SUM(b.gross_orders_1000_1500k) / p.days AS gross_ado_1000_1500k
    , SUM(b.gross_orders_1500_2000k) / p.days AS gross_ado_1500_2000k
    , SUM(b.gross_orders_2000k) / p.days AS gross_ado_2000k
     , SUM(b.net_orders_500k) / p.days AS net_ado_500k
    , SUM(b.net_orders_500_1000k) / p.days AS net_ado_500_1000k
    , SUM(b.net_orders_1000_1500k) / p.days AS net_ado_1000_1500k
    , SUM(b.net_orders_1500_2000k) / p.days AS net_ado_1500_2000k
    , SUM(b.net_orders_2000k) / p.days AS net_ado_2000k
    , TRY(SUM(b.net_orders_500k) / CAST(SUM(b.gross_orders_500k) AS DOUBLE)) AS g2n_500k
    , TRY(SUM(b.net_orders_500_1000k) / CAST(SUM(b.gross_orders_500_1000k) AS DOUBLE)) AS g2n_500_1000k
    , TRY(SUM(b.net_orders_1000_1500k) / CAST(SUM(b.gross_orders_1000_1500k) AS DOUBLE)) AS g2n_1000_1500k
    , TRY(SUM(b.net_orders_1500_2000k) / CAST(SUM(b.gross_orders_1500_2000k) AS DOUBLE)) AS g2n_1500_2000k
    , TRY(SUM(b.net_orders_2000k) / CAST(SUM(b.gross_orders_2000k) AS DOUBLE)) AS g2n_2000k
    , TRY(SUM(b.net_orders_0_1km) / CAST(SUM(b.gross_orders_0_1km) AS DOUBLE)) AS g2n_0_1km
    , TRY(SUM(b.net_orders_1_3km) / CAST(SUM(b.gross_orders_1_3km) AS DOUBLE)) AS g2n_1_3km
    , TRY(SUM(b.net_orders_3_5km) / CAST(SUM(b.gross_orders_3_5km) AS DOUBLE)) AS g2n_3_5km
    , TRY(SUM(b.net_orders_5_7km) / CAST(SUM(b.gross_orders_5_7km) AS DOUBLE)) AS g2n_5_7km
    , TRY(SUM(b.net_orders_7_10km) / CAST(SUM(b.gross_orders_7_10km) AS DOUBLE)) AS g2n_7_10km
    , TRY(SUM(b.net_orders_10km) / CAST(SUM(b.gross_orders_10km) AS DOUBLE)) AS g2n_10km
    , TRY(SUM(b.net_orders) / CAST(SUM(b.gross_orders) AS DOUBLE)) AS vn_g2n
    , TRY(SUM(b.hcm_net_orders) / CAST(SUM(b.hcm_gross_orders) AS DOUBLE)) AS hcm_g2n
    , TRY(SUM(b.hn_net_orders) / CAST(SUM(b.hn_gross_orders) AS DOUBLE)) AS hn_g2n
    , TRY(SUM(b.dn_net_orders) / CAST(SUM(b.dn_gross_orders) AS DOUBLE)) AS dn_g2n
    , TRY(SUM(b.t2_net_orders) / CAST(SUM(b.t2_gross_orders) AS DOUBLE)) AS t2_g2n
    , TRY(SUM(b.t3_net_orders) / CAST(SUM(b.t3_gross_orders) AS DOUBLE)) AS t3_g2n
    , TRY(SUM(b.net_orders_food) / CAST(SUM(b.gross_orders_food) AS DOUBLE)) AS food_g2n
    , TRY(SUM(b.net_orders_market) / CAST(SUM(b.gross_orders_market) AS DOUBLE)) AS market_g2n    
FROM base b
INNER JOIN params p ON b.created_date BETWEEN p.start_date AND p.end_date
GROUP BY 1,2