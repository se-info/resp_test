WITH raw AS 
(SELECT
    CASE WHEN source = 'now_ship_shopee' THEN 'Ecom' ELSE 'Instant' END as new_source,
    created_month, order_code, source, order_status, cancel_by, cancel_type,created_date,
    -- created_timestamp, order_weight,
    CASE 
        WHEN ceiling_distance <= 5 THEN '0. <=5'
        WHEN ceiling_distance <= 10 THEN '1. 5-10'
        WHEN ceiling_distance <= 15 THEN '2. 10-15'
        WHEN ceiling_distance <= 20 THEN '3. 15-20'
        ELSE '4. > 20' END as distance_range,
    CASE 
        WHEN mins <= 11*60 THEN '0. Early morning'
        WHEN mins <= 14*60 THEN '1. Lunch'
        WHEN mins <= 18*60 THEN '2. Afternoon Off-peak'
        WHEN mins <= 21*60 THEN '3. Dinner'
        ELSE '4.Late Night' END as timeframe,
    CASE 
        WHEN coalesce(order_weight,0) <= 2000 THEN '0. 2kg'
        WHEN coalesce(order_weight,0) <= 4000 THEN '1. 4kg'
        WHEN coalesce(order_weight,0) <= 6000 THEN '2. 6kg'
        WHEN coalesce(order_weight,0) <= 6000 THEN '3. 6kg'
        WHEN coalesce(order_weight,0) <= 8000 THEN '4. 8kg'
        WHEN coalesce(order_weight,0) <= 10000 THEN '5. 10kg'
        ELSE '6. > 10 kg' END as weight_range,
    CASE
        WHEN pay_by_type = 'non cod' THEN '0. non cod' 
        WHEN cod_value <= 500000 THEN '1. 0-500k'
        WHEN cod_value <= 1000000 THEN '2. 500-1000k'
        WHEN cod_value <= 3000000 THEN '3. 1000-3000k'
        WHEN cod_value > 3000000 THEN '4. 3000k++' 
        END AS cod_range,
    CASE 
    WHEN order_status IN ('Delivered','Returned') THEN 1 ELSE 0 END AS is_net,        
    case 
        when order_status in ('Delivered','Returned','Others') then order_status 
        WHEN order_status = 'Pickup Failed' or lower(cancel_type) = 'driver_cancelled' THEN 'Cancel by Driver'
        WHEN order_status IN ('Cancelled','Assigning Timeout') then 
                case when order_status IN ('Assigning Timeout') then 'Cancel by Platform'
                        when cancel_reason = 'No Reason' and source = 'now_ship_shopee' then 'Cancel by Shopee'
                        when lower(cancel_type) = 'user_cancelled' then 'Cancel by User'
                        else 'Cancel by Platform' end end as order_type,

    CASE WHEN order_status = 'Pickup Failed' THEN pick_failed_reason
        when cancel_reason = 'No Reason' and source = 'now_ship_shopee' THEN 'Shopee Cancelled'
        WHEN order_status = 'Cancelled' THEN cancel_reason
        when order_status = 'Assigning Timeout' THEN 'Assigning Timeout' 
        ELSE NULL END as cancel_reason_v2 ,
    CASE
    WHEN order_status IN ('Assigning Timeout') then 'Time out' ELSE COALESCE(cancel_reason,pick_failed_reason) END AS cancel_reason,
    CASE
    WHEN regexp_like(lower(city_name),'hcm|ha noi|da nang') THEN city_name
    ELSE 'OTH' END AS city_group

FROM (
    SELECT 
        ns.source, ns.cancel_by, ns.cancel_type,ns.city_name,
        DATE_FORMAT(created_date, '%Y-%m') created_month,created_date,
        ns.order_code, ns.order_status, ns.cancel_reason,
        CEILING(ns.distance) ceiling_distance, ns.created_timestamp,
        HOUR(ns.created_timestamp)*60 + MINUTE(ns.created_timestamp) mins,
        weight.order_weight,
        weight.pay_by_type,
        weight.cod_value,ns.pick_failed_reason

    FROM vnfdbi_opsndrivers.ns_performance_tab ns

    LEFT JOIN (
                SELECT
                    code, created_at,
                    pay_by_type,
                    cod_value*1.000/100 AS cod_value,
                    SUM(CAST(t.item['weight'] AS DOUBLE)) order_weight 

                FROM (
                    SELECT 
                        code, from_unixtime(create_time - 3600) created_at,
                        CAST(json_extract(extra_data, '$.items') AS ARRAY(MAP(VARCHAR, JSON))) item_json,
                        cod_value,
                        CASE 
                        WHEN cod_value > 0 then 'cod' else 'non cod' end as pay_by_type

                    FROM shopeefood.foody_express_db__shopee_booking_tab__reg_daily_s0_live
                    WHERE  -- to make sure cover diff between adcountant table
                    (DATE(from_unixtime(create_time - 3600)) BETWEEN DATE'2022-12-01' AND DATE'2022-12-31'
           OR DATE(from_unixtime(create_time - 3600)) BETWEEN DATE'2021-12-01' AND DATE'2021-12-31'
           OR DATE(from_unixtime(create_time - 3600)) BETWEEN DATE'2023-07-01' AND DATE'2023-08-31')
                   -- AND status IN (11, 14)
                    ) items

                CROSS JOIN UNNEST (item_json) AS t(item)
                GROUP BY 1, 2, 3, 4

                UNION ALL 
                    
                SELECT 
                    bt.code, bt.created_at,pay_by_type,bill_amount AS cod_value, bt.order_weight
                FROM (
                        SELECT 
                            code, from_unixtime(create_time - 3600) created_at,
                            CAST(json_extract(extra_data, '$.item_info.item_weight') AS DOUBLE)*1000 order_weight,
                            CAST(JSON_EXTRACT(extra_data,'$.receiver_info.bill_amount') AS DOUBLE) bill_amount,
                            CASE 
                            WHEN CAST(JSON_EXTRACT(extra_data,'$.pay_by_type') AS DOUBLE) = 1 THEN 'cod'
                            ELSE 'non cod' END AS pay_by_type

                        FROM shopeefood.foody_express_db__booking_tab__reg_continuous_s0_live
                        WHERE  -- to make sure cover diff between adcountant table
                        (DATE(from_unixtime(create_time - 3600)) BETWEEN DATE'2022-12-01' AND DATE'2022-12-31'
           OR DATE(from_unixtime(create_time - 3600)) BETWEEN DATE'2021-12-01' AND DATE'2021-12-31'
           OR DATE(from_unixtime(create_time - 3600)) BETWEEN DATE'2023-07-01' AND DATE'2023-08-31')
                       -- AND status IN (11, 14)
                        ) bt 
                ) weight 
                
                ON ns.order_code = weight.code

    WHERE (ns.created_date BETWEEN DATE'2022-12-01' AND DATE'2022-12-31'
           OR ns.created_date BETWEEN DATE'2021-12-01' AND DATE'2021-12-31'
           OR ns.created_date BETWEEN DATE'2023-07-01' AND DATE'2023-08-31')
)
)            
SELECT 
        created_month,
        order_type,
        CASE
        WHEN cancel_reason_v2 = 'Assigning Timeout' THEN 'Assigning Timeout'
        WHEN cancel_reason_v2 = 'Change delivery time' THEN 'Change delivery time'
        WHEN cancel_reason_v2 = 'Cheating' THEN 'Cheating'
        WHEN cancel_reason_v2 = 'Chưa lấy được hàng do thời tiết xấu' THEN 'Bad weather'
        WHEN cancel_reason_v2 = 'Couldn’t find biker' THEN 'Couldn’t find biker'
        WHEN cancel_reason_v2 = 'Địa chỉ lấy/giao không cùng quận' THEN 'Incorrect address'
        WHEN cancel_reason_v2 = 'Driver asks for order cancellation' THEN 'Driver asks for order cancellation'
        WHEN cancel_reason_v2 = 'Driver is too far from pickup point' THEN 'Driver is too far from pickup point'
        WHEN cancel_reason_v2 = 'Forget to input promocode' THEN 'Forget to input promocode'
        WHEN cancel_reason_v2 = 'Incorrect route/Incorrect latlng' THEN 'Incorrect route/Incorrect latlng'
        WHEN cancel_reason_v2 = 'Input wrong/incomplete delivery information' THEN 'Input wrong/incomplete delivery information'
        WHEN cancel_reason_v2 = 'Không liên hệ được Người gửi' THEN 'Can''t contact Seller'
        WHEN cancel_reason_v2 = 'Không liên hệ được Người nhận' THEN 'Can''t contact Buyer'
        WHEN cancel_reason_v2 = 'Kiện hàng gửi trong danh mục hàng cấm' THEN 'Package not in delivery list'
        WHEN cancel_reason_v2 = 'Kiện hàng sai tiêu chuẩn về khối lượng/ kích thước.' THEN 'Wrong Size'
        WHEN cancel_reason_v2 = 'Kiện hàng sai tiêu chuẩn về khối lượng/ kích thước/ quy cách đóng gói' THEN 'Wrong Size'
        WHEN cancel_reason_v2 = 'Kiện hàng sai tiêu chuẩn về khối lượng/ kích thước/quy cách đóng gói' THEN 'Wrong Size'
        WHEN cancel_reason_v2 = 'Kiện hàng sai tiêu chuẩn về khối lượng/kích thước' THEN 'Wrong Size'
        WHEN cancel_reason_v2 = 'Made duplicate orders' THEN 'Made duplicate orders'
        WHEN cancel_reason_v2 = 'Người gửi báo hủy đơn/ không thể giao hàng' THEN 'Seller request cancel'
        WHEN cancel_reason_v2 = 'Người gửi đóng gói không cẩn thận/ chưa đóng gói xong' THEN 'Order not prepared'
        WHEN cancel_reason_v2 = 'Người nhận báo hủy đơn/ không thể nhận hàng' THEN 'Buyer request cancel'
        WHEN cancel_reason_v2 = 'Người nhận hẹn giao lại sau' THEN 'Change delivery time'
        WHEN cancel_reason_v2 = 'Người nhận hẹn giao lại sau (sau 1 giờ)' THEN 'Change delivery time'
        WHEN cancel_reason_v2 = 'Người nhận thay đổi địa chỉ' THEN 'Change address'
        WHEN cancel_reason_v2 = 'No Reason' THEN 'No Reason'
        WHEN cancel_reason_v2 = 'Others' THEN 'Others'
        WHEN cancel_reason_v2 = 'Personal reasons' THEN 'Personal reasons'
        WHEN cancel_reason_v2 = 'Recipient asks to cancel the order' THEN 'Recipient asks to cancel the order'
        WHEN cancel_reason_v2 = 'Recipient changed delivery address' THEN 'Recipient changed delivery address'
        WHEN cancel_reason_v2 = 'Returning to hub reasons: Không liên hệ được Người gửi' THEN 'Returning reasons'
        WHEN cancel_reason_v2 = 'Returning to hub reasons: Người gửi báo không thể trả hàng' THEN 'Returning reasons'
        WHEN cancel_reason_v2 = 'Sai định vị Người Gửi' THEN 'Incorrect seller address'
        WHEN cancel_reason_v2 = 'Sai định vị Người Nhận' THEN 'Incorrect buyer address'
        WHEN cancel_reason_v2 = 'Sender requests to get cash in advance' THEN 'Sender requests to get cash in advance'
        WHEN cancel_reason_v2 = 'Shipper denied Auto Accept order' THEN 'Shipper denied Auto Accept order'
        WHEN cancel_reason_v2 = 'Shopee Cancelled' THEN 'Shopee Cancelled'
        WHEN cancel_reason_v2 = 'Shopee yêu cầu hủy đơn' THEN 'Shopee Cancelled'
        WHEN cancel_reason_v2 = 'Think again on price and fees' THEN 'Think again on price and fees'
        WHEN cancel_reason_v2 = 'Unable to contact driver' THEN 'Unable to contact driver'
        WHEN cancel_reason_v2 = 'Unable to contact recipient' THEN 'Unable to contact recipient'
        WHEN cancel_reason_v2 = 'Wait for assigning driver too long' THEN 'Wait for assigning driver too long'
        ELSE 'No reason'
        END AS cancel_reason,
        COUNT(DISTINCT order_code)*1.000/COUNT(DISTINCT created_date) AS num_cancel 

FROM raw 

WHERE regexp_like(order_type,'Cancel') = true
AND source = 'now_ship_shopee'
GROUP BY 1,2,3 
--  * FROM raw WHERE order_type = 'Cancel by Driver' AND cancel_reason IS NULL        
--         created_month,
--         COALESCE(city_group,'VN') AS metrics,
--         COUNT(DISTINCT CASE WHEN is_net = 1 THEN order_code ELSE NULL END)*1.00/COUNT(DISTINCT order_code) AS g2n,
--         COUNT(DISTINCT CASE WHEN is_net = 1 THEN order_code ELSE NULL END)*1.00/COUNT(DISTINCT created_date) AS net_order,
--         COUNT(DISTINCT order_code)*1.00/COUNT(DISTINCT created_date) AS gross_order,        
--         COUNT(DISTINCT CASE WHEN regexp_like(order_type,'Cancel') = true THEN order_code ELSE NULL END)*1.000/COUNT(DISTINCT order_code) AS cancel_pp

-- FROM raw 
-- WHERE source = 'now_ship_shopee'
-- GROUP BY 1, grouping sets (city_group,())

