-- g2n: Deliver only (a)
-- cancel: cancel or pick failed (b)
-- (a) + (b) <= 100% (other status like returned success/ failed/...)

WITH base AS (
SELECT
    CASE WHEN source = 'now_ship_shopee' THEN 'Ecom' ELSE 'Instant' END as new_source,
    created_month, order_code, source, order_status, cancel_by, cancel_type,
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
        WHEN cod_value < 500000 THEN '1. 0-500k'
        WHEN cod_value < 1000000 THEN '2. 500-1000k'
        WHEN cod_value < 1500000 THEN '3. 1000-1500k'
        WHEN cod_value < 2000000 THEN '4. 1500-2000k'
        WHEN cod_value >= 2000000 THEN '5. 2000k++' END AS cod_range,
    case 
        when order_status in ('Delivered','Returned','Others') then order_status 
        WHEN order_status = 'Pickup Failed' or lower(cancel_type) = 'driver_cancelled' THEN 'Cancel by Driver'
        WHEN order_status IN ('Cancelled','Assigning Timeout') then 
                case when order_status IN ('Assigning Timeout') then 'Cancel by Platform'
                        when cancel_reason = 'No Reason' and source = 'now_ship_shopee' then 'Cancel by Shopee'
                        when lower(cancel_type) = 'user_cancelled' then 'Cancel by User'
                        else 'Cancel by Platform' end
        end as order_type

FROM (
    SELECT 
        ns.source, ns.cancel_by, ns.cancel_type,
        DATE_FORMAT(created_date, '%Y-%m') created_month,
        ns.order_code, ns.order_status, ns.cancel_reason,
        CEILING(ns.distance) ceiling_distance, ns.created_timestamp,
        HOUR(ns.created_timestamp)*60 + MINUTE(ns.created_timestamp) mins,
        weight.order_weight,
        weight.pay_by_type,
        weight.cod_value

    FROM vnfdbi_opsndrivers.ns_performance_tab ns

    LEFT JOIN (
                SELECT
                    code, created_at,
                    pay_by_type,
                    cod_value*1.000 AS cod_value,
                    SUM(CAST(t.item['weight'] AS DOUBLE)) order_weight 

                FROM (
                    SELECT 
                        code, from_unixtime(create_time - 3600) created_at,
                        CAST(json_extract(extra_data, '$.items') AS ARRAY(MAP(VARCHAR, JSON))) item_json,
                        cod_value,
                        CASE 
                        WHEN cod_value > 0 then 'cod' else 'non cod' end as pay_by_type

                    FROM shopeefood.foody_express_db__shopee_booking_tab__reg_daily_s0_live
                    WHERE DATE(from_unixtime(create_time - 3600)) BETWEEN date'${start_date}' AND date'${end_date}' -- to make sure cover diff between adcountant table
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
                        WHERE DATE(from_unixtime(create_time - 3600)) BETWEEN date'${start_date}' AND date'${end_date}' -- to make sure cover diff between adcountant table
                       -- AND status IN (11, 14)
                        ) bt 
                ) weight 
                
                ON ns.order_code = weight.code

    WHERE ns.created_date BETWEEN date'${start_date}' AND date'${end_date}'
    )
)


-- each service
SELECT
    created_month, new_source,
    'Long Distance' as grp,
    distance_range grp_item, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type not in ('Cancel by Driver','Cancel by Platform','Cancel by User','Cancel by Shopee') THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) g2n,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Driver' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_driver,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Platform' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_platform, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by User' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_user,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Shopee' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_shopee, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type not in ('Delivered','Cancel by Driver','Cancel by Platform','Cancel by User','Cancel by Shopee') THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) other_status     
    
FROM base 
GROUP BY 1, 2, 3, 4

UNION ALL

SELECT
    created_month, new_source,
    'Time' as grp,
    timeframe grp_item, 
    -- 1.00000*COUNT(DISTINCT CASE WHEN order_status = 'Delivered' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) g2n,
    -- 1.00000*COUNT(DISTINCT CASE WHEN order_status = 'Pickup Failed'  THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_driver,
    -- 1.00000*(COUNT(DISTINCT CASE WHEN order_status IN ('Cancelled','Assigning Timeout') THEN order_code ELSE NULL END) - COUNT(DISTINCT CASE WHEN (lower(cancel_by) = 'user_cancelled' OR lower(cancel_type) = 'user_cancelled' ) THEN order_code ELSE NULL END))/ COUNT(DISTINCT order_code) cancel_by_platform, 
    -- 1.00000*COUNT(DISTINCT CASE WHEN lower(cancel_type) = 'user_cancelled' OR lower(cancel_by) = 'user_cancelled' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) user_cancel 
    1.00000*COUNT(DISTINCT CASE WHEN order_type not in ('Cancel by Driver','Cancel by Platform','Cancel by User','Cancel by Shopee') THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) g2n,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Driver' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_driver,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Platform' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_platform, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by User' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_user,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Shopee' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_shopee, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type not in ('Delivered','Cancel by Driver','Cancel by Platform','Cancel by User','Cancel by Shopee') THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) other_status    
FROM base 
GROUP BY 1, 2, 3, 4

UNION ALL

SELECT
    created_month, new_source,
    'Weight' as grp,
    weight_range grp_item, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type not in ('Cancel by Driver','Cancel by Platform','Cancel by User','Cancel by Shopee') THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) g2n,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Driver' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_driver,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Platform' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_platform, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by User' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_user,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Shopee' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_shopee, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type not in ('Delivered','Cancel by Driver','Cancel by Platform','Cancel by User','Cancel by Shopee') THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) other_status    

FROM base 
GROUP BY 1, 2, 3, 4

UNION ALL 

SELECT
    created_month, new_source,
    'COD' as grp,
    cod_range grp_item, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type not in ('Cancel by Driver','Cancel by Platform','Cancel by User','Cancel by Shopee') THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) g2n,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Driver' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_driver,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Platform' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_platform, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by User' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_user,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Shopee' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_shopee, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type not in ('Delivered','Cancel by Driver','Cancel by Platform','Cancel by User','Cancel by Shopee') THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) other_status    

FROM base 
GROUP BY 1, 2, 3, 4

UNION ALL 
SELECT
    created_month, new_source,
    '#. All' as grp,
    '#. All' grp_item, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type not in ('Cancel by Driver','Cancel by Platform','Cancel by User','Cancel by Shopee') THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) g2n,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Driver' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_driver,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Platform' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_platform, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by User' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_user,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Shopee' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_shopee, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type not in ('Delivered','Cancel by Driver','Cancel by Platform','Cancel by User','Cancel by Shopee') THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) other_status    

FROM base 
GROUP BY 1, 2, 3, 4

UNION 

-- all service
SELECT
    created_month, 'SPX' as new_source,
    'Long Distance' as grp,
    distance_range grp_item, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type not in ('Cancel by Driver','Cancel by Platform','Cancel by User','Cancel by Shopee') THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) g2n,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Driver' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_driver,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Platform' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_platform, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by User' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_user,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Shopee' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_shopee, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type not in ('Delivered','Cancel by Driver','Cancel by Platform','Cancel by User','Cancel by Shopee') THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) other_status     

FROM base 
GROUP BY 1, 2, 3, 4

UNION ALL

SELECT
    created_month, 'SPX' as new_source,
    'Time' as grp,
    timeframe grp_item, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type not in ('Cancel by Driver','Cancel by Platform','Cancel by User','Cancel by Shopee') THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) g2n,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Driver' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_driver,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Platform' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_platform, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by User' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_user,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Shopee' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_shopee, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type not in ('Delivered','Cancel by Driver','Cancel by Platform','Cancel by User','Cancel by Shopee') THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) other_status    
FROM base 
GROUP BY 1, 2, 3, 4

UNION ALL

SELECT
    created_month, 'SPX' as new_source,
    'Weight' as grp,
    weight_range grp_item, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type not in ('Cancel by Driver','Cancel by Platform','Cancel by User','Cancel by Shopee') THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) g2n,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Driver' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_driver,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Platform' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_platform, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by User' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_user,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Shopee' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_shopee, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type not in ('Delivered','Cancel by Driver','Cancel by Platform','Cancel by User','Cancel by Shopee') THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) other_status    

FROM base 
GROUP BY 1, 2, 3, 4

UNION ALL 
SELECT
    created_month, 'SPX' new_source,
    'COD' as grp,
    cod_range grp_item, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type not in ('Cancel by Driver','Cancel by Platform','Cancel by User','Cancel by Shopee') THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) g2n,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Driver' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_driver,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Platform' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_platform, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by User' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_user,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Shopee' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_shopee, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type not in ('Delivered','Cancel by Driver','Cancel by Platform','Cancel by User','Cancel by Shopee') THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) other_status    

FROM base 
GROUP BY 1, 2, 3, 4


UNION ALL
SELECT
    created_month, 'SPX' as new_source,
    '#. All' as grp,
    '#. All' grp_item, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type not in ('Cancel by Driver','Cancel by Platform','Cancel by User','Cancel by Shopee') THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) g2n,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Driver' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_driver,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Platform' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_platform, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by User' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_user,
    1.00000*COUNT(DISTINCT CASE WHEN order_type = 'Cancel by Shopee' THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) cancel_by_shopee, 
    1.00000*COUNT(DISTINCT CASE WHEN order_type not in ('Delivered','Cancel by Driver','Cancel by Platform','Cancel by User','Cancel by Shopee') THEN order_code ELSE NULL END)/ COUNT(DISTINCT order_code) other_status    

FROM base 
GROUP BY 1, 2, 3, 4

