    SELECT 
        DATE_FORMAT(created_date, '%Y-%m') created_month,
        new_source,
        cancel_by, 
        cancel_by_v2,
        CASE WHEN cancel_reason = 'Others' THEN 'No Reason' ELSE COALESCE(pf.reason_en, cancel_reason, 'No Reason') END as cancel_reason,
        CASE WHEN cancel_reason_v2 = 'Others' THEN 'No Reason' ELSE COALESCE(pf.reason_en, cancel_reason_v2, 'No Reason') END as cancel_reason_v2,
        1.0000*COUNT(DISTINCT order_code)/ COUNT(DISTINCT created_date) orders 
    FROM (
            SELECT
                ns.created_date,
                CASE WHEN ns.source = 'now_ship_shopee' THEN 'NS Ecom' ELSE 'NS Instant' END as new_source,
                ns.source,
                ns.order_code, 
                ns.cancel_by AS cancel_by_original,
                ns.cancel_type,
                ns.order_status,
                CASE 
                    WHEN (ns.order_status = 'Pickup Failed' OR lower(cancel_type) = 'driver_cancelled') THEN 'Driver' 
                    WHEN trim(lower(ns.cancel_type)) = 'user_cancelled' THEN 'User'
                    ELSE 'Platform' END as cancel_by,
                CASE WHEN ns.order_status = 'Pickup Failed' THEN pick_failed_reason
                    WHEN ns.order_status = 'Cancelled' THEN cancel_reason
                    when ns.order_status = 'Assigning Timeout' THEN 'Assigning Timeout' 
                    ELSE NULL END as cancel_reason,
                ns.order_status,
                dot.ref_order_status,
                CASE
                    WHEN dot.ref_order_status = 6 THEN 'User/Buyer' 
                    WHEN (ns.order_status = 'Pickup Failed' OR lower(cancel_type) = 'driver_cancelled') THEN 'Driver' 
                    WHEN trim(lower(ns.cancel_type)) = 'user_cancelled' THEN 'User'
                    ELSE 'Platform' END AS cancel_by_v2,
                CASE WHEN ns.order_status = 'Pickup Failed' THEN pick_failed_reason
                    when dot.ref_order_status = 6 THEN 'Shopee Cancelled'
                    WHEN ns.order_status = 'Cancelled' THEN cancel_reason
                    when ns.order_status = 'Assigning Timeout' THEN 'Assigning Timeout' 
                    ELSE NULL END as cancel_reason_v2



            FROM vnfdbi_opsndrivers.ns_performance_tab ns
            LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 
                on dot.ref_order_code = ns.order_code 
                and dot.ref_order_category = ns.order_type
            WHERE ns.order_status IN ('Cancelled', 'Pickup Failed', 'Assigning Timeout')
            AND ns.created_date BETWEEN date'${start_date}' AND  date'${end_date}'
        ) ns_base


    LEFT JOIN dev_vnfdbi_opsndrivers.ns_pickup_fail_reason pf
        ON ns_base.cancel_reason_v2 = pf.reason_vi
    WHERE 1 = 1 
    GROUP BY 1, 2, 3, 4, 5, 6

