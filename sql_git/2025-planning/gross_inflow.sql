WITH raw AS 
(select 
        raw.*,
        CASE 
        WHEN raw.source = 'now_ship_shopee' 
             THEN 
             (CASE 
             WHEN raw.order_status = 'Assigning Timeout' THEN 1 ELSE 0 END)
             WHEN raw.source in ('now_ship_user','now_ship_merchant') THEN 
                (CASE WHEN raw.last_incharge_timestamp is null and sa.assigning_count > 0 
                     and raw.cancel_reason in ('No Reason','Wait for assigning driver too long','Unable to contact driver','Shipper denied Auto Accept order','Mistaken accept') 
                     THEN 1 ELSE 0 END)
            WHEN raw.source in ('now_ship_same_day') THEN 
                (CASE WHEN raw.last_incharge_timestamp is null 
                and raw.cancel_reason in ('No Reason','Wait for assigning driver too long','Unable to contact driver','Shipper denied Auto Accept order','Mistaken accept') THEN 1 ELSE 0 END)
            WHEN raw.source in ('order_food','order_fresh','order_market') THEN 
                (CASE WHEN raw.cancel_reason = 'No driver' THEN 1 ELSE 0 END )
        ELSE 0 END AS is_no_driver



from (select raw.*,if(raw.order_type != 0,1,coalesce(is_foody_delivery,0)) as filter_delivery
from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab raw 
left join (select id,is_foody_delivery 
           from shopeefood.shopeefood_mart_dwd_vn_order_completed_da 
           where date(dt) = current_date - interval '1' day) oct 
                on raw.id = oct.id
) raw 
LEFT JOIN 
    (SELECT 
             ref_order_id
            ,order_category
            ,COUNT(ref_order_id) AS assigning_count

     FROM driver_ops_order_assign_log_tab
     WHERE status in (3,4,2,14,15,8,9,17,18) 
     GROUP BY 1,2
     ) sa 
     on sa.ref_order_id = raw.id 
     and sa.order_category = raw.order_type
where filter_delivery = 1 
and source = 'order_food'
)
SELECT 
        date_trunc('month',created_date),
        case 
        when HOUR(created_timestamp)*100+MINUTE(created_timestamp) between 0 and 1100 then 'Morning (12AM - 11AM)'
        when HOUR(created_timestamp)*100+MINUTE(created_timestamp) between 1100 and 1400 then 'Lunch (11AM - 2PM)'
        when HOUR(created_timestamp)*100+MINUTE(created_timestamp) between 1400 and 1800 then 'Afternoon Off-peak (2PM - 6PM)'
        when HOUR(created_timestamp)*100+MINUTE(created_timestamp) between 1800 and 2100 then 'Dinner (6PM - 9PM)'
        when HOUR(created_timestamp)*100+MINUTE(created_timestamp) between 2100 and 2359 then 'Late night (9PM - 12 AM)'
        end as hour_range,
        city_name,
        COUNT(DISTINCT order_code)*1.0000/count(distinct created_date) AS gross_order
        -- COUNT(DISTINCT CASE WHEN is_no_driver = 1 THEN order_code ELSE NULL END) AS cnd

FROM raw

WHERE created_date >= DATE'2023-12-01'
and city_id in (217,218)
GROUP BY 1,2,3