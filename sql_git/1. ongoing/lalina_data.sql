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
           where date(dt) = current_date - interval '1' day
) oct on raw.id = oct.id
) raw 
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
)
SELECT 
        date_trunc('month',created_date) as month_,
        city_name,
        case 
        when city_id in (217,218,219) then 'T1'
        when city_id in (222,273,221,230,220,223) then 'T2'
        when city_id in (248,271,257,228,254,265,263) then 'T3'
        else 'new_cities'
        end as city_tier,
        'Food + Mart' AS service,
        COALESCE(TRY(COUNT(DISTINCT order_code)*1.0000/COUNT(DISTINCT created_date)),0) AS gross_order,
        COALESCE(TRY(COUNT(DISTINCT CASE WHEN order_status = 'Delivered' THEN order_code ELSE NULL END)*1.0000/COUNT(DISTINCT created_date)),0) AS net_order,
        COALESCE(TRY(COUNT(DISTINCT CASE WHEN is_no_driver = 1 THEN order_code ELSE NULL END)*1.0000/COUNT(DISTINCT created_date)),0) AS cnd_order,
        COALESCE(TRY(COUNT(DISTINCT CASE WHEN order_status = 'Delivered' THEN order_code ELSE NULL END)*1.0000/COUNT(DISTINCT order_code)),0) AS g2n,
        COALESCE(TRY(COUNT(DISTINCT CASE WHEN is_no_driver = 1 THEN order_code ELSE NULL END)*1.0000/COUNT(DISTINCT order_code)),0) AS pp_cnd,
        COALESCE(TRY(COUNT(DISTINCT CASE WHEN bad_weather_fee > 0 THEN order_code ELSE NULL END)*1.0000
                /COUNT(DISTINCT CASE WHEN bad_weather_fee > 0 THEN created_date ELSE NULL END)),0) AS "avg đơn có bwf - tính trên số ngày có đơn bwf",
        COALESCE(TRY(COUNT(DISTINCT CASE WHEN order_status = 'Delivered' AND bad_weather_fee > 0 THEN order_code ELSE NULL END)*1.0000
                /COUNT(DISTINCT CASE WHEN order_status = 'Delivered' AND bad_weather_fee > 0 THEN created_date ELSE NULL END)),0) AS "avg đơn HOÀN THÀNH có bwf - tính trên số ngày có đơn bwf"

FROM raw

WHERE created_date >= DATE'2021-01-01' 
AND created_date <= DATE'2022-12-31'
AND order_type = 0
GROUP BY 1,2,3,4 