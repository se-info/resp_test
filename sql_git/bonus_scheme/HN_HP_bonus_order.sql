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
        ELSE 0 END AS is_no_driver,
        di.name_en as district_name,
        IF(REGEXP_LIKE(di.name_en,'Dong Da|Cau Giay|Ha Dong|Hai Ba Trung|Hoan Kiem|Ba Dinh|Thanh Xuan|Le Chan District') = true,1,0) AS is_qualified

from (select raw.*,if(raw.order_type != 0,1,coalesce(is_foody_delivery,0)) as filter_delivery

from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab raw 
left join (select id,is_foody_delivery 
           from shopeefood.shopeefood_mart_dwd_vn_order_completed_da 
           where date(dt) = current_date - interval '1' day) oct 
                on raw.id = oct.id
where 1 = 1 
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
LEFT JOIN (SELECT id,name_en FROM shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live) di on di.id = raw.district_id 
WHERE raw.order_status = 'Delivered'
)
,f AS 
(SELECT 
        date(raw.delivered_timestamp) AS report_date,
        raw.shipper_id,
        dp.shipper_tier,
        dp.city_name,
        dp.sla_rate,
        COUNT(DISTINCT CASE WHEN order_status = 'Delivered' THEN order_code ELSE NULL END) AS ado_full_day,
        COUNT(DISTINCT CASE WHEN order_status = 'Delivered' AND is_qualified = 1 AND HOUR(delivered_timestamp) BETWEEN 11 and 12  THEN order_code ELSE NULL END) AS lunch_bonus_ado,
        COUNT(DISTINCT CASE WHEN order_status = 'Delivered' AND is_qualified = 1 AND HOUR(delivered_timestamp) BETWEEN 17 and 19  THEN order_code ELSE NULL END) AS dinner_bonus_ado

FROM raw

LEFT JOIN driver_ops_driver_performance_tab dp on dp.shipper_id = raw.shipper_id AND dp.report_date = date(raw.delivered_timestamp)

WHERE 1 = 1 
AND dp.city_id IN (218,220)
AND dp.shipper_tier NOT IN ('Hub','Other')
AND raw.order_type = 0
AND date(raw.delivered_timestamp) BETWEEN date'2024-06-29' AND date'2024-06-30'
AND raw.shipper_id > 0 
-- AND (HOUR(delivered_timestamp) BETWEEN 11 and 12
-- OR HOUR(delivered_timestamp) BETWEEN 17 and 19)
GROUP BY 1,2,3,4,5 
)
select * from 
(select  
        *,
        case 
        when sla_rate >= 95 then lunch_bonus_ado * 2000 
        else 0 end as lunch_bonus,
        case 
        when city_name = 'Ha Noi City' and report_date = date'2024-06-30' then 0
        when sla_rate >= 95 then dinner_bonus_ado * 2000 
        else 0 end as dinner_bonus

from f 

where 1 = 1 
)
where (lunch_bonus > 0 OR dinner_bonus > 0)