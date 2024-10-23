with data_realtime as
(SELECT
        DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) AS date_ts
       , uid as shipper_id
       ,CASE
             WHEN dot.pick_city_id = 217 then 'HCM'
             WHEN dot.pick_city_id = 218 then 'HN'
             WHEN dot.pick_city_id = 219 then 'DN'
             ELSE 'Other' END AS city_group
        ,COUNT(DISTINCT dot.ref_order_code) AS total_order
        ,COUNT(DISTINCT CASE WHEN HOUR(FROM_UNIXTIME(dot.real_drop_time - 3600))*100 + MINUTE(FROM_UNIXTIME(dot.real_drop_time - 3600)) between 1330 and 2030  
            THEN dot.ref_order_code ELSE NULL END) AS "total_order_13h30 20h30"
        ,COUNT(DISTINCT CASE WHEN CAST(json_extract(dotet.order_data,'$.shipper_policy.type') AS BIGINT) = 2 THEN dot.ref_order_code ELSE NULL END) AS hub_order
        ,COUNT(DISTINCT CASE WHEN dot.group_id > 0 then dot.ref_order_code ELSE NULL END) AS total_stack_group

FROM shopeefood.foody_partner_db__driver_order_tab__reg_continuous_s0_live dot 

LEFT JOIN shopeefood.foody_partner_db__driver_order_extra_tab__reg_continuous_s0_live dotet 
    on dotet.order_id = dot.id 


WHERE 1 = 1
AND DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) = date'2024-10-20'
AND dot.ref_order_category = 0
AND dot.order_status = 400
GROUP BY 1,2,3
)
select * from
(SELECT 
        d.date_ts,
        d.shipper_id,
        -- d.city_group,
        sm.shipper_name,
        sm.city_name,
        IF("total_order_13h30 20h30" >= 15,1,0) as is_eligible,
        case  
        when "total_order_13h30 20h30" between 15 and 24 and dp.sla_rate >= 95 then 50000
        when "total_order_13h30 20h30" >= 25 and dp.sla_rate >= 95 then 120000
        ELSE 0 END AS bonus_value,
        dp.total_order,
        "total_order_13h30 20h30",
        sla_rate
        


FROM data_realtime d 

LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = d.shipper_id and sm.grass_date = 'current'

LEFT JOIN driver_ops_driver_performance_tab dp on dp.shipper_id = d.shipper_id and dp.report_date = d.date_ts

INNER JOIN dev_vnfdbi_opsndrivers.driver_ops_yagi_driver_list dr on cast(dr.shipper_id as bigint) = d.shipper_id
)
-- where bonus_value > 0
where is_eligible = 1 