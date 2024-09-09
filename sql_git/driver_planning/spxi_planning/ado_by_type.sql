WITH driver_list AS
(SELECT  
       report_date, 
       uid AS shipper_id,
       service_name,
       CARDINALITY(FILTER(service_name,x ->x in ('Delivery') )) as delivery_service_filter,
       CARDINALITY(FILTER(service_name,x ->x in ('Now Ship','Ship Shopee') )) as ship_service_filter

FROM dev_vnfdbi_opsndrivers.driver_ops_driver_services_tab 

WHERE 1 = 1
)
,raw as 
(SELECT 
       DATE(raw.delivered_timestamp) AS report_date, 
       raw.shipper_id,
       IF(raw.city_id IN (217,218),raw.city_name,'DN/Other') as city_name,
       if(dp.shipper_type=12,'hub','non-hub') as working_group,
       case 
       when dl.ship_service_filter > 0 AND delivery_service_filter = 0 then 1 
       else 0 end as is_spxi_only,
       count(distinct order_code) as cnt_order


FROM dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab raw 

LEFT JOIN driver_list dl 
        ON dl.shipper_id = raw.shipper_id
        AND dl.report_date = DATE(raw.delivered_timestamp)

LEFT JOIN driver_ops_driver_performance_tab dp on dp.report_date = DATE(raw.delivered_timestamp) and dp.shipper_id = raw.shipper_id

WHERE 1 = 1 
AND raw.order_status IN ('Delivered')
AND DATE(raw.delivered_timestamp) BETWEEN DATE'2023-12-01' AND DATE'2024-08-31'
AND dp.shipper_type != 12
AND raw.order_type != 0 
GROUP BY 1,2,3,4,5
)
select 
        date_trunc('month',report_date) as month_,
        is_spxi_only,
        coalesce(city_name,'VN') as cities,
        
        sum(cnt_order)*1.00/count(distinct report_date) as total_order,
        count(distinct (shipper_id,report_date))*1.00/count(distinct report_date) as active,
        sum(cnt_order)*1.00/count(distinct (shipper_id,report_date)) as ado
        

from raw 


group by 1,2,grouping sets(city_name,())
