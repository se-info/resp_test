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
,base_assignment as
(select
        date(create_timestamp) created_date
        ,ref_order_category
        ,order_uid
        ,shipper_uid
        ,create_timestamp
        ,status
from vnfdbi_opsndrivers.shopeefood_bnp_assignment_order_tab raw
where status in (3,4,2,14,15,8,9,17,18) 
and date(create_timestamp) between date '2023-12-01' and date'2024-08-31'
and ref_order_category != 0
)
,f as 
(select 
        created_date,
        shipper_uid as shipper_id,
        count(distinct case when status in (3,4,2,14,15,8,9,17,18) then (shipper_uid,order_uid,create_timestamp) else null end) as no_assign,
        count(distinct case when status in (3,4) then (shipper_uid,order_uid,create_timestamp) else null end) as no_incharged,
        count(distinct case when status in (8,9,17,18) then (shipper_uid,order_uid,create_timestamp) else null end) as no_ignored,
        count(distinct case when status in (2,14,15) then (shipper_uid,order_uid,create_timestamp) else null end) as no_deny


from base_assignment

group by 1,2
)
,s as 
(select 
        f.*,
        if(dp.shipper_type=12,'hub','non-hub') as working_group,
        case 
        when dl.ship_service_filter > 0 AND delivery_service_filter = 0 then 1 
        else 0 end as is_spxi_only



from f 

LEFT JOIN driver_list dl 
        ON dl.shipper_id = f.shipper_id
        AND dl.report_date = f.created_date

LEFT JOIN driver_ops_driver_performance_tab dp 
        on dp.report_date = f.created_date
        and dp.shipper_id = f.shipper_id

)
select 
        date_trunc('month',created_date) as month_,
        coalesce(is_spxi_only,2) as type_,
        (sum(no_incharged) + sum(no_deny))*1.00/sum(no_assign) as ar


from s 

group by 1,grouping sets(is_spxi_only,())
