SELECT *

FROM
(
SELECT 
        case when processing_info LIKE '%ds_stack_request%' then 'Stack Request'
            when processing_info LIKE '%ds_request%' then 'Single Requeset'
            else null end as ds_type

       ,case when processing_info LIKE '%ds_stack_request%' then json_extract(processing_info, '$.ds_stack_request.stacking_orders')
            when processing_info LIKE '%ds_request%' then json_extract(processing_info, '$.ds_request.order_shippers')
            else null end as candidate_shipper
       ,grass_date
       --,processing_info
       

FROM foody_partner_archive_db__order_assign_shipper_batch_processing_log_tab

where 1=1
and date(from_unixtime(create_time - 60*60)) = date('2021-04-12')
and (processing_info LIKE '%ds_request%' or processing_info LIKE '%ds_stack_request%')

)base 

WHERE processing_info LIKE '%13121462%'
