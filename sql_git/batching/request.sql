with raw as 
(select 
        from_unixtime(create_time - 3600) as created,
        id,
        processing_time,
        case
        when processing_info like '%ds_stack%' then 'stack'
        when processing_info like '%ds_response%' then 'single'
        when processing_info like '%grouping%' then 'group'
        end as type,
        case
        when processing_info like '%ds_stack%' then cast(json_extract(processing_info,'$.ds_stack_request.info.stack_id') as varchar)
        when processing_info like '%ds_request%' then cast(json_extract(processing_info,'$.ds_request.info.batch_id') as varchar) 
        when processing_info like '%grouping%' then cast(json_extract(processing_info,'$.ds_group_request.info.request_id') as varchar) 
        end as batch_id,
        json_extract(processing_info,'$.ds_stack_request') as stack_requests,
        json_extract(processing_info,'$.ds_stack_response') as stack_results,
        json_extract(processing_info,'$.ds_stack_request.stacking_orders') as stack_order,
        json_extract(processing_info,'$.ds_stack_request.shippers') as stack_shipper

        -- json_extract(processing_info,'$.ds_response') as single_response,
        -- json_extract(processing_info,'$.ds_group_response') as group_response

        -- case
        -- when processing_info like '%ds_stack%' then json_extract(processing_info,'$.ds_stack_request.stacking_orders')
        -- when processing_info like '%ds_request%' then json_extract(processing_info,'$.ds_request.order_shippers')
        -- -- when processing_info like '%grouping%' then json_extract(processing_info,'$.ds_group_request.orders')
        -- end as order_array,
        -- case 
        -- when processing_info like '%grouping%' then json_extract(processing_info,'$.ds_group_request.orders')
        -- end as group_array,
        -- cast(json_extract(processing_info,'$.order_category') as int) as category,
        -- processing_info

from shopeefood.foody_partner_archive_db__order_assign_shipper_batch_processing_log_tab_di


where city_id = 217

and 
(
-- json_extract(processing_info, '$.ds_request["info"]') is not null  
-- or 
json_extract(processing_info, '$.ds_stack_response') is not null 
-- or 
-- json_extract(processing_info, '$.ds_group_request["info"]') is not null 
)
and cast(grass_date as date) = date'2024-08-01'

)
,summary as 
(select 
        raw.batch_id,
        raw.created,
        raw.processing_time,
        t.order_id,
        cast(json_extract(t.order_data,'$.delivery_remaining_time') as bigint) as delivery_remaining_time,
        cast(json_extract(t.order_data,'$.committed_pickup_eta') as bigint) as committed_pickup_eta,
        s.shipper_id,
        json_array_length(json_extract(si.shipper_data,'$.travel_plan')) as step,
        si.shipper_data



from raw 

cross join unnest (cast(raw.stack_order as map<varchar,json>)) as t(order_id,order_data)

cross join unnest (cast(json_extract(t.order_data,'$.candidate_shippers') as array<bigint>)) as s(shipper_id)


-- mapping candidate with shipper requests
left join 
(select 
        raw.*,
        s.shipper_id,
        s.shipper_data

from raw 

cross join unnest (cast(raw.stack_shipper as map<varchar,json>)) as s(shipper_id,shipper_data)
) si on cast(si.shipper_id as bigint) = s.shipper_id and raw.batch_id = si.batch_id



where 1 = 1 
-- and t.order_id = '534058136'
and raw.stack_order is not null 
)
select * from summary where order_id = '533819065'

-- ;
-- select 
--         from_unixtime(create_time - 3600) as created,
--         *
-- from dev_shopeefood.shopeefood_mart_dwd_vn_multi_stacking_shippingfee_group_di
-- where cardinality(filter(order_id_list,x -> x = 14511312  )) > 0  
-- -- where group_id = 18490


