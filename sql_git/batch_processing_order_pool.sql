with raw as 
(select 
        from_unixtime(create_time - 3600) as created_,
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
        case
        when processing_info like '%ds_stack%' then json_extract(processing_info,'$.ds_stack_request.stacking_orders')
        when processing_info like '%ds_request%' then json_extract(processing_info,'$.ds_request.order_shippers')
        -- when processing_info like '%grouping%' then json_extract(processing_info,'$.ds_group_request.orders')
        end as order_array,
        case 
        when processing_info like '%grouping%' then json_extract(processing_info,'$.ds_group_request.orders')
        end as group_array,
        cast(json_extract(processing_info,'$.order_category') as int) as category,
        processing_info

from shopeefood.foody_partner_archive_db__order_assign_shipper_batch_processing_log_tab_di


where city_id = 218

and 
(
json_extract(processing_info, '$.ds_request["info"]') is not null  
or 
json_extract(processing_info, '$.ds_stack_request["info"]') is not null 
or 
json_extract(processing_info, '$.ds_group_request["info"]') is not null 
)
and cast(grass_date as date) = date'2023-10-06'

)
,resquest_tab as 
(select 
        raw.created_,
        date(raw.created_) as created_date,
        type,
        batch_id,
        category,
        cast(json_extract(g.group_info,'$.order_id') as varchar) as order_info,
        id

from raw

cross join unnest (cast(raw.group_array as array<json>)) as g(group_info)

UNION ALL 

select 
        raw.created_,
        date(raw.created_) as created_date,
        type,
        batch_id,
        case 
        when regexp_like(t.order_info,'gg')=false and regexp_like(t.order_info,'do')=true then 0
        when regexp_like(t.order_info,'gg')=false and regexp_like(t.order_info,'do')=false then 6
        -- when regexp_like(t.order_info,'gg')=true then cast(regexp_replace(t.order_info, '[^0-9]', '') as int) 
        when regexp_like(t.order_info,'gg')=true then ogi.ref_order_category
        end as category,        
        t.order_info,
        raw.id

from raw

cross join unnest (cast(raw.order_array as map<varchar,json>)) as t(order_info,order_value)

left join shopeefood.foody_partner_db__order_group_info_tab__reg_daily_s0_live ogi 
    on ogi.id = (case when regexp_like(t.order_info,'gg')=true then cast(regexp_replace(t.order_info, '[^0-9]', '') as int) else 0 end )



)
select * from resquest_tab where batch_id = '55bef26e4f944ba99e34c24980b647a8'


