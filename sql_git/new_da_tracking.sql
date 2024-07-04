with base as
(select
    dot.*
    -- ,dotet.order_data
    ,cast(json_extract(dotet.order_data,'$.delivery.is_instant_prep') as varchar) is_instant_prep
    ,cast(json_extract(dotet.order_data,'$.delivery.order_flow') as int) order_flow -- 1:DFF , 0:MFF
    ,case when da.delay_assign_enable = 1 and da.delay_assign_time> 0 then 1 else 0 end as is_da_order
    ,case when cast(json_extract(dotet.order_data,'$.delivery.is_instant_prep') as varchar) = 'true' and cast(json_extract(dotet.order_data,'$.delivery.order_flow') as int) = 0 then 1 else 0 end as is_da_order_new
    ,case when dot.delay_assign_time > 0 and cast(json_extract(dotet.order_data,'$.delivery.order_flow') as int) = 0 then 1 else 0 end is_da_order_v3
from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet
right join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
    on dot.id = dotet.order_id
left join shopeefood_assignment.algo_delay_assign_order_data_vn da
    on dot.ref_order_id = da.order_id
where date(from_unixtime(dot.submitted_time-3600)) >= date '2024-01-01'
-- and (cast(json_extract(dotet.order_data,'$.delivery.is_instant_prep') as varchar) = 'true'
-- and cast(json_extract(dotet.order_data,'$.delivery.order_flow') as int) = 0
-- -- and dot.delay_assign_time = 0
-- )
-- and dot.ref_order_id = 744005853
)
select 
    *
from base
where is_da_order_new = 1
and is_da_order_v3 = 0

and date(FROM_UNIXTIME(submitted_time-3600)) >= date '2024-03-01'