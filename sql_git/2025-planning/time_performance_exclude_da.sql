with first_auto_assign_timestamp as
(select 
    order_id
    ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
    ,max(case when status = 6 then from_unixtime(create_time-3600) else null end) as last_picked_timestamp
from shopeefood.foody_order_db__order_status_log_tab_di
group by 1
)
,assign_order_log as
(
    select 
        ref_order_id
        ,ref_order_category
        ,min(create_timestamp) as first_driver_found_timestamp
        ,max(case when status in (3,4) then create_timestamp else null end) as last_incharge_timestamp
    from vnfdbi_opsndrivers.shopeefood_bnp_assignment_order_tab
    group by 1,2
)

,base as
(select
    dot.*
    -- ,dotet.order_data
    ,cast(json_extract(dotet.order_data,'$.delivery.is_instant_prep') as varchar) is_instant_prep
    ,cast(json_extract(dotet.order_data,'$.delivery.order_flow') as int) order_flow -- 1:DFF , 0:MFF
    ,case when da.delay_assign_enable = 1 and da.delay_assign_time> 0 then 1 else 0 end as is_da_order
    ,case when cast(json_extract(dotet.order_data,'$.delivery.is_instant_prep') as varchar) = 'true' and cast(json_extract(dotet.order_data,'$.delivery.order_flow') as int) = 0 then 1 else 0 end as is_da_order_new
    ,case when dot.delay_assign_time > 0 and cast(json_extract(dotet.order_data,'$.delivery.order_flow') as int) = 0 then 1 else 0 end is_da_order_v3
    ,case when dot.delay_assign_time != 0 then FROM_UNIXTIME(dot.delay_assign_time-3600) else FROM_UNIXTIME(dot.delay_assign_time-3600) end as delay_assign_timestamp
    ,fa.first_auto_assign_timestamp
    ,aol.first_driver_found_timestamp
    ,aol.last_incharge_timestamp

from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet
right join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
    on dot.id = dotet.order_id
left join shopeefood_assignment.algo_delay_assign_order_data_vn da
    on dot.ref_order_id = da.order_id
left join first_auto_assign_timestamp fa
    on dot.ref_order_id = fa.order_id and dot.ref_order_category = 0
left join assign_order_log aol
    on dot.ref_order_id = aol.ref_order_id and dot.ref_order_category = aol.ref_order_category
where date(from_unixtime(dot.submitted_time-3600)) >= date '2024-01-01'
and ref_order_status in (7)

-- )
-- and dot.ref_order_id = 744005853
)
,raw_ as
(select 
    date(FROM_UNIXTIME(submitted_time-3600)) as date_
    ,is_da_order_new
    ,ref_order_id
    ,pick_city_id
    ,FROM_UNIXTIME(submitted_time-3600) as order_create_timestamp
    ,greatest(delay_assign_timestamp,first_auto_assign_timestamp ) as first_auto_assign_timestamp
    ,first_driver_found_timestamp
    ,last_incharge_timestamp
    ,from_unixtime(real_drop_time -3600) as delivered_timestamp
    ,DATE_DIFF('second', FROM_UNIXTIME(submitted_time-3600), greatest(delay_assign_timestamp,first_auto_assign_timestamp ))/60.00 as from_create_to_start_assign
    ,DATE_DIFF('second', greatest(delay_assign_timestamp,first_auto_assign_timestamp ),  first_driver_found_timestamp)/60.00 as lt_start_assign_to_found_driver
    ,DATE_DIFF('second', first_driver_found_timestamp,  last_incharge_timestamp)/60.00 as lt_from_first_found_to_last_incharge
    ,DATE_DIFF('second',  last_incharge_timestamp, from_unixtime(real_drop_time -3600))/60.00 as lt_from_last_incharge_to_complete
    ,IF(real_drop_time > estimated_drop_time,1,0) as is_late_eta
    ,IF(real_pick_time > estimated_pick_time,1,0) as is_late_pickup


    -- ,count(distinct ref_order_id)
from base
where 1=1
-- and is_da_order_new = 1
and date(FROM_UNIXTIME(submitted_time-3600)) >= date '2024-07-01' 
and date(FROM_UNIXTIME(submitted_time-3600)) <= date'2024-10-30'
and is_asap = 1
)
select 
    date_
    ,is_da_order_new as is_da_order
    -- ,pick_city_id
    ,sum(from_create_to_start_assign) from_create_to_start_assign
    ,sum(lt_start_assign_to_found_driver) lt_start_assign_to_found_driver
    ,sum(lt_from_first_found_to_last_incharge) lt_from_first_found_to_last_incharge
    ,sum(lt_from_last_incharge_to_complete) lt_from_last_incharge_to_complete
    ,count(distinct ref_order_id) as total_orders
    ,count(distinct case when is_late_eta = 1 then ref_order_id else null end) as late_eta_order
    ,count(distinct case when is_late_pickup = 1 then ref_order_id else null end) as late_pickup_order

from raw_
group by 1,2



-- ;
-- with base as
-- (select 
--     date_trunc('month', date(order_create_time)) as grass_month
--     ,ref_order_id
--     ,count(distinct (create_timestamp, shipper_uid,order_uid)) as assignment_attemp
-- from vnfdbi_opsndrivers.shopeefood_bnp_assignment_order_tab
-- where grass_date >= date '2023-01-01'
-- and ref_order_category = 0
-- and is_test = 0
-- group by 1,2
-- )
-- select 
--     grass_month
--     ,case 
--         when assignment_attemp <= 1 then '1. 1 attempt'
--         when assignment_attemp <= 2 then '2. 2 attempt'
--         when assignment_attemp <= 5 then '3. 5 attempt'
--         when assignment_attemp <= 10 then '4. 10 attempt'
--         when assignment_attemp <= 15 then '5. 15 attempt'
--         when assignment_attemp <= 20 then '6. 20 attempt'
--         when assignment_attemp >20 then '7. ++20 attempt'
--         end as attempt_range
--     ,count(distinct ref_order_id) as total_orders
-- from base
-- group by 1,2