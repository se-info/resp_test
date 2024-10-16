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
and dot.ref_order_status in (7)
and dot.ref_order_category = 0
-- )
-- and dot.ref_order_id = 744005853
)
,raw_ as
(select 
    date(FROM_UNIXTIME(submitted_time-3600)) as date_
    ,is_da_order_new as is_da_order
    ,ref_order_id
    ,pick_city_id
    ,c.name_en as city_name
    ,case 
    when pick_city_id in (217,218) then c.name_en
    else 'OTH' end as city_group
    ,HOUR(FROM_UNIXTIME(submitted_time-3600)) as created_hour
    ,FROM_UNIXTIME(submitted_time-3600) as order_create_timestamp
    ,greatest(delay_assign_timestamp,first_auto_assign_timestamp ) as first_auto_assign_timestamp
    ,first_driver_found_timestamp
    ,last_incharge_timestamp
    ,from_unixtime(real_drop_time -3600) as delivered_timestamp
    ,DATE_DIFF('second', FROM_UNIXTIME(submitted_time-3600), greatest(delay_assign_timestamp,first_auto_assign_timestamp ))/60.00 as from_create_to_start_assign
    ,DATE_DIFF('second', greatest(delay_assign_timestamp,first_auto_assign_timestamp ),  first_driver_found_timestamp)/60.00 as lt_start_assign_to_found_driver
    ,DATE_DIFF('second', first_driver_found_timestamp,  last_incharge_timestamp)/60.00 as lt_from_first_found_to_last_incharge
    ,DATE_DIFF('second',  last_incharge_timestamp, from_unixtime(real_drop_time -3600))/60.00 as lt_from_last_incharge_to_complete
    ,if(dn.delivery_id is not null,1,0) as is_dn_long_prep
    -- ,count(distinct ref_order_id)
from base
left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live c on c.id = base.pick_city_id

left join (select distinct delivery_id from driver_ops_deny_log_tab where reason_id = 2) dn on dn.delivery_id = base.id

where 1=1
-- and is_da_order_new = 1
and date(FROM_UNIXTIME(submitted_time-3600)) >= date '2024-10-05'
and date(FROM_UNIXTIME(submitted_time-3600)) < date '2024-10-10'

and is_asap = 1
)
select  
    date_
    ,created_hour
    ,coalesce(city_group,'VN') as cities
    ,sum(from_create_to_start_assign) from_create_to_start_assign
    ,sum(lt_start_assign_to_found_driver) lt_start_assign_to_found_driver
    ,sum(lt_from_first_found_to_last_incharge) lt_from_first_found_to_last_incharge
    ,sum(lt_from_last_incharge_to_complete) lt_from_last_incharge_to_complete
    ,count(distinct ref_order_id) as total_orders
from raw_
where is_da_order = 0
and is_dn_long_prep = 0
group by 1,2,grouping sets(city_group,())


