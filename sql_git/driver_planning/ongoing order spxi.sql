with dt_array_tab as
(select 1 mapping, sequence(cast('${date} 00:00:00' as timestamp ), cast('${date} 23:59:59' as timestamp ), interval '1800' second  ) dt_array
) 
,list_time_range as
(select 
    t1.mapping
    ,t2.dt_array as start_time
    ,t2.dt_array + interval '1799.99' second as end_time
from dt_array_tab t1
cross join unnest (dt_array) as t2(dt_array)
)
,status_tab(status_id,status_name) as
(VALUES
(1,'INIT '),
(2,'ASSIGNING '),
(3,'ASSIGNING TIMEOUT '),
(4,'ASSIGNED '),
(6,'SHOPEE CANCELED '),
(8,'PICKUP '),
(9,'DRIVER CANCELED '),
(11,'COMPLETED '),
(12,'SYSTEM CANCELED '),
(13,'SYSTEM ASSIGNED '),
(14,'RETURN SUCCESS '),
(15,'RETURN FAILED '),
(16,'LOST '),
(17,'PICKUP FAILED '),
(18,'DELIVERY PENDING '),
(19,'RETURN TO HUB '),
(20,'RETURNING TO HUB '),
(21,'DELIVERY RETRY '),
(22,'RECLAIMED ')
)
,log_tab as 
(select 
        booking_id,
        1 as mapping,
        cast(json_extract(old_value,'$.status') as int) as status_old,
        cast(json_extract(new_value,'$.status') as int) as status_new,
        from_unixtime(update_time - 3600) as updated

from shopeefood.foody_express_db__shopee_booking_change_log_tab__reg_daily_s0_live
)
,log_mapping as 
(select 
        booking_id,
        mapping,
        st.status_name as status_old,
        stt.status_name as status_new,
        updated,
        row_number()over(partition by booking_id,stt.status_name order by updated asc) as status_rank

from log_tab lt 

left join status_tab st 
    on st.status_id = lt.status_old

left join status_tab stt 
    on stt.status_id = lt.status_new


where 1 = 1 
)
,order_raw as 
(select
        raw.id as booking_id,
        raw.code as booking_code,
        1 as mapping,
        case 
        when raw.drop_real_time > 0 then from_unixtime(raw.drop_real_time - 3600)
        else from_unixtime(raw.status_update_time - 3600) end as final_status_time,
        -- raw.status,
        st.status_name as status,
        lm.updated as assigning_time,
        from_unixtime(create_time - 3600) as submit_time

from shopeefood.foody_express_db__shopee_booking_tab__reg_daily_s0_live raw

left join status_tab st 
    on st.status_id = raw.status

left join log_mapping lm 
    on lm.booking_id = raw.id
    and TRIM(lm.status_new) = 'ASSIGNING'
    and lm.status_rank = 1 

where 1 = 1 
and date(from_unixtime(create_time - 3600)) = date'${date}'
order by 1 desc 
)
,final_tab as 
(select 
        ro.*,
        lt.start_time,
        lt.end_time,
        case
        when lt.start_time <= ro.submit_time and ro.final_status_time <= lt.end_time then 1 
        when ro.submit_time <= lt.start_time and lt.start_time <= ro.final_status_time and ro.final_status_time <= lt.end_time then 1 
        when lt.start_time <= ro.submit_time and ro.submit_time <= lt.end_time and lt.end_time <= ro.final_status_time then 1 
        when ro.submit_time <= lt.start_time and lt.end_time <= ro.final_status_time then 1 
        else 0 end as is_valid_processing,
        case
        when lt.start_time <= ro.assigning_time and ro.final_status_time <= lt.end_time then 1 
        when ro.assigning_time <= lt.start_time and lt.start_time <= ro.final_status_time and ro.final_status_time <= lt.end_time then 1 
        when lt.start_time <= ro.assigning_time and ro.assigning_time <= lt.end_time and lt.end_time <= ro.final_status_time then 1 
        when ro.assigning_time <= lt.start_time and lt.end_time <= ro.final_status_time then 1 
        else 0 end as is_valid_assigning

from order_raw ro 

left join list_time_range lt 
    on lt.mapping = ro.mapping
)
select 
        date(submit_time) as created,
        start_time,
        end_time,
        count(distinct case when is_valid_processing = 1 then booking_id else null end) as processing_order,  
        count(distinct case when is_valid_assigning = 1 then booking_id else null end) as assigning_order

from final_tab 

where (is_valid_assigning = 1 or is_valid_processing = 1)

group by 1,2,3
order by 2 asc
