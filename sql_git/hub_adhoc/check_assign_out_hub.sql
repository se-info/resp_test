with raw as 
(select 
        sa.ref_order_id,
        sa.driver_id,
        sa.create_time,
        date(sa.create_time) as created,
        date_add('second',slot.start_time,cast(date(from_unixtime(slot.date_ts - 3600)) as TIMESTAMP)) as start_shift_time,
        date_add('second',slot.end_time,cast(date(from_unixtime(slot.date_ts - 3600)) as TIMESTAMP)) as end_shift_time,
        ro.drop_latitude,ro.drop_longitude,t2.hub_id,hi.hub_priority,cf.hub_id as hub_id_registered

from driver_ops_order_assign_log_tab  sa 
left join driver_ops_raw_order_tab ro 
        on ro.id = sa.ref_order_id
        and ro.order_type = sa.order_category

left join  dev_vnfdbi_opsndrivers.driver_ops_hub_polygon_tab t2
    on ST_Within(ST_Point(ro.drop_latitude,ro.drop_longitude), ST_GeometryFromText(t2.hub_polygon))


left join shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live slot 
        on slot.uid = sa.driver_id
        and date(from_unixtime(slot.date_ts - 3600)) = date(sa.create_time)
        and sa.create_time between 
                date_add('second',(slot.start_time),cast(date(from_unixtime(slot.date_ts - 3600)) as TIMESTAMP)) 
                and date_add('second',(slot.end_time),cast(date(from_unixtime(slot.date_ts - 3600)) as TIMESTAMP)) 
LEFT JOIN shopeefood.foody_internal_db__shipper_config_slot_tab__reg_daily_s0_live cf on cf.id = slot.slot_id

left join shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live hi 
   on hi.id = cast(t2.hub_id as bigint)

where date(sa.create_time) = current_date - interval '1' day 
and slot.uid is not null )
,metrics as 
(select 
        *,
        date_diff('second',create_time,end_shift_time)*1.0000/60 as assign_to_end_time,
        case 
        when (date_diff('second',create_time,end_shift_time)*1.0000/60) <= 10 then '1. <= 10m'
        when (date_diff('second',create_time,end_shift_time)*1.0000/60) <= 20 then '2. 10 - 20m'
        when (date_diff('second',create_time,end_shift_time)*1.0000/60) <= 30 then '3. 20 - 30m'
        else '4. Other' end as assign_to_end_time_range,
        case 
        when hub_id_registered != coalesce(cast(drop_hub_id_assigned as bigint),0) then 1 else 0 end as is_drop_out
from
(select 
        ref_order_id,
        driver_id,
        hub_id_registered,
        create_time,
        end_shift_time,
        max_by(hub_id,hub_priority) as drop_hub_id_assigned,
        array_agg(hub_id) as hub_id_overlapped

from raw 
group by 1,2,3,4,5)
)
select
        assign_to_end_time_range,
        count(*)
from metrics 
where is_drop_out = 1 
group by 1 
