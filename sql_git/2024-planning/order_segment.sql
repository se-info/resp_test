with last_incharge_time_tab as 
(
    select 
        ref_order_id
        ,max(create_timestamp) as last_incharge_timestamp

    from vnfdbi_opsndrivers.shopeefood_bnp_assignment_order_tab
    where 1=1
    group by 1
) 
,raw as 
(select 
        date(from_unixtime(report_date - 3600)) as report_date,
        id,
        uid as shipper_id,
        from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600) as start_shift_time,
        from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600) as end_shift_time

from shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live
)
,f as
(select 
        raw.shipper_id,
        raw.report_date,
        sm.city_id,
        array_agg(date_diff('hour',start_shift_time,end_shift_time)) as shift_hour_agg,    
        case 
        when CARDINALITY(array_agg(date_diff('hour',start_shift_time,end_shift_time))) > 1 then 'multi'
        when CARDINALITY(array_agg(date_diff('hour',start_shift_time,end_shift_time))) = 1 and 
            cardinality(filter(array_agg(distinct date_diff('hour',start_shift_time,end_shift_time)),x -> x <= 5)) > 0 then 'short'
        else 'long' end as hub_segment
FROM raw  

left join shopeefood.foody_mart__profile_shipper_master sm 
    on sm.shipper_id = raw.shipper_id 
    and sm.grass_date = 'current'

where 1 = 1 
and month(raw.report_date) = 12
group by 1,2,3
)
,base_cost as 
(select 
        bf.order_id,
        li.last_incharge_timestamp,
        case 
        when bf.delivered_by = 'hub' then f.hub_segment
        else bf.delivered_by end as segment,
        bf.grass_date,
        case 
        when city_name in ('HCM','HN') and delivered_by = 'hub' then 'hub'
        when city_name in ('HCM','HN') and delivered_by != 'hub' 
            then if(new_driver_tier_v2 in ('T1','T2','T3','T4','T5'),new_driver_tier_v2,'T1') 
        else 'OTH' end as order_segment


from vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf 

left join last_incharge_time_tab li on li.ref_order_id = bf.order_id

-- left join raw on bf.partner_id = raw.shipper_id and li.last_incharge_timestamp between raw.start_shift_time and raw.end_shift_time

left join f on f.shipper_id = bf.partner_id and f.report_date = bf.grass_date

where month(bf.grass_date) = 12
and source = 'Food'
and status = 7
)
-- select distinct order_segment from base_cost
-- select * from base_cost
select
        date_trunc('month',grass_date) as "month",
        -- regexp_replace(order_segment,'Hub|HUB|hub','HUB'),
        -- order_segment,
        segment,
        count(distinct order_id)/count(distinct grass_date)*1.0000 as ado

from base_cost
group by 1,2
;
-- a1_hub_segment
with raw as 
(select 
        date(from_unixtime(report_date - 3600)) as report_date,
        id,
        uid as shipper_id,
        from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600) as start_shift_time,
        from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600) as end_shift_time

from shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live
)
,f as
(select 
        raw.shipper_id,
        raw.report_date,
        sm.city_id,
        array_agg(date_diff('hour',start_shift_time,end_shift_time)) as shift_hour_agg,    
        case 
        when CARDINALITY(array_agg(date_diff('hour',start_shift_time,end_shift_time))) > 1 then 'multi'
        when CARDINALITY(array_agg(date_diff('hour',start_shift_time,end_shift_time))) = 1 and 
            cardinality(filter(array_agg(distinct date_diff('hour',start_shift_time,end_shift_time)),x -> x <= 5)) > 0 then 'short'
        else 'long' end as hub_segment
FROM raw  

left join shopeefood.foody_mart__profile_shipper_master sm 
    on sm.shipper_id = raw.shipper_id 
    and sm.grass_date = 'current'

where 1 = 1 
and month(raw.report_date) = 12
group by 1,2,3
)
select
        date_trunc('month',report_date) as "month",
        hub_segment,
        count(distinct (shipper_id,report_date))*1.0000/count(distinct report_date) as a1 


from f

where city_id in (217,218)
group by 1,2

