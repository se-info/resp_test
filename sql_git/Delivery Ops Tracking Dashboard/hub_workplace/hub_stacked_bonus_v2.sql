WITH params(report_date,uid,hub_type_x_start_time) AS
(VALUES
(date'2024-09-09',50550043,'5 hour shift-11')
) 
,pay_note(week_num,max_date,min_date) as 
(SELECT
        year(report_date)*100 + week(report_date),
        max(report_date),
        min(report_date)
FROM
(
(SELECT SEQUENCE(date_trunc('week',current_date)- interval '2' month,date_trunc('week',current_date) - interval '1' day) bar)

CROSS JOIN
    unnest (bar) as t(report_date)
)
group by 1 
) 
,hub_order_tab as
(
    select 
        ho.uid as shipper_id
        ,ho.slot_id
        ,ho.autopay_report_id
        ,ho.ref_order_id
        ,ho.ref_order_category
        ,case when coalesce(oct.risk_bearer_id,0) != 2 then 1 else 0 end as is_hub_order
        ,date(from_unixtime(ho.autopay_date_ts-3600)) as autopay_date
        ,date(from_unixtime(ho.create_time-3600)) as created_date
    from shopeefood.foody_partner_archive_db__shipper_hub_order_tab__reg_daily_s0_live ho
    left join (select id,cast(json_extract_scalar(oct.extra_data, '$.risk_bearer_type') as int) as risk_bearer_id from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct) oct
        on ho.ref_order_id = oct.id and ho.ref_order_category = 0
where date(from_unixtime(ho.autopay_date_ts-3600)) >= date'2024-08-19'
)
,summary as 
(select 
        date(r.delivered_timestamp) as report_date,
        r.group_id,
        r.id,
        ho.slot_id,
        hub.hub_type_original,
        hub.hub_type_x_start_time,
        hub.hub_locations,
        hub.kpi,
                
                                                                                                          
                                                                                                          
                                    
        r.shipper_id

from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab r 

left join hub_order_tab ho on ho.ref_order_id = r.id and ho.ref_order_category = r.order_type

left join driver_ops_hub_driver_performance_tab hub on hub.slot_id = ho.slot_id and hub.uid = r.shipper_id


where 1 = 1 
and ho.ref_order_id is not null 
and r.order_status = 'Delivered'
and date(r.delivered_timestamp) between date'2024-09-09' and date'2024-09-15' 
and hub.city_name != 'Hai Phong City'
and hub.hub_type_original in ('1 hour shift','3 hour shift','5 hour shift')
)
,f as 
(select 
        s.report_date,
        s.shipper_id,
        s.hub_type_original,
        s.hub_type_x_start_time,
        s.kpi,
        case 
        when p.uid is not null then 1
        else s.kpi end as kpi_adjusted,
        s.slot_id,
        count(distinct s.id) as total_order,
        count(distinct case when s.group_id > 0 then s.id else null end) as cnt_from_2nd_stacked

from summary s 

LEFT JOIN params p 
    on p.uid = s.shipper_id 
    and p.report_date = s.report_date
    and p.hub_type_x_start_time = s.hub_type_x_start_time            

group by 1,2,3,4,5,6,7
)
,final_ as 
(select
        report_date,
        shipper_id,
        slot_id,
        kpi,
        kpi_adjusted,
        hub_type_original,
        hub_type_x_start_time,
        case 
        when hub_type_original = '3 hour shift' and cnt_from_2nd_stacked >= 9 and kpi = 1 then (4000 * (cnt_from_2nd_stacked -  8)) + 8000
        when hub_type_original = '3 hour shift' and cnt_from_2nd_stacked >= 5 and kpi = 1 then 2000 * (cnt_from_2nd_stacked - 4)

        when hub_type_original = '5 hour shift' and cnt_from_2nd_stacked >= 14 and kpi = 1 then (5000 * (cnt_from_2nd_stacked -  13)) + 15000
        when hub_type_original = '5 hour shift' and cnt_from_2nd_stacked >= 9 and kpi = 1 then 3000 * (cnt_from_2nd_stacked - 8)

        else 0 end as bonus_value,
        total_order,
        cnt_from_2nd_stacked,
        case 
        when hub_type_original = '3 hour shift' and cnt_from_2nd_stacked >= 9 and kpi_adjusted = 1 then (4000 * (cnt_from_2nd_stacked -  8)) + 8000
        when hub_type_original = '3 hour shift' and cnt_from_2nd_stacked >= 5 and kpi_adjusted = 1 then 2000 * (cnt_from_2nd_stacked - 4)

        when hub_type_original = '5 hour shift' and cnt_from_2nd_stacked >= 14 and kpi_adjusted = 1 then (5000 * (cnt_from_2nd_stacked -  13)) + 15000
        when hub_type_original = '5 hour shift' and cnt_from_2nd_stacked >= 9 and kpi_adjusted = 1 then 3000 * (cnt_from_2nd_stacked - 8)

        else 0 end bonus_value_adjust

from f 
)
select distinct hub_type_x_start_time FROM final_

        f.report_date,
        f.shipper_id,
        sm.shipper_name,
        sm.city_name,
        'spf_do_0007||HUB_MODEL_Gia tang thu nhap don cho tai xe_'||date_format(f.report_date,'%Y-%m-%d') as txn_note,
        array_agg(distinct case when bonus_value_adjust > 0 then hub_type_x_start_time end) as shift_ext_info,
        sum(total_order) as cnt_order,
        sum(cnt_from_2nd_stacked) as cnt_stacked,
        sum(bonus_value) as original_bonus,
        sum(bonus_value_adjust) as bonus_value_adjust

from final_ f 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = f.shipper_id and sm.grass_date = 'current'

group by 1,2,3,4,5
having sum(bonus_value_adjust) > 0