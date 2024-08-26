with hub_order_tab as
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
where date(from_unixtime(ho.autopay_date_ts-3600)) = date'2024-08-19'
)
,filter_hub as 
(select ARRAY['HCM_ Go Vap C','HCM_ Go Vap D','HCM_ Binh Thanh A','HCM_Q2 B','HCM_ Binh Thanh B','HCM_ Go Vap A','HCM_ Go Vap B','HCM_Q12 B','HCM_Q12 A','HCM_Q12 C','HCM_ Binh Thanh C','HCM_ Phú Nhuận','HCM_ Q11','HCM_Tan Binh B','Bình Thạnh X','Gò Vấp X','Bình Thạnh Y','Phú Nhuận X','Tân Bình X','Tân Bình Y'] as hub_filter)
,summary as 
(select 
        r.group_id,
        r.id,
        ho.slot_id,
        hub.hub_type_original,
        hub.hub_locations,
        -- if(cardinality(filter(ft.hub_filter,x -> x = hub.hub_locations)) >0,1,0) as is_apply_bonus,
        hub.kpi,
        -- case 
        -- when r.group_id > 0 and row_number()over(partition by r.group_id order by r.id asc) > 1 then 2 
        -- when r.group_id > 0 and row_number()over(partition by r.group_id order by r.id asc) = 1 then 1 
        -- else 0 end as rank_group,
        r.shipper_id

from driver_ops_raw_order_tab r 

left join hub_order_tab ho on ho.ref_order_id = r.id and ho.ref_order_category = r.order_type

left join driver_ops_hub_driver_performance_tab hub on hub.slot_id = ho.slot_id and hub.uid = r.shipper_id

-- cross join filter_hub ft 

where 1 = 1 
and r.city_id = 217
and ho.ref_order_id is not null 
and r.order_status = 'Delivered'
and date(r.delivered_timestamp) = date'2024-08-19'
and hub.hub_type_original in ('3 hour shift','5 hour shift')
and hub.hub_locations in ('HCM_ Go Vap C','HCM_ Go Vap D','HCM_ Binh Thanh A','HCM_Q2 B','HCM_ Binh Thanh B','HCM_ Go Vap A','HCM_ Go Vap B','HCM_Q12 B','HCM_Q12 A','HCM_Q12 C','HCM_ Binh Thanh C','HCM_ Phú Nhuận','HCM_ Q11','HCM_Tan Binh B','Bình Thạnh X','Gò Vấp X','Bình Thạnh Y','Phú Nhuận X','Tân Bình X','Tân Bình Y')
)
,f as 
(select 
        shipper_id,
        hub_type_original,
        kpi,
        slot_id,
        count(distinct id) as total_order,
        count(distinct case when group_id > 0 then id else null end) as cnt_from_2nd_stacked

from summary 


group by 1,2,3,4 
)
select 
      hub_type_original,
      sum(total_order) as ado,
      sum(cnt_from_2nd_stacked) as stacked,
      sum(bonus_value) as bonus_,
      count(distinct (shipper_id,slot_id))
from
(select
        shipper_id,
        slot_id,
        hub_type_original,
        case 
        when hub_type_original = '3 hour shift' and cnt_from_2nd_stacked >= 9 and kpi = 1 then (4000 * (cnt_from_2nd_stacked -  8)) + 8000
        when hub_type_original = '3 hour shift' and cnt_from_2nd_stacked >= 5 and kpi = 1 then 2000 * (cnt_from_2nd_stacked - 4)

        when hub_type_original = '5 hour shift' and cnt_from_2nd_stacked >= 14 and kpi = 1 then (5000 * (cnt_from_2nd_stacked -  13)) + 15000
        when hub_type_original = '5 hour shift' and cnt_from_2nd_stacked >= 9 and kpi = 1 then 3000 * (cnt_from_2nd_stacked - 8)

        else 0 end as bonus_value,
        total_order,
        cnt_from_2nd_stacked

from f 
)
group by 1 
