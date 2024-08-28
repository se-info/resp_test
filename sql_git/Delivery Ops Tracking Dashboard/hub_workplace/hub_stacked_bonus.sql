WITH params(report_date,uid,hub_type_x_start_time) AS
(VALUES
(date'2024-08-19',23118280,'5 hour shift-8'),
(date'2024-08-20',41932631,'5 hour shift-18'),
(date'2024-08-22',42218501,'5 hour shift-11'),
(date'2024-08-22',50539632,'5 hour shift-11'),
(date'2024-08-22',50752311,'5 hour shift-11'),
(date'2024-08-23',13064243,'5 hour shift-8'),
(date'2024-08-23',14631984,'5 hour shift-8'),
(date'2024-08-23',50309907,'5 hour shift-11'),
(date'2024-08-23',42324590,'5 hour shift-11'),
(date'2024-08-23',41058400,'5 hour shift-6'),
(date'2024-08-23',21560579,'5 hour shift-8'),
(date'2024-08-23',40162647,'5 hour shift-8'),
(date'2024-08-23',23144181,'5 hour shift-6'),
(date'2024-08-23',40610724,'5 hour shift-11'),
(date'2024-08-24',50377440,'5 hour shift-8'),
(date'2024-08-24',18480376,'5 hour shift-6'),
(date'2024-08-24',41192070,'5 hour shift-18'),
(date'2024-08-24',50072548,'5 hour shift-18'),
(date'2024-08-24',42324577,'5 hour shift-18'),
(date'2024-08-25',16009183,'5 hour shift-8'),
(date'2024-08-25',41412271,'5 hour shift-11'),
(date'2024-08-25',22821843,'5 hour shift-11'),
(date'2024-08-25',40050679,'5 hour shift-8'),
(date'2024-08-23',40936706,'5 hour shift-11'),
(date'2024-08-25',41072333,'5 hour shift-11'),
(date'2024-08-25',41264589,'5 hour shift-11'),
(date'2024-08-25',50749525,'5 hour shift-11'),
(date'2024-08-25',50389031,'5 hour shift-11'),
(date'2024-08-25',22138424,'5 hour shift-11'),
(date'2024-08-25',41740977,'5 hour shift-16'),
(date'2024-08-25',50076407,'5 hour shift-16'),
(date'2024-08-25',23113360,'5 hour shift-16'),
(date'2024-08-25',50339489,'5 hour shift-11'),
(date'2024-08-25',23069073,'5 hour shift-18'),
(date'2024-08-25',50405316,'5 hour shift-18'),
(date'2024-08-25',42060106,'5 hour shift-11'),
(date'2024-08-25',50501020,'3 hour shift-21'),
(date'2024-08-24',50753395,'5 hour shift-18'),
(date'2024-08-22',50180245,'5 hour shift-8'),
(date'2024-08-22',50747567,'5 hour shift-11'),
(date'2024-08-22',12316505,'5 hour shift-11'),
(date'2024-08-22',17527081,'5 hour shift-11'),
(date'2024-08-23',41391954,'5 hour shift-8'),
(date'2024-08-24',50163106,'5 hour shift-11'),
(date'2024-08-24',50061952,'5 hour shift-11'),
(date'2024-08-24',50749189,'3 hour shift-21'),
(date'2024-08-24',22411538,'5 hour shift-18'),
(date'2024-08-25',41955692,'5 hour shift-11'),
(date'2024-08-25',50611087,'3 hour shift-10'),
(date'2024-08-22',50747775,'5 hour shift-18')
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
        case 
        when hub.hub_locations in ('HCM_ Go Vap C','HCM_ Go Vap D','HCM_ Binh Thanh A','HCM_Q2 B','HCM_ Binh Thanh B','HCM_ Go Vap A','HCM_ Go Vap B','HCM_Q12 B','HCM_Q12 A','HCM_Q12 C','HCM_ Binh Thanh C','HCM_ Phú Nhuận','HCM_ Q11','HCM_Tan Binh B','Bình Thạnh X','Gò Vấp X','Bình Thạnh Y','Phú Nhuận X','Tân Bình X','Tân Bình Y') then 'phase1'
        when hub.hub_locations in ('HCM_Q5','HCM_Q6 A','Quận 6 X','Tân Phú X','HCM_Q8 A','HCM_Q8 B','HCM_Binh Tan D','HCM_Tan Binh A','HCM_Tan Phu C','HCM_ Tan Phu A','HCM_Binh Tan F','HCM_ Tan Phu B','HCM_Binh Tan E','HCM_Binh Tan A','HCM_Binh Tan B','HCM_Q6 B','HCM_Q10','HCM_Q3','Quận 1 X','Quận 3 X','Quận 5 X','Quận 10 X','Quận 11 X','HCM_Q1 A','HCM_Q1 B') then 'phase2'
        when hub.hub_locations in ('Tay Ho C','Bac Tu Liem B','Bac Tu Liem D','Long Bien A','Long Biên B','Long Biên C','Cau Giay A','Cau Giay B','Nam Tu Liem B','Nam Từ Liêm A','Ba Dinh','Dong Da A','Thanh Xuan C','Thanh Xuan F','Nam Tu Liem C','Thanh Xuan E','Hà Đông X','Thanh Xuân X','Đống Đa Y','Hai bà Trưng X','Cầu Giấy Y','Đống Đa X','Ba Đình X','Cầu Giấy X') then 'phase2'
        when hub.hub_locations in ('HCM_Q2 A','HCM_Q2 C','HCM_Q2 D','HCM_Q4','HCM_Q7 B','HCM_Q7 C','HCM_Q7 D','HCM_Q7 E','HCM_Q9 A','HCM_Q9 C','HCM_Q9 B','HCM_Thu Duc A','HCM_Thu Duc E','HCM_Thu Duc F') then 'phase3'
        when hub.hub_locations in ('Ha Dong B','Ha Dong E','Hà Đông C','Hoang Mai A','Thanh Tri C','Dong Da B','Hai Ba Trung A','Hai Ba Trung B','Hoan Kiem','Tay Ho A','Hoang Mai B','Thanh Xuan D','Hoàng Mai X','Hoàng Mai Y') then 'phase3'
        end as phase_rollout,
        hub.kpi,
        -- case 
        -- when r.group_id > 0 and row_number()over(partition by r.group_id order by r.id asc) > 1 then 2 
        -- when r.group_id > 0 and row_number()over(partition by r.group_id order by r.id asc) = 1 then 1 
        -- else 0 end as rank_group,
        r.shipper_id

from dev_vnfdbi_opsndrivers.phong_raw_order r 

left join hub_order_tab ho on ho.ref_order_id = r.id and ho.ref_order_category = r.order_type

left join driver_ops_hub_driver_performance_tab hub on hub.slot_id = ho.slot_id and hub.uid = r.shipper_id


where 1 = 1 
and ho.ref_order_id is not null 
and r.order_status = 'Delivered'
and date(r.delivered_timestamp) between date'2024-08-19' and date'2024-08-25' 

-- and date'2024-08-25'
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
        s.phase_rollout,
        count(distinct s.id) as total_order,
        count(distinct case when s.group_id > 0 then s.id else null end) as cnt_from_2nd_stacked

from summary s 

LEFT JOIN params p 
    on p.uid = s.shipper_id 
    and p.report_date = s.report_date
    and p.hub_type_x_start_time = s.hub_type_x_start_time            

group by 1,2,3,4,5,6,7,8
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
        phase_rollout,
        case 
        when phase_rollout = 'phase1' and report_date >= date'2024-08-19' then 
        (
        case when hub_type_original = '3 hour shift' and cnt_from_2nd_stacked >= 9 and kpi = 1 then (4000 * (cnt_from_2nd_stacked -  8)) + 8000
        when hub_type_original = '3 hour shift' and cnt_from_2nd_stacked >= 5 and kpi = 1 then 2000 * (cnt_from_2nd_stacked - 4)

        when hub_type_original = '5 hour shift' and cnt_from_2nd_stacked >= 14 and kpi = 1 then (5000 * (cnt_from_2nd_stacked -  13)) + 15000
        when hub_type_original = '5 hour shift' and cnt_from_2nd_stacked >= 9 and kpi = 1 then 3000 * (cnt_from_2nd_stacked - 8)

        else 0 end) 
        when phase_rollout = 'phase2' and report_date >= date'2024-08-22' then 
        (
        case when hub_type_original = '3 hour shift' and cnt_from_2nd_stacked >= 9 and kpi = 1 then (4000 * (cnt_from_2nd_stacked -  8)) + 8000
        when hub_type_original = '3 hour shift' and cnt_from_2nd_stacked >= 5 and kpi = 1 then 2000 * (cnt_from_2nd_stacked - 4)

        when hub_type_original = '5 hour shift' and cnt_from_2nd_stacked >= 14 and kpi = 1 then (5000 * (cnt_from_2nd_stacked -  13)) + 15000
        when hub_type_original = '5 hour shift' and cnt_from_2nd_stacked >= 9 and kpi = 1 then 3000 * (cnt_from_2nd_stacked - 8)

        else 0 end)
        when phase_rollout = 'phase3' and report_date >= date'2024-08-26' then 
        (
        case when hub_type_original = '3 hour shift' and cnt_from_2nd_stacked >= 9 and kpi = 1 then (4000 * (cnt_from_2nd_stacked -  8)) + 8000
        when hub_type_original = '3 hour shift' and cnt_from_2nd_stacked >= 5 and kpi = 1 then 2000 * (cnt_from_2nd_stacked - 4)

        when hub_type_original = '5 hour shift' and cnt_from_2nd_stacked >= 14 and kpi = 1 then (5000 * (cnt_from_2nd_stacked -  13)) + 15000
        when hub_type_original = '5 hour shift' and cnt_from_2nd_stacked >= 9 and kpi = 1 then 3000 * (cnt_from_2nd_stacked - 8)

        else 0 end)
        else 0 
        end as bonus_value,
        total_order,
        cnt_from_2nd_stacked,
        case 
        when phase_rollout = 'phase1' and report_date >= date'2024-08-19' then 
        (
        case when hub_type_original = '3 hour shift' and cnt_from_2nd_stacked >= 9 and kpi_adjusted = 1 then (4000 * (cnt_from_2nd_stacked -  8)) + 8000
        when hub_type_original = '3 hour shift' and cnt_from_2nd_stacked >= 5 and kpi_adjusted = 1 then 2000 * (cnt_from_2nd_stacked - 4)

        when hub_type_original = '5 hour shift' and cnt_from_2nd_stacked >= 14 and kpi_adjusted = 1 then (5000 * (cnt_from_2nd_stacked -  13)) + 15000
        when hub_type_original = '5 hour shift' and cnt_from_2nd_stacked >= 9 and kpi_adjusted = 1 then 3000 * (cnt_from_2nd_stacked - 8)

        else 0 end) 
        when phase_rollout = 'phase2' and report_date >= date'2024-08-22' then 
        (
        case when hub_type_original = '3 hour shift' and cnt_from_2nd_stacked >= 9 and kpi_adjusted = 1 then (4000 * (cnt_from_2nd_stacked -  8)) + 8000
        when hub_type_original = '3 hour shift' and cnt_from_2nd_stacked >= 5 and kpi_adjusted = 1 then 2000 * (cnt_from_2nd_stacked - 4)

        when hub_type_original = '5 hour shift' and cnt_from_2nd_stacked >= 14 and kpi_adjusted = 1 then (5000 * (cnt_from_2nd_stacked -  13)) + 15000
        when hub_type_original = '5 hour shift' and cnt_from_2nd_stacked >= 9 and kpi_adjusted = 1 then 3000 * (cnt_from_2nd_stacked - 8)

        else 0 end)
        when phase_rollout = 'phase3' and report_date >= date'2024-08-26' then 
        (
        case when hub_type_original = '3 hour shift' and cnt_from_2nd_stacked >= 9 and kpi_adjusted = 1 then (4000 * (cnt_from_2nd_stacked -  8)) + 8000
        when hub_type_original = '3 hour shift' and cnt_from_2nd_stacked >= 5 and kpi_adjusted = 1 then 2000 * (cnt_from_2nd_stacked - 4)

        when hub_type_original = '5 hour shift' and cnt_from_2nd_stacked >= 14 and kpi_adjusted = 1 then (5000 * (cnt_from_2nd_stacked -  13)) + 15000
        when hub_type_original = '5 hour shift' and cnt_from_2nd_stacked >= 9 and kpi_adjusted = 1 then 3000 * (cnt_from_2nd_stacked - 8)

        else 0 end)
        else 0 
        end as bonus_value_adjust

from f 
)
select 
        f.report_date,
        f.shipper_id,
        sm.shipper_name,
        sm.city_name,
        'spf_do_0007||HUB_MODEL_Gia tang thu nhap don cho tai xe_'||date_format(f.report_date,'%Y-%m-%d') as txn_note,
        array_agg(distinct hub_type_x_start_time) as shift_ext_info,
        sum(total_order) as cnt_order,
        sum(cnt_from_2nd_stacked) as cnt_stacked,
        sum(bonus_value) as original_bonus,
        sum(bonus_value_adjust) as bonus_value_adjust

from final_ f 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = f.shipper_id and sm.grass_date = 'current'

group by 1,2,3,4,5
having sum(bonus_value_adjust) > 0