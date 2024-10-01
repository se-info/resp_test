with filter_hub as 
(select 
array['Bình Thạnh X','Bình Thạnh Y','Gò Vấp X','HCM_ Binh Thanh A','HCM_ Binh Thanh B','HCM_ Binh Thanh C','HCM_ Go Vap A','HCM_ Go Vap B','HCM_ Go Vap C','HCM_ Go Vap D','HCM_ Phú Nhuận','HCM_ Q11','HCM_Q12 A','HCM_Q12 B','HCM_Q12 C','HCM_Q2 B','HCM_Tan Binh B','Phú Nhuận X','Tân Bình X','Tân Bình Y','HCM_Q10','HCM_Q3','Quận 1 X','Quận 3 X','Quận 5 X','Quận 10 X','Quận 11 X','HCM_Q1 A','HCM_Q1 B'] 
        as batch1,
array['HCM_Q5','HCM_Q6 A','Quận 6 X','Tân Phú X','HCM_Q8 A','HCM_Q8 B','HCM_Binh Tan D','HCM_Tan Binh A','HCM_Tan Phu C','HCM_ Tan Phu A','HCM_Binh Tan F','HCM_ Tan Phu B','HCM_Binh Tan E','HCM_Binh Tan A','HCM_Binh Tan B','HCM_Q6 B','HCM_Q2 A','HCM_Q2 C','HCM_Q2 D','HCM_Q4','HCM_Q7 B','HCM_Q7 C','HCM_Q7 D','HCM_Q7 E','HCM_Q9 A','HCM_Q9 C','HCM_Q9 B','HCM_Thu Duc A','HCM_Thu Duc E','HCM_Thu Duc F','Bình Thạnh X','Bình Thạnh Y','Gò Vấp X','HCM_ Binh Thanh A','HCM_ Binh Thanh B','HCM_ Binh Thanh C','HCM_ Go Vap A','HCM_ Go Vap B','HCM_ Go Vap C','HCM_ Go Vap D','HCM_ Phú Nhuận','HCM_ Q11','HCM_Q12 A','HCM_Q12 B','HCM_Q12 C','HCM_Q2 B','HCM_Tan Binh B','Phú Nhuận X','Tân Bình X','Tân Bình Y','HCM_Q10','HCM_Q3','Quận 1 X','Quận 3 X','Quận 5 X','Quận 10 X','Quận 11 X','HCM_Q1 A','HCM_Q1 B']
        as batch2
)
,params(report_date,uid,hub_type_x_start_time) AS
(VALUES
(date'2024-09-23',23082769,'5 hour shift-8'),
(date'2024-09-24',50762166,'5 hour shift-11'),
(date'2024-09-24',50754503,'3 hour shift-21'),
(date'2024-09-25',42198763,'5 hour shift-11'),
(date'2024-09-25',50763551,'5 hour shift-11'),
(date'2024-09-25',40782709,'5 hour shift-11'),
(date'2024-09-23',40039820,'5 hour shift-11'),
(date'2024-09-26',50030289,'5 hour shift-8'),
(date'2024-09-26',41735243,'5 hour shift-11'),
(date'2024-09-26',40018429,'5 hour shift-11'),
(date'2024-09-26',50549033,'5 hour shift-11'),
(date'2024-09-26',41992244,'5 hour shift-11'),
(date'2024-09-26',40223308,'5 hour shift-11'),
(date'2024-09-26',40302624,'5 hour shift-18'),
(date'2024-09-26',40007861,'5 hour shift-18'),
(date'2024-09-26',50672732,'5 hour shift-11'),
(date'2024-09-26',50610946,'5 hour shift-11'),
(date'2024-09-27',23082769,'5 hour shift-8'),
(date'2024-09-27',50031412,'5 hour shift-8'),
(date'2024-09-26',22901719,'5 hour shift-11'),
(date'2024-09-27',50343804,'5 hour shift-11'),
(date'2024-09-27',41558089,'5 hour shift-11'),
(date'2024-09-27',41243908,'5 hour shift-11'),
(date'2024-09-27',22141792,'5 hour shift-11'),
(date'2024-09-28',41300954,'5 hour shift-8'),
(date'2024-09-27',23107566,'5 hour shift-18'),
(date'2024-09-27',22411538,'5 hour shift-18'),
(date'2024-09-27',50749189,'3 hour shift-21'),
(date'2024-09-28',40119948,'5 hour shift-8'),
(date'2024-09-28',40814169,'3 hour shift-21'),
(date'2024-09-28',40802274,'5 hour shift-11'),
(date'2024-09-28',23082769,'5 hour shift-8'),
(date'2024-09-23',41976097,'5 hour shift-18'),
(date'2024-09-28',40039820,'3 hour shift-18'),
(date'2024-09-28',50374389,'5 hour shift-8'),
(date'2024-09-28',50755653,'5 hour shift-11'),
(date'2024-09-28',50343909,'5 hour shift-18'),
(date'2024-09-29',50767689,'5 hour shift-11'),
(date'2024-09-29',41336377,'5 hour shift-11'),
(date'2024-09-23',41957945,'5 hour shift-8'),
(date'2024-09-23',40002347,'5 hour shift-8'),
(date'2024-09-23',41090596,'5 hour shift-11'),
(date'2024-09-23',41039808,'5 hour shift-11'),
(date'2024-09-23',17438712,'5 hour shift-16'),
(date'2024-09-23',50735504,'5 hour shift-16'),
(date'2024-09-23',41084486,'5 hour shift-8'),
(date'2024-09-23',18204505,'5 hour shift-18'),
(date'2024-09-24',42324590,'5 hour shift-11'),
(date'2024-09-24',41391055,'5 hour shift-16'),
(date'2024-09-24',50764599,'5 hour shift-16'),
(date'2024-09-24',50754047,'5 hour shift-18'),
(date'2024-09-25',22835859,'5 hour shift-8'),
(date'2024-09-25',50572351,'5 hour shift-16'),
(date'2024-09-25',12733407,'5 hour shift-8'),
(date'2024-09-25',23120524,'5 hour shift-18'),
(date'2024-09-26',8200322,'5 hour shift-6'),
(date'2024-09-26',23080575,'5 hour shift-6'),
(date'2024-09-26',50747462,'5 hour shift-11'),
(date'2024-09-26',12446059,'5 hour shift-11'),
(date'2024-09-26',41991424,'5 hour shift-11'),
(date'2024-09-26',40237608,'5 hour shift-11'),
(date'2024-09-27',10026991,'5 hour shift-8'),
(date'2024-09-27',40269224,'5 hour shift-11'),
(date'2024-09-27',12446059,'5 hour shift-11'),
(date'2024-09-27',41473858,'5 hour shift-11'),
(date'2024-09-27',22642093,'5 hour shift-11'),
(date'2024-09-27',22049949,'5 hour shift-11'),
(date'2024-09-27',21646327,'5 hour shift-11'),
(date'2024-09-27',16666538,'5 hour shift-11'),
(date'2024-09-27',14217070,'5 hour shift-11'),
(date'2024-09-28',50749525,'5 hour shift-11'),
(date'2024-09-28',40801504,'5 hour shift-11'),
(date'2024-09-28',40827659,'5 hour shift-11'),
(date'2024-09-28',40797662,'5 hour shift-11'),
(date'2024-09-27',40023045,'5 hour shift-11'),
(date'2024-09-26',14446416,'5 hour shift-11'),
(date'2024-09-26',50750870,'5 hour shift-16'),
(date'2024-09-28',23080575,'5 hour shift-11'),
(date'2024-09-28',41609819,'5 hour shift-18'),
(date'2024-09-29',50766868,'5 hour shift-8'),
(date'2024-09-29',50759866,'5 hour shift-11'),
(date'2024-09-29',40797662,'5 hour shift-11'),
(date'2024-09-29',42170301,'5 hour shift-11'),
(date'2024-09-29',23013624,'5 hour shift-11'),
(date'2024-09-29',41297673,'5 hour shift-11'),
(date'2024-09-29',50352339,'5 hour shift-16'),
(date'2024-09-23',40903659,'8 hour shift-11'),
(date'2024-09-23',10391947,'10 hour shift-10'),
(date'2024-09-23',15595349,'10 hour shift-10'),
(date'2024-09-23',23122434,'8 hour shift-11'),
(date'2024-09-24',9795649,'10 hour shift-10'),
(date'2024-09-24',11988895,'10 hour shift-10'),
(date'2024-09-24',50760653,'8 hour shift-11'),
(date'2024-09-24',41690433,'8 hour shift-11'),
(date'2024-09-25',16158142,'8 hour shift-11'),
(date'2024-09-25',9795649,'10 hour shift-10'),
(date'2024-09-25',20406944,'10 hour shift-10'),
(date'2024-09-25',50377278,'8 hour shift-11'),
(date'2024-09-25',41446048,'8 hour shift-11'),
(date'2024-09-25',41568136,'8 hour shift-11'),
(date'2024-09-25',10391947,'10 hour shift-10'),
(date'2024-09-25',20605453,'10 hour shift-10'),
(date'2024-09-25',17135262,'10 hour shift-10'),
(date'2024-09-26',12086658,'10 hour shift-10'),
(date'2024-09-26',7075148,'8 hour shift-11'),
(date'2024-09-26',50598014,'8 hour shift-11'),
(date'2024-09-27',22389899,'8 hour shift-11'),
(date'2024-09-27',40953679,'8 hour shift-11'),
(date'2024-09-27',40188845,'10 hour shift-10'),
(date'2024-09-27',16405945,'10 hour shift-10'),
(date'2024-09-27',40857298,'8 hour shift-11'),
(date'2024-09-27',50765360,'10 hour shift-10'),
(date'2024-09-27',40162137,'10 hour shift-10'),
(date'2024-09-28',23127102,'8 hour shift-11'),
(date'2024-09-28',22618264,'8 hour shift-11'),
(date'2024-09-28',50761966,'8 hour shift-11'),
(date'2024-09-28',40379852,'8 hour shift-11'),
(date'2024-09-28',40059656,'10 hour shift-10'),
(date'2024-09-28',18783236,'8 hour shift-11'),
(date'2024-09-28',50748705,'10 hour shift-10'),
(date'2024-09-28',23137552,'10 hour shift-10'),
(date'2024-09-28',9977569,'10 hour shift-10'),
(date'2024-09-27',40650999,'10 hour shift-10'),
(date'2024-09-28',40010780,'8 hour shift-11'),
(date'2024-09-28',50164497,'8 hour shift-11'),
(date'2024-09-29',19899313,'10 hour shift-10'),
(date'2024-09-29',8551029,'10 hour shift-10'),
(date'2024-09-29',50766671,'10 hour shift-10'),
(date'2024-09-29',50284684,'8 hour shift-11'),
(date'2024-09-29',41702443,'8 hour shift-11'),
(date'2024-09-29',7361315,'8 hour shift-11'),
(date'2024-09-28',40162096,'8 hour shift-11')
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
        r.shipper_id,
        case 
        when hub.hub_type_original in ('1 hour shift','3 hour shift','5 hour shift') then 1
        when date(r.delivered_timestamp) >= date'2024-09-23' and date(r.delivered_timestamp) < date'2024-09-26' and hub_type_original in ('10 hour shift','8 hour shift') then if(cardinality(filter(ft.batch1,x -> x = hub.hub_locations)) >0,1,0) 
        when date(r.delivered_timestamp) >= date'2024-09-26' and hub_type_original in ('10 hour shift','8 hour shift') then if(cardinality(filter(ft.batch2,x -> x = hub.hub_locations)) >0,1,0) 
        else 0 end as is_apply_bonus

from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab r 

left join hub_order_tab ho on ho.ref_order_id = r.id and ho.ref_order_category = r.order_type

left join driver_ops_hub_driver_performance_tab hub on hub.slot_id = ho.slot_id and hub.uid = r.shipper_id

cross join filter_hub ft 

where 1 = 1 
and ho.ref_order_id is not null 
and r.order_status = 'Delivered'
and date(r.delivered_timestamp) between date'2024-09-23' and date'2024-09-29' 
and hub.city_name != 'Hai Phong City'
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
        s.is_apply_bonus,
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
        case 
        when hub_type_original = '3 hour shift' and cnt_from_2nd_stacked >= 9 and kpi = 1 then (4000 * (cnt_from_2nd_stacked -  8)) + 8000
        when hub_type_original = '3 hour shift' and cnt_from_2nd_stacked >= 5 and kpi = 1 then 2000 * (cnt_from_2nd_stacked - 4)

        when hub_type_original = '5 hour shift' and cnt_from_2nd_stacked >= 14 and kpi = 1 then (5000 * (cnt_from_2nd_stacked -  13)) + 15000
        when hub_type_original = '5 hour shift' and cnt_from_2nd_stacked >= 9 and kpi = 1 then 3000 * (cnt_from_2nd_stacked - 8)

        when hub_type_original = '8 hour shift' and cnt_from_2nd_stacked >= 20 and kpi = 1 and is_apply_bonus = 1  then (6000 * (cnt_from_2nd_stacked -  19)) + 20000
        when hub_type_original = '8 hour shift' and cnt_from_2nd_stacked >= 15 and kpi = 1 then 4000 * (cnt_from_2nd_stacked - 14)

        when hub_type_original = '10 hour shift' and cnt_from_2nd_stacked >= 23 and kpi = 1 and is_apply_bonus = 1  then (6000 * (cnt_from_2nd_stacked -  22)) + 20000
        when hub_type_original = '10 hour shift' and cnt_from_2nd_stacked >= 18 and kpi = 1 then 4000 * (cnt_from_2nd_stacked - 17)

        else 0 end as bonus_value,
        total_order,
        cnt_from_2nd_stacked,
        case 
        when hub_type_original = '3 hour shift' and cnt_from_2nd_stacked >= 9 and kpi_adjusted = 1 then (4000 * (cnt_from_2nd_stacked -  8)) + 8000
        when hub_type_original = '3 hour shift' and cnt_from_2nd_stacked >= 5 and kpi_adjusted = 1 then 2000 * (cnt_from_2nd_stacked - 4)

        when hub_type_original = '5 hour shift' and cnt_from_2nd_stacked >= 14 and kpi_adjusted = 1 then (5000 * (cnt_from_2nd_stacked -  13)) + 15000
        when hub_type_original = '5 hour shift' and cnt_from_2nd_stacked >= 9 and kpi_adjusted = 1 then 3000 * (cnt_from_2nd_stacked - 8)

        when hub_type_original = '8 hour shift' and cnt_from_2nd_stacked >= 20 and kpi_adjusted = 1 and is_apply_bonus = 1  then (6000 * (cnt_from_2nd_stacked -  19)) + 20000
        when hub_type_original = '8 hour shift' and cnt_from_2nd_stacked >= 15 and kpi_adjusted = 1 then 4000 * (cnt_from_2nd_stacked - 14)

        when hub_type_original = '10 hour shift' and cnt_from_2nd_stacked >= 23 and kpi_adjusted = 1 and is_apply_bonus = 1  then (6000 * (cnt_from_2nd_stacked -  22)) + 20000
        when hub_type_original = '10 hour shift' and cnt_from_2nd_stacked >= 18 and kpi_adjusted = 1 then 4000 * (cnt_from_2nd_stacked - 17)

        else 0 end bonus_value_adjust

from f 
)
select
        f.report_date,
        f.shipper_id,
        sm.shipper_name,
        sm.city_name,
        'spf_do_0007||HUB_MODEL_Gia tang thu nhap don cho tai xe_'||date_format(f.report_date,'%Y-%m-%d') as txn_note,
        array_agg(distinct case when bonus_value_adjust > 0 then hub_type_x_start_time end) as shift_ext_info,
        map_agg(hub_type_x_start_time,bonus_value_adjust) as bonus_ext_info,
        sum(total_order) as cnt_order,
        sum(cnt_from_2nd_stacked) as cnt_stacked,
        sum(bonus_value) as original_bonus,
        sum(bonus_value_adjust) as bonus_value_adjust

from final_ f 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = f.shipper_id and sm.grass_date = 'current'

group by 1,2,3,4,5
having sum(bonus_value_adjust) > 0


