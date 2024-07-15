with check_list as 
(select 
        uid,
        -- case 
        -- when template_id = 1 then ' Received supporting documents'
        -- when template_id = 2 then ' Received deposit'
        -- when template_id = 3 then ' Shipper received uniform'
        -- when template_id = 4 then ' 01 Criminal record check (photo of the front of the record)'
        -- when template_id = 5 then ' Identity card / Passport (Notarized)'
        -- when template_id = 6 then ' 01 Driving license (photo of the front and back of license)'
        -- when template_id = 7 then ' 01 Vehicle registration certificate (photo of the front and back of license)'
        -- when template_id = 8 then ' Sacombank ATM card (Internet Banking)'
        -- when template_id = 9 then ' 01 Family register or Temporary Residence Card (photo of householder information + photo of driver information)'
        -- when template_id = 10 then ' Curriculum vitae (Notarized)'
        -- when template_id = 11 then ' Health certificate (last 6 months)'
        -- when template_id = 12 then ' Have AirPay account with GIRO' end as template_name
        count(distinct template_id) as total_documents,
        count(distinct case when template_id = 6 then template_id else null end) as "bằng lái xe",
        count(distinct case when template_id = 4 then template_id else null end) as "lý lịch tư pháp",
        count(distinct case when template_id = 5 then template_id else null end) as "chứng minh thư",
        count(distinct case when template_id = 7 then template_id else null end) as "giấy tờ xe"

from shopeefood.foody_internal_db__shipper_check_list_tab__reg_daily_s0_live
-- where template_id in (6,7)
where 1 = 1 
and template_id in (4,5,6,7)
-- change binary to varchar
and from_utf8(is_checked) = '1'
group by 1 
)
,agg as 
(select 
        shipper_id,
        array_agg(distinct date(delivered_timestamp)) as agg_delivered

from driver_ops_raw_order_tab
where order_status = 'Delivered'
group by 1
)

select 
        r.uid,
        sm.shipper_name,
        sm.city_name,
        r.shopee_uid,
        case 
        when r.take_order_status = 1 then 'Normal' 
        when r.take_order_status = 2 then 'Stop'
        else 'Pending' end as order_status,
        'Working' as working_status,
        date(from_unixtime(r.create_time - 3600)) as onboard_date,
        c.total_documents,
        c."bằng lái xe",
        c."lý lịch tư pháp",
        c."chứng minh thư",
        c."giấy tờ xe",
        case 
        when r.take_order_status != 1 then coalesce(re.name_eng,null)
        else null end as pending_reason,
        if(cardinality(filter(agg.agg_delivered,x -> x = current_date - interval '1' day)) > 0,1,0) as is_a1,
        if(cardinality(filter(agg.agg_delivered,x -> x between current_date - interval '30' day and current_date - interval '1' day)) > 0,1,0) as is_a30,
        if(cardinality(filter(agg.agg_delivered,x -> x between current_date - interval '60' day and current_date - interval '1' day)) > 0,1,0) as is_a60,
        if(cardinality(filter(agg.agg_delivered,x -> x between current_date - interval '90' day and current_date - interval '1' day)) > 0,1,0) as is_a90,
        if(cardinality(filter(agg.agg_delivered,x -> x between current_date - interval '120' day and current_date - interval '1' day)) > 0,1,0) as is_a120

from shopeefood.foody_internal_db__shipper_profile_tab__reg_continuous_s0_live r 

left join check_list c on c.uid = r.uid

left join agg on agg.shipper_id = r.uid

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = r.uid and sm.grass_date = 'current'

left join 
(select *,row_number()over(partition by uid order by id desc) as rank_ 
from shopeefood.foody_internal_db__shipper_log_pending_reason_tab__reg_daily_s0_live 
where is_deleted = 0
)pd on pd.uid = sm.shipper_id and rank_ = 1

left join dev_vnfdbi_opsndrivers.driver_pending_reason_tab re on cast(re.reason_id as bigint) = pd.reason_id

where working_status = 1 
and regexp_like(sm.city_name,'Dien Bien|Test|Stress') = false 
