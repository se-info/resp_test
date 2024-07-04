/* 
stack/realtime group/single/ADO/ Timeperformance của nhóm brand dk?
--> cái muốn thấy là real time group chỉ work khi nào đơn surge cao
*/
with raw as 
(select 
        id,
        group_code,
        ref_order_category,
        coalesce(cast(json_extract(extra_data,'$.group_and_assign') as bigint),0) as is_realtime_group,
        coalesce(cast(json_extract(extra_data,'$.re') as bigint),0) as re_group

from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da
where date(dt) = current_date - interval '1' day 
order by 1 desc )
select 
        -- date_trunc('month',created_date) as month_,
        created_date,
        assign_type,
        count(distinct order_code)*1.00/cast(count(distinct created_date) as double) as total_ado,
        sum(case when is_asap = 1 then e2e else null end)/count(distinct case when is_asap = 1 then order_code else null end) as e2e,
        sum(case when is_asap = 1 then created_to_last_incharged else null end)/count(distinct case when is_asap = 1 then order_code else null end) as created_to_last_incharged,
        sum(case when is_asap = 1 then last_incharged_to_pickup else null end)/count(distinct case when is_asap = 1 then order_code else null end) as last_incharged_to_pickup,
        array_agg(distinct sender_name) as mex_list,
        count(distinct case when is_asap = 1 and is_late_eta = 1 then order_code else null end)/cast(count(distinct order_code) as decimal(10,2)) as late_eta_ado,
        count(distinct case when is_asap = 1 and e2e >lt_sla then order_code else null end)/cast(count(distinct order_code) as decimal(10,2)) as late_sla_ado

from
(select 
        ro.created_date,
        ro.order_code,
        if(ro.order_type=0,'1. delivery','2. spxi') as source,
        ro.group_id,
        raw.is_realtime_group,
        raw.re_group,
        case 
        when ro.order_assign_type = 'Group' and ro.group_id > 0 and raw.is_realtime_group = 1 then 'real_time_group'
        when ro.order_assign_type = 'Group' and ro.group_id > 0 and raw.is_realtime_group = 1 then 'pre_group'
        when ro.order_assign_type != 'Group' and ro.group_id > 0 then 'stack'
        else 'single' end as assign_type,
        date_diff('second',created_timestamp,delivered_timestamp)/cast(60 as decimal(10,2)) as e2e,
        date_diff('second',created_timestamp,last_incharge_timestamp)/cast(60 as decimal(10,2)) as "created_to_last_incharged",
        date_diff('second',last_incharge_timestamp,picked_timestamp)/cast(60 as decimal(10,2)) as "last_incharged_to_pickup",
        ro.is_asap,
        ro.sender_name,
        case 
        when delivered_timestamp > eta_drop_time then 1 else 0 end as is_late_eta,
        case 
        when distance <= 1 then 30
        when distance > 1 then least(60,30 + 5*(ceiling(distance) -1))
        else null end as lt_sla


from driver_ops_raw_order_tab ro 
left join raw on ro.group_id = raw.id and ro.order_type = raw.ref_order_category

where 1 = 1 
and ro.order_type = 0
and ro.order_status = 'Delivered'
and ro.created_date between date'2024-05-01' and current_date - interval '1' day 
)
where regexp_like(sender_name,'McDonald') = true 
group by 1,2 
