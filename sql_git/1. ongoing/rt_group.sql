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
        date_trunc('month',created_date) as month_,
        count(distinct case when is_realtime_group = 1 then order_code else null end)*1.00/cast(count(distinct created_date) as double) as rt_group,
        count(distinct order_code)*1.00/cast(count(distinct created_date) as double) as total_ado,
        sum(case when group_id > 0 then re_group else null end)/cast(count(distinct case when group_id > 0 then order_code else null end) as double) as avg_re
from
(select 
        ro.created_date,
        ro.order_code,
        if(ro.order_type=0,'1. delivery','2. spxi') as source,
        ro.group_id,
        raw.is_realtime_group,
        raw.re_group

from driver_ops_raw_order_tab ro 
left join raw on ro.group_id = raw.id and ro.order_type = raw.ref_order_category

where 1 = 1 
and ro.order_type != 0
and ro.order_status = 'Delivered'
and ro.created_date between date_trunc('month',current_date) - interval '3' month and current_date - interval '1' day 
)
group by 1;
