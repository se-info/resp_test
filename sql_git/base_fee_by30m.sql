with report_date_time as 
(
SELECT
     DATE(report_date) AS report_date
    ,sequence(cast(t.report_date as timestamp ) ,cast(t.report_date as timestamp) + interval '86400' second, interval '1799.99' second  ) dt_array 
    ,1 as mapping
FROM
    (
(
SELECT sequence(current_date - interval '7' day, current_date - interval '1' day) bar)
CROSS JOIN

    unnest (bar) as t(report_date)
)
)
,list_time_range as 
(select 
       t1.mapping
      ,t2.dt_array_unnest as start_time 
      ,t2.dt_array_unnest + interval '1798.99' second as end_time 



from report_date_time t1 

cross join unnest (dt_array) as t2(dt_array_unnest) 

order by 2 asc) 
,driver_cost_base as 
(select 
        bf.order_id,
        (bf.driver_cost_base + bf.return_fee_share_basic) as dr_cost_base,
        bf.delivered_by,
        osl.last_auto_assign_time,
        bf.grass_date,
        bf.is_stack_group_order,
        partner_id

from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bf

left join 
(select 
        order_id,
        from_unixtime(coalesce(max(case when status = 21 then create_time else null end),0) - 3600) as last_auto_assign_time 

from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
group by 1) osl on osl.order_id = bf.order_id

WHERE bf.grass_date >= date'2024-02-26'
AND bf.status = 7
AND bf.source in ('Food')
)
,group_info as 
(select 
        ogi.group_code,
        ogm.ref_order_id,
        ogm.ref_order_category,
        cast(json_extract_scalar(ogi.extra_data,'$.re') as double) as re

from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm

left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi
    on ogm.group_id = ogi.id
    and ogm.ref_order_category = ogi.ref_order_category
where date(from_unixtime(ogm.create_time - 3600)) >= current_date - interval '7' day
order by ogm.ref_order_id desc 
)
,f as 
(select 
        d.grass_date,
        d.order_id,
        d.delivered_by,
        d.dr_cost_base,
        d.last_auto_assign_time,
        lt.start_time,
        lt.end_time,
        d.is_stack_group_order,
        case 
        when d.is_stack_group_order = 2 then 'stack'
        when d.is_stack_group_order = 1 then 'group'
        else 'single' end as assign_type,
        coalesce(gi.re,0) as re_stack_group

from driver_cost_base d 

left join list_time_range lt
        on d.last_auto_assign_time between lt.start_time and lt.end_time

left join group_info gi
        on gi.ref_order_id = d.order_id
        and gi.ref_order_category = 0
)
select 
        grass_date,
        start_time,
        end_time,
        count(distinct order_id) as ado,
        count(distinct case when delivered_by = 'hub' then order_id else null end) as ado_hub,
        count(distinct case when delivered_by != 'hub' then order_id else null end) as ado_non_hub,

---
        count(distinct case when assign_type = 'stack' then order_id else null end) as stack_ado,
        count(distinct case when delivered_by = 'hub' and assign_type = 'stack' then order_id else null end) as stack_ado_hub,
        count(distinct case when delivered_by != 'hub' and assign_type = 'stack' then order_id else null end) as stack_ado_non_hub,

---
        sum(dr_cost_base)/cast(count(distinct order_id) as double) as base_fee,
        sum(case when delivered_by = 'hub' then dr_cost_base else null end)/cast(count(distinct case when delivered_by = 'hub' then order_id else null end) as double) as hub_base_fee,
        sum(case when delivered_by != 'hub' then dr_cost_base else null end)/cast(count(distinct case when delivered_by != 'hub' then order_id else null end) as double) as non_hub_base_fee,

---
        coalesce(avg(case when re_stack_group > 0 then re_stack_group else null end),0) as re_stack,
        coalesce(avg(case when re_stack_group > 0 and delivered_by = 'hub' and assign_type = 'stack' then re_stack_group else null end),0) as hub_re_stack,
        coalesce(avg(case when re_stack_group > 0 and delivered_by != 'hub' and assign_type = 'stack' then re_stack_group else null end),0) as non_hub_re_stack

from f
group by 1,2,3
