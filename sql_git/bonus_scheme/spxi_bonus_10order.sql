with last_incharge_time_tab as 
(
    select 
        ref_order_id
        ,max(create_timestamp) as last_incharge_timestamp

    from vnfdbi_opsndrivers.shopeefood_bnp_assignment_order_tab
    -- where status in (3,4)
    where 1=1
    and ref_order_category != 0
    group by 1
)

,raw as
(select 
    bf.shipper_id
    ,sm.shipper_name
    ,sm.city_name
    ,bf.uid
    ,bf.report_date
    ,case when sm.shipper_type_id = 12 then 'Hub' else 'non-hub' end as shipper_type
    ,bf.ref_order_id
    ,bf.order_status
    ,rank () over (partition by bf.shipper_id, bf.grass_date order by bf.ref_order_id asc) as rnk
    
    -- ,coalesce(t2.completed_rate/100.00,0.00) sla
    -- ,count(distinct bf.uid) as total_orders

from dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_order_performance_dev bf

left join shopeefood.foody_mart__profile_shipper_master sm
    on bf.shipper_id = sm.shipper_id and try_cast(sm.grass_date as date) = current_date - interval '1' day
left join vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level bfo
    on bf.ref_order_id = bfo.order_id and bfo.source not in ('Food','Market')
-- inner join vnfdbi_opsndrivers.phong_test_table s
    -- on bf.shipper_id = cast(s.shipper_id as bigint)
left join last_incharge_time_tab li
    on bf.ref_order_id = li.ref_order_id

where is_del = 1
-- and bf.source in ('NowShip')
and bf.source_2 in ('now_ship_shopee')
and bfo.grass_date = date'2024-09-11'
-- and hour(inflow_timestamp) in (7,8,9,13,14,15)
and hour (li.last_incharge_timestamp) in (7,8,9,13,14,15)

)
,driver_performance as
(select 
        raw.report_date
        ,raw.shipper_id
        ,raw.shipper_name
        ,raw.city_name
        ,raw.shipper_type
        ,coalesce(t2.completed_rate/100.00,0.00) sla
        ,count(distinct raw.uid) as total_orders
        
from raw
left join shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live t2 
on raw.shipper_id = t2.uid and raw.report_date = date(from_unixtime(t2.report_date-3600))
group by 1,2,3,4,5,6
)
,eligible_driver as
(select 
    raw.*
from driver_performance raw 
inner join dev_vnfdbi_opsndrivers.driver_ops_spxi_normal_scheme p on cast(p.shipper_id as bigint) = raw.shipper_id
where sla >= 95 
and total_orders >= 10
)
select  
        report_date,
        shipper_id,
        shipper_name,
        city_name,
        sla,
        total_orders,
        80000 as bonus_value,
        'spf_do_0011|Thu nhap hoan thanh don SPX_'||date_format(report_date,'%Y-%m-%d') as txn_note


from eligible_driver  

where report_date = date'2024-09-11'
and city_name = 'Ha Noi City'


