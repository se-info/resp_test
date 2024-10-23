    with raw as 
(select
        id,
        order_code,
        quantity,
        shipper_id,
        5000 as bonus_value,
        date(first_auto_assign_timestamp) as inflow_date

from driver_ops_raw_order_tab

where date(first_auto_assign_timestamp) between date'2024-10-18' and date'2024-10-20'
and hour(first_auto_assign_timestamp) between 13 and 15
and order_type = 0
and order_status = 'Delivered'
and shipper_id > 0 
and quantity > 10
and city_id in (217,218)
)
select 
        raw.inflow_date,
        raw.shipper_id,
        sm.shipper_name,
        sm.city_name,
        'spf_do_00017|Gia tang thu nhap cho don nhieu mon_'||date_format(raw.inflow_date,'%Y-%m-%d') as txn_note,
        sum(bonus_value) as total_bonus,
        count(distinct order_code) as cnt_order

from raw 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = raw.shipper_id and sm.grass_date = 'current'

group by 1,2,3,4,5