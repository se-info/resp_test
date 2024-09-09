select 
        date(delivered_date) as report_date,
        partner_id,
        full_name,
        city_name,
        'ADJUSTMENT_SHIPPING FEE_HUB_'||date_format(delivered_date,'%d/%m/%Y') as note_,
        count(distinct order_id) as total_order,
        sum(diff) as adjustment
        

from dev_vnfdbi_opsndrivers.shopeefood_vn_tet_holiday_min_fee_tab_adhoc

where 1=1
and partner_id = 41044976
and delivered_date >= date '2024-08-30'
and delivered_date <= date'2024-09-03' 
and is_hub_order = 1
and autopay_date = delivered_date
and is_need_adjust_shipping_fee = 1
and min_fee_type = 'spike'
and source in ('Food','Market')
group by 1,2,3,4,5