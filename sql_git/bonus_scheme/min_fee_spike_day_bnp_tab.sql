WITH pay_note(week_num,max_date,min_date) as 
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
select 
        date(delivered_date) as report_date,
        partner_id,
        full_name,
        city_name,
        -- 'HUB_MODEL_Thuong tai xe guong mau chu nhat tuan '||date_format(pn.min_date,'%d/%m')||' - '|| date_format(pn.max_date,'%d/%m') as sunday_note,
        'ADJUSTMENT_SHIPPING FEE_HUB_07/07/2024' as note_,
        count(distinct order_id) as total_order,
        sum(diff) as adjustment
from
(select *,YEAR(date(delivered_date))*100 + WEEK(date(delivered_date)) AS created_week from dev_vnfdbi_opsndrivers.shopeefood_vn_tet_holiday_min_fee_tab_adhoc
where 1=1
and delivered_date = date '2024-07-07' 
and is_hub_order = 1
and autopay_date = delivered_date
and is_need_adjust_shipping_fee = 1
and source in ('Food','Market')
) a 
LEFT JOIN pay_note pn 
    on pn.week_num = a.created_week

group by 1,2,3,4,5