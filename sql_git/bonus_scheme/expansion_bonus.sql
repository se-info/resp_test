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
        year(dp.report_date)*100 + week(dp.report_date) as year_week, 
        dp.shipper_id,
        sm.shipper_name,
        sm.city_name,
        'spf_do_0005|Gia tang thu nhap cho tai xe khu vuc moi_'||date_format(pn.min_date,'%Y-%m-%d')||'_'|| date_format(pn.max_date,'%Y-%m-%d') as note,
        max_by(dp.shipper_type,dp.report_date) as shipper_type,
        sum(dp.total_order) as ado,
        case 
        when sum(dp.total_order) >= 70 then 200000
        when sum(dp.total_order) >= 40 then 100000
        else 0 end as bonus

from driver_ops_driver_performance_tab dp

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = dp.shipper_id and sm.grass_date = 'current'

LEFT JOIN pay_note pn 
    on pn.week_num = (year(dp.report_date)*100 + week(dp.report_date))

where dp.report_date between date'2024-07-15' and date'2024-08-11'
and dp.total_order > 0
and dp.shipper_id IN 
(21671089,
21447937,
23174517,
40002460,
40157487,
40900450,
41239904,
41557632,
41587171,
42125165,
42329100,
50164508,
50198358,
50254150,
50705941,
50745760,
50745973,
50745963,
50746147,
50746477,
50746512,
50746557,
50746791,
50746943,
50747062,
50747374,
50747444,
50747516,
50747582,
50747835,
50747934,
50748152,
50748304,
50748434,
50748501,
50748563,
50748695,
50748732,
50748746,
50748749,
50748854,
50748952,
50750042,
50750043,
50750192,
50750196,
50750332,
50750339,
50750464,
50750490,
50750499,
50750719,
50750758,
50750759,
50751005,
50751285,
50751652,
50751761,
50751844,
50751852,
50751865,
50751870,
50751888,
50752096,
50752336,
50752381,
50752431)

group by 1,2,3,4,5 
having sum(total_order) >= 40