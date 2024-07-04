with raw as 
(select user_id as partner_id,id,reference_id,balance,note,split(note,'_') as note_split
from shopeefood.foody_accountant_db__partner_transaction_tab__reg_continuous_s0_live
where note like '%SHIPPING FEE_HUB%'
and date(from_unixtime(create_time - 3600)) >= date'2024-01-01'
)
,min_fee as 
(select 
        partner_id,
        cast(date_parse(note_split[cardinality(note_split)],'%d/%m/%Y') as date) as date_adjust,
        note,
        sum(balance*1.00/100) as adjust


from raw 
where 1 = 1 
and cast(date_parse(note_split[cardinality(note_split)],'%d/%m/%Y') as date) >= date'2024-02-02'
group by 1,2,3
)
,week_raw as 
(select 
        user_id as partner_id,
        id,
        reference_id,
        balance,note,
        split(split(note_trim,'_')[cardinality(split(note_trim,'_'))],' ') as note_split,
        year(date(from_unixtime(create_time - 3600))) as year_

from (select *,trim(note) as note_trim from shopeefood.foody_accountant_db__partner_transaction_tab__reg_continuous_s0_live )
where note like 'HUB_MODEL_Thuong tai xe guong mau%'
)
,weekly as 
(select
        year(cast(a.end_date as date)) * 100 + week(cast(a.end_date as date)) as bonus_week,
        cast(a.end_date as date) - interval '6' day as start_date,
        cast(a.end_date as date) as end_date,
        a.partner_id,
        a.balance*1.00/100 as bonus_value,
        a.note,
        array_agg(distinct dp.shipper_id) as working_day_info,
        cardinality(array_agg(distinct dp.report_date)) as working_day,
        (a.balance*1.00/100)/cardinality(array_agg(distinct dp.report_date)) as bonus_allocate

from 
(select  
        try(date_parse(note_split[cardinality(note_split)]||'/'||cast(year_ as varchar),'%d/%m/%Y')) as end_date,
        *
from week_raw ) a 
left join driver_ops_driver_performance_tab dp 
    on dp.shipper_id=  a.partner_id
    and dp.report_date between cast(a.end_date as date) - interval '6' day and cast(a.end_date as date)
    and dp.total_order > 0 

where date(end_date) >= date'2024-01-01'
group by 1,2,3,4,5,6,a.balance
)
,f as 
(select 
        year(dp.report_date)*100 + week(dp.report_date) as report_week,
        dp.report_date,
        dp.shipper_id,
        dp.shipper_tier,
        dp.city_name,
        dp.total_order,
        dp.hub_order,
        dp.driver_income,
        coalesce(m.adjust,0) as min_fee_adjust,
        coalesce(w.bonus_allocate,0) as weekly_bonus

from driver_ops_driver_performance_tab dp

left join min_fee m on m.partner_id = dp.shipper_id and m.date_adjust = dp.report_date

left join weekly w on w.partner_id = dp.shipper_id and w.bonus_week = (year(dp.report_date)*100 + week(dp.report_date))

where dp.hub_order > 0 
and dp.shipper_tier = 'Hub'
and dp.report_date >= date'2024-02-02'
)
select 
        report_date,
        city_name,
        shipper_tier,
        (sum(driver_income) + sum(min_fee_adjust) + sum(weekly_bonus))/cast(count(distinct shipper_id) as double) as avg_income,
        sum(weekly_bonus) as weekly_bonus

from f 
where report_date <= date'2024-02-18'
group by 1,2,3

