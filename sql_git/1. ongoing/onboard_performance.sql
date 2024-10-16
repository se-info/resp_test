with raw as 
(select 
        report_date,
        onboard_date,
        date_diff('day',onboard_date,report_date) + 1 as from_onboard,
        if(shipper_type=12,'hub','non-hub') as working_type,
        shipper_id,
        total_order,
        online_hour,
        work_hour,
        city_name

from driver_ops_driver_performance_tab


where (onboard_date between date'2024-07-01' and date'2024-07-15'
or onboard_date between date'2024-09-15' and date'2024-09-30')
and city_id in (217,218)
and total_order > 0 
)
select 
        date_trunc('month',onboard_date) as onboard_period,
        city_name,
        working_type,
        count(distinct shipper_id) as cnt_driver_onboard,

        sum(case when from_onboard <= 7 and total_order > 0 then total_order else null end)*1.0000
                /count(distinct case when from_onboard <= 7 and total_order > 0 then shipper_id else null end) as "Avg Driver ADO in 1st 7 days since OB",

        sum(case when from_onboard <= 7 and total_order > 0 then online_hour else null end)*1.0000
                /count(distinct case when from_onboard <= 7 and total_order > 0 then shipper_id else null end) as "Avg Online time in 1st 7 days OB",

        sum(case when from_onboard <= 7 and total_order > 0 then work_hour else null end)*1.0000
                /count(distinct case when from_onboard <= 7 and total_order > 0 then shipper_id else null end) as "Avg Working time in 1st 7 days OB",

        avg(case when from_onboard <=7 then from_onboard else null end) as "Days working in first 7 days"

from raw

group by 1,2,3