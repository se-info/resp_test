with raw_date as 
(SELECT

      DATE(report_date) AS report_date
FROM
    (
(
SELECT sequence(date'2023-01-01',date'2023-12-24') bar)

CROSS JOIN
    unnest (bar) as t(report_date)
)
)
,params_date(period_grp,period,start_date,end_date,days) as 
(
SELECT 
        '1. Daily'
        ,CAST(report_date as varchar)
        ,CAST(report_date as date)
        ,CAST(report_date as date)
        ,CAST(1 as double)

from raw_date
where cast(report_date as date) >= date'2023-12-01'
UNION 

SELECT 
        '2. Weekly'
        ,'W- ' || CAST(YEAR(date_trunc('week',report_date))*100 + WEEK(date_trunc('week',report_date)) as varchar)
        ,CAST(date_trunc('week',report_date) as date)
        ,max(report_date)
        ,date_diff('day',cast(date_trunc('week',report_date) as date),max(report_date)) + 1

from raw_date

group by 1,2,3

UNION 

SELECT 
        '3. Monthly'
        ,'M- ' || CAST(YEAR(date_trunc('month',report_date))*100 + MONTH(date_trunc('month',report_date)) as varchar)
        ,date_trunc('month',report_date)
        ,max(report_date)
        ,date_diff('day',cast(date_trunc('month',report_date) as date),max(report_date)) + 1

from raw_date

group by 1,2,3
) 
,sa as 
(select 
        *, 
        case 
        when order_type = 'Group' then 'group'
        when assign_type = '6. New Stack Assign' then 'stack'
        else 'single' end as assign_type_v2
from driver_ops_order_assign_log_tab
where status in (3,4,2,13,14,15,7)
)
,metrics as 
(select  
        de.*,
        r.city_name,
        di.name_en as district_name,
        de.order_code||'-'||cast(de.shipper_id as varchar) as check_dup,
        sa.assign_type_v2 as order_assign_type

from dev_vnfdbi_opsndrivers.driver_ops_deny_log_tab de 

left join driver_ops_raw_order_tab r 
    on de.delivery_id = r.delivery_id

left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live di on di.id = r.district_id

left join sa 
    on sa.order_code = de.order_code
    and sa.driver_id = de.shipper_id
    and de.created_ts >= sa.create_time 

-- where de.created = date'2023-12-24'
)
,f as 
(select 
        created,
        city_name,
        district_name,
        count(case when order_assign_type = 'group' then check_dup else null end) as total_denied_group,
        count(case when order_assign_type = 'stack' then check_dup else null end) as total_denied_stack,
        count(case when order_assign_type = 'single' then check_dup else null end) as total_denied_single
from metrics 

group by 1,2,3)
select 
        p.period,
        f.city_name,
        f.district_name,
        sum(total_denied_group)/cast(p.days as double) as total_denied_group,
        sum(total_denied_stack)/cast(p.days as double) as total_denied_stack,
        sum(total_denied_single)/cast(p.days as double) as total_denied_single

from f 

inner join params_date p
    on f.created between p.start_date and p.end_date

group by 1,2,3,p.days

