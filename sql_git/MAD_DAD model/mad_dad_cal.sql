with raw_date as 
(SELECT

DATE(report_date) AS report_date
FROM
(
(
SELECT sequence(date'2023-06-01',date'2023-10-31') bar)

CROSS JOIN
unnest (bar) as t(report_date)
)
)
,params_date(period_grp,period,start_date,end_date,days) as 
(SELECT 
        '1. Daily'
        ,CAST(report_date as varchar)
        ,CAST(report_date as date)
        ,CAST(report_date as date)
        ,CAST(1 as double)

from raw_date
UNION
SELECT 
    '3. Monthly'
    ,cast(date_trunc('month',report_date) as varchar) 
                                                                                                                    
    ,date_trunc('month',report_date)
    ,max(report_date)
    ,date_diff('day',cast(date_trunc('month',report_date) as date),max(report_date)) + 1

from raw_date

group by 1,2,3
)
,metrics as 
(SELECT 
    p.period,
    p.period_grp,
    p.start_date,
    p.end_date,
    p.days,
    shipper_id,
    city_name as city_group,
            
                                                                                     
                                        
    ARRAY_AGG(DISTINCT case when total_order > 0 then dot.report_date else null end) as check_period,
    CARDINALITY(FILTER(ARRAY_AGG(DISTINCT case when total_order > 0 then dot.report_date else null end),x-> x is not null)) AS cnt_period


FROM (
SELECT  
        report_date,
        shipper_id,
        city_name,
        total_order

from dev_vnfdbi_opsndrivers.driver_ops_driver_performance_tab 
) dot 

INNER JOIN params_date p ON report_date BETWEEN p.start_date and p.end_date

GROUP BY 1,2,3,4,5,6,7
)
,key_value as 
(select
        period,
        period_grp,
        start_date,
        end_date,
        days,
        case 
        when period_grp = '1. Daily' and cnt_period > 0 then 'dad' 
        when period_grp = '3. Monthly' and cnt_period > 0 then 'mad'
        else 'non_active'
        end as key_,
        shipper_id,
        city_group

from metrics
)
,delivery_performance as 
(select 
        month_,
        city_group,
        sum(total_order)/cast(sum(online_hour) as double) as blended_ado,
        sum(online_hour)/cast(count(shipper_id) as double) as avg_supply_hour
from
(select 
        date_trunc('month',report_date) as month_,
        report_date,
        shipper_id,
        total_order,
        online_hour,
        city_name as city_group,
                    
                                                                                     
                                        
        total_order*1.0000/online_hour as blended_ado_per_sh


from driver_ops_driver_performance_tab
where total_order > 0
                                      
                                      
)
group by 1,2
)
select 
        v1.*,
        dp.blended_ado,
        avg_supply_hour,
                     
        avg(case when v2.dad_value is not null then v2.dad_value else null end) as dad_value,
        count(distinct v2.period) as days
               

from
(select 
        v1.period,
        v1.period_grp,
        v1.start_date - interval '2' month as start_date,
        v1.end_date,
        v1.city_group,        
        count(distinct case when v1.key_ = 'mad' then v1.shipper_id else null end) as mad_value

from key_value v1
where v1.period_grp = '3. Monthly'
and v1.key_ = 'mad'
group by 1,2,3,4,5
) v1 
left join 
(select 
        cast(v1.period as date) as period,
        v1.period_grp,
        v1.city_group,
        count(distinct shipper_id) as dad_value

from key_value v1  
where period_grp = '1. Daily'
and key_ = 'dad'
group by 1,2,3
) v2 on v1.city_group = v2.city_group
        and v2.period between cast(v1.start_date as date) and cast(v1.end_date as date)

left join delivery_performance dp 
        on cast(dp.month_ as varchar) = v1.period
        and dp.city_group = v1.city_group

where cast(v1.period as date) = date'2023-09-01'
                                   
group by 1,2,3,4,5,6,7,8