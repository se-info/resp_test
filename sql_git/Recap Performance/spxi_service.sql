with raw_date as 
(SELECT

      DATE(report_date) AS report_date
FROM
    (
(
SELECT sequence(date'2021-12-01',current_date - interval '1' day) bar)

CROSS JOIN
    unnest (bar) as t(report_date)
)
)
,params(period_group,period,start_date,end_date) as 
(
SELECT 
        '1. Daily'
        ,CAST(report_date as varchar)
        ,report_date
        ,report_date

from raw_date

UNION ALL 
SELECT 
        '2. Weekly'
        ,'W- ' || CAST(YEAR(date_trunc('week',report_date))*100 + WEEK(date_trunc('week',report_date)) as varchar)
        ,date_trunc('week',report_date) 
        ,max(report_date)

from raw_date

group by 1,2,3

UNION 

SELECT 
        '3. Monthly'
        ,'M- ' || CAST(YEAR(date_trunc('month',report_date))*100 + MONTH(date_trunc('month',report_date)) as varchar)
        ,date_trunc('month',report_date)
        ,max(report_date)

from raw_date

group by 1,2,3
)


select 
    p.period_group,
    p.period,
    cast((date_diff('day',p.start_date,p.end_date) +1) as double),
    -- concat_ws('_', p.period, 'SPXI') key,
    -- city_name,
    1.00000*count(distinct id)/cast((date_diff('day',p.start_date,p.end_date) +1) as double) total_orders,
    1.00000*COUNT(DISTINCT case when order_status IN ('Delivered', 'Returned') then id else null end)/cast((date_diff('day',p.start_date,p.end_date) +1) as double) total_net_order,
    1.00000*COUNT(DISTINCT case when order_status IN ('Delivered') then id else null end)/count(distinct id) g2n,
    1.00000*COUNT(DISTINCT case when order_status IN ('Cancelled') then id else null end)/count(distinct id) cancel_rate,
    1.00000*COUNT(DISTINCT case when order_status IN ('Assigning Timeout') then id else null end)/count(distinct id) to_rate,
    1.00000*COUNT(DISTINCT case when order_status IN ('Pickup Failed') then id else null end)/count(distinct id) pu_rate

from (SELECT * FROM vnfdbi_opsndrivers.ns_performance_tab ) a
inner join params p ON date(a.grass_date) BETWEEN p.start_date and p.end_date
where p.period_group = '3. Monthly'
group by 1,2,3,date_diff('day',p.start_date,p.end_date)
