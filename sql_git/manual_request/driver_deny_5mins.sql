with report_date_time as 
(
SELECT
     DATE(report_date) AS report_date
    ,sequence(cast(t.report_date as timestamp ) ,cast(t.report_date as timestamp) + interval '86400' second, interval '300' second  ) dt_array 
    ,1 as mapping
FROM
    (
(
SELECT sequence(current_date - interval '30' day, current_date - interval '1' day) bar)
CROSS JOIN

    unnest (bar) as t(report_date)
)
)

select 
       t1.mapping
      ,t1.report_date  
      ,t2.dt_array_unnest



from report_date_time t1 

cross join unnest (dt_array) as t2(dt_array_unnest) 

order by 2,1 desc
