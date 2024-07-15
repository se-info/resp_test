WITH raw_date AS
(SELECT

      DATE(report_date) AS report_date,
      1 AS mapping
FROM
    (
(
SELECT sequence(date_trunc('month',current_date) - interval '2' month,current_date - interval '1' day) bar
)

CROSS JOIN
    unnest (bar) as t(report_date)
)
)
,params_date(period_group,period_,start_date,end_date,days) as 
(
SELECT 
        '1. Daily'
        ,CAST(report_date as varchar)
        ,report_date
        ,report_date
        ,1

from raw_date

UNION ALL 
SELECT 
        '2. Weekly'
        ,'W- ' || CAST(YEAR(date_trunc('week',report_date))*100 + WEEK(date_trunc('week',report_date)) as varchar)
        ,date_trunc('week',report_date) 
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
,f as 
(select 
        r.customer_id,
        1 as mapping,
        array_agg(distinct created_date) as agg_created_c2c_date, 
        min(distinct created_date) as first_c2c_date,
        map_agg(created_date,total_order) map_value 

from 
(select 
        date(from_unixtime(create_time - 3600)) as created_date,
        customer_id,
        count(distinct code) as total_order
from shopeefood.foody_express_db__booking_tab__reg_daily_s0_live
group by 1,2 
)r 

group by 1
)
,metrics as 
(select 
        rd.report_date,
        date_trunc('month',rd.report_date) as first_month_start ,
        date_trunc('month',rd.report_date) + interval '1' month - interval '1' day as first_month_end,
        date_trunc('month',rd.report_date) - interval '1' month second_month_start,
        date_trunc('month',rd.report_date) - interval '1' day second_month_end,
        f.*

from f

left join raw_date rd on rd.mapping = f.mapping
)
,summary as 
(select 
        report_date,
        customer_id,
        cardinality(filter(agg_created_c2c_date, x -> x between first_month_start and first_month_end)) as t1,
        cardinality(filter(agg_created_c2c_date, x -> x between second_month_start and second_month_end)) as t2,
        if(cardinality(filter(agg_created_c2c_date, x -> x = report_date)) > 0 ,1, 0) is_a1,
        if(cardinality(filter(agg_created_c2c_date, x -> x between report_date - interval '29' day and report_date)) > 0 ,1, 0) is_a30,
        if(cardinality(filter(agg_created_c2c_date, x -> x between report_date - interval '59' day and report_date)) > 0 ,1, 0) is_a60,
        case
        when first_c2c_date between first_month_start and first_month_end then 'new' 
        when cardinality(filter(agg_created_c2c_date, x -> x between first_month_start and first_month_end)) > 0 
            and cardinality(filter(agg_created_c2c_date, x -> x between second_month_start and second_month_end)) > 0 then 'retain'
        when cardinality(filter(agg_created_c2c_date, x -> x between first_month_start and first_month_end)) > 0 
            and cardinality(filter(agg_created_c2c_date, x -> x between second_month_start and second_month_end)) = 0 then 'reactivated'
        else 'inactive' end as user_segment,
        reduce(
        (map_values(map_filter(map_value,(k, v) -> cast(k as date) between first_month_start and first_month_end)))
        , 0, (s,x) -> s + x, s -> s) as monthly_order

from metrics 
)
select
        p.period_,
        count(distinct case when is_a1 = 1 then (s.customer_id,s.report_date) else null end)*1.0000/count(distinct s.report_date) as a1,
        count(distinct case when is_a30 = 1 then (s.customer_id,s.report_date) else null end)*1.0000/count(distinct s.report_date) as a30,
        count(distinct case when is_a1 = 1 then (s.customer_id,s.report_date) else null end)*1.0000/
             count(distinct case when is_a30 = 1 then (s.customer_id,s.report_date) else null end) as a1_a30,
        count(distinct case when user_segment = 'retain' then (s.customer_id,s.report_date) else null end)*1.0000/count(distinct s.report_date) as retain,
        count(distinct case when user_segment = 'reactivated' then (s.customer_id,s.report_date) else null end)*1.0000/count(distinct s.report_date) as reactivate,
        count(distinct case when user_segment = 'new' then (s.customer_id,s.report_date) else null end)*1.0000/count(distinct s.report_date) as new,
        sum(case when user_segment = 'retain' then monthly_order else null end)*1.0000/count(distinct s.report_date) as monthly_retain,         
        sum(case when user_segment = 'reactivated' then monthly_order else null end)*1.0000/count(distinct s.report_date) as monthly_reactivated,         
        sum(case when user_segment = 'new' then monthly_order else null end)*1.0000/count(distinct s.report_date) as monthly_new,         

        sum(case when user_segment = 'retain' then monthly_order else null end)*1.0000/
                count(distinct case when user_segment = 'retain' then (s.customer_id) else null end) as monthly_retain_per,
        sum(case when user_segment = 'reactivated' then monthly_order else null end)*1.0000/
                count(distinct case when user_segment = 'reactivated' then (s.customer_id) else null end) as monthly_reactivated_per,         
        sum(case when user_segment = 'new' then monthly_order else null end)*1.0000/
                count(distinct case when user_segment = 'new' then (s.customer_id) else null end) as monthly_new_per

from summary s 

inner join params_date p on s.report_date between p.start_date and p.end_date

group by 1




