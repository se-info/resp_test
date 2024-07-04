with db as 
(
--daily

SELECT '1. NowFood' as section 
      ,food_service as service 
      ,'1. Total Delivered' as metrics 
      ,'Daily' as period_group
      ,cast(created_date as varchar) as period
      ,'VN' as city_group
      ,'' as tier
      ,sum(total_order_delivered) kpi 
      ,sum(total_order_delivered) total
      
FROM  foody_bi_anlys.snp_foody_order_performance_tab     

WHERE 1=1 
and source = 'NowFood'
and created_date between date(current_date) - interval '35' day and date(current_date) - interval '1' day
and food_service <> 'Food - Others'
GROUP BY 1,2,3,4,5,6,7

UNION

SELECT '1. NowFood' as section 
      ,a.food_service as service 
      ,case when is_group_order = 1 then 'a. Group Order' 
            when is_group_order = 0 and is_stack_order = 1 then 'b. Stack Order'
            else 'c. Single Order' end as metrics 
      ,'Daily' as period_group
      ,cast(a.created_date as varchar) as period
      ,'VN' as city_group
      ,'' as tier
      ,sum(a.total_order_delivered) kpi 
      ,b.total
      
FROM  foody_bi_anlys.snp_foody_order_performance_tab a     

LEFT JOIN 
         (
         SELECT created_date as period
               ,food_service as service 
               ,sum(total_order_delivered) total
         
         FROM foody_bi_anlys.snp_foody_order_performance_tab   
         WHERE 1=1 
         and source = 'NowFood'
         and food_service <> 'Food - Others'
         group by 1,2
         )b on a.created_date = b.period and a.food_service = b.service
         
WHERE 1=1 
and source = 'NowFood'
and created_date between date(current_date) - interval '35' day and date(current_date) - interval '1' day
and food_service <> 'Food - Others'
GROUP BY 1,2,3,4,5,6,7,9

UNION

SELECT '2. NowShip' as section 
      ,'NowShip' as service 
      ,'1. Total Delivered' as metrics 
      ,'Daily' as period_group
      ,cast(created_date as varchar) as period
      ,'VN' as city_group
      ,'' as tier
      ,count(distinct case when order_status = 'Delivered' then uid else null end) kpi 
      ,count(distinct case when order_status = 'Delivered' then uid else null end) total 
      
FROM  foody_bi_anlys.snp_foody_nowship_performance_tab     

WHERE 1=1 
and created_date between date(current_date) - interval '35' day and date(current_date) - interval '1' day

GROUP BY 1,2,3,4,5,6,7

UNION

SELECT '2. NowShip' as section 
      ,'NowShip' as service 
      ,case when is_group_order = 1 then 'a. Group Order' 
            when is_group_order = 0 and is_stacked = 1 then 'b. Stack Order'
            else 'c. Single Order' end as metrics 
      ,'Daily' as period_group
      ,cast(a.created_date as varchar) as period
      ,'VN' as city_group
      ,'' as tier
      ,count(distinct case when a.order_status = 'Delivered' then uid else null end) kpi 
      ,b.total
      
FROM  foody_bi_anlys.snp_foody_nowship_performance_tab a    

LEFT JOIN 
         (
         SELECT created_date as period
               ,count(distinct case when order_status = 'Delivered' then uid else null end) total
         
         FROM foody_bi_anlys.snp_foody_nowship_performance_tab   
         WHERE 1=1 
         group by 1
         )b on a.created_date = b.period 
         
WHERE 1=1 
and created_date between date(current_date) - interval '35' day and date(current_date) - interval '1' day

GROUP BY 1,2,3,4,5,6,7,9

UNION

---weekly

SELECT '1. NowFood' as section 
      ,food_service as service 
      ,'1. Total Delivered' as metrics 
      ,'Weekly' as period_group
      ,cast(created_year_week as varchar) as period
      ,'VN' as city_group
      ,'' as tier
      ,sum(total_order_delivered)*1.00/7 kpi 
      ,sum(total_order_delivered)*1.00/7 total
      
FROM  foody_bi_anlys.snp_foody_order_performance_tab     

         
WHERE 1=1 
and source = 'NowFood'
and created_year_week > YEAR(date(current_date) - interval '35' day)*100 + WEEK(date(current_date) - interval '35' day)
and created_year_week < YEAR(date(current_date))*100 + WEEK(date(current_date))
and food_service <> 'Food - Others'
GROUP BY 1,2,3,4,5,6,7

UNION 

SELECT '1. NowFood' as section 
      ,a.food_service as service 
      ,case when is_group_order = 1 then 'a. Group Order' 
            when is_group_order = 0 and is_stack_order = 1 then 'b. Stack Order'
            else 'c. Single Order' end as metrics
      ,'Weekly' as period_group
      ,cast(a.created_year_week as varchar) as period
      ,'VN' as city_group
      ,'' as tier
      ,sum(a.total_order_delivered)*1.00/7 kpi 
      ,b.total
      
FROM  foody_bi_anlys.snp_foody_order_performance_tab a     

LEFT JOIN 
         (
         SELECT created_year_week as period
               ,food_service as service 
               ,sum(total_order_delivered)*1.00/7 total
         
         FROM foody_bi_anlys.snp_foody_order_performance_tab   
         WHERE 1=1 
         and source = 'NowFood'
         and food_service <> 'Food - Others'
         group by 1,2
         )b on a.created_year_week = b.period and a.food_service = b.service
         
WHERE 1=1 
and source = 'NowFood'
and created_year_week > YEAR(date(current_date) - interval '35' day)*100 + WEEK(date(current_date) - interval '35' day)
and created_year_week < YEAR(date(current_date))*100 + WEEK(date(current_date))
and food_service <> 'Food - Others'
GROUP BY 1,2,3,4,5,6,7,9

UNION

SELECT '2. NowShip' as section 
      ,'NowShip' as service
      ,'1. Total Delivered' as metrics 
      ,'Weekly' as period_group
      ,cast(created_year_week as varchar) as period
      ,'VN' as city_group
      ,'' as tier
      ,count(distinct case when order_status = 'Delivered' then uid else null end)*1.00/7 kpi 
      ,count(distinct case when order_status = 'Delivered' then uid else null end)*1.00/7 total
      
FROM  foody_bi_anlys.snp_foody_nowship_performance_tab     

WHERE 1=1 
and created_year_week > YEAR(date(current_date) - interval '35' day)*100 + WEEK(date(current_date) - interval '35' day)
and created_year_week < YEAR(date(current_date))*100 + WEEK(date(current_date))

GROUP BY 1,2,3,4,5,6,7

UNION

SELECT '2. NowShip' as section 
      ,'NowShip' as service
      ,case when is_group_order = 1 then 'a. Group Order' 
            when is_group_order = 0 and is_stacked = 1 then 'b. Stack Order'
            else 'c. Single Order' end as metrics
      ,'Weekly' as period_group
      ,cast(created_year_week as varchar) as period
      ,'VN' as city_group
      ,'' as tier
      ,count(distinct case when order_status = 'Delivered' then uid else null end)*1.00/7 kpi 
      ,b.total
      
FROM  foody_bi_anlys.snp_foody_nowship_performance_tab a    

LEFT JOIN 
         (
         SELECT created_year_week as period
               ,count(distinct case when order_status = 'Delivered' then uid else null end)*1.00/7 total
         
         FROM foody_bi_anlys.snp_foody_nowship_performance_tab   
         WHERE 1=1 
         group by 1
         )b on a.created_year_week = b.period 
         
WHERE 1=1 
and created_year_week > YEAR(date(current_date) - interval '35' day)*100 + WEEK(date(current_date) - interval '35' day)
and created_year_week < YEAR(date(current_date))*100 + WEEK(date(current_date))

GROUP BY 1,2,3,4,5,6,7,9

UNION

--by city
--daily

SELECT '1. NowFood' as section 
      ,food_service as service 
      ,'1. Total Delivered' as metrics 
      ,'Daily' as period_group
      ,cast(created_date as varchar) as period
      ,case when city_group in ('HCM','HN','DN') then city_group when city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then city_name else 'OTH' end as city_group
      ,case when city_group in ('HCM','HN','DN') then 'Tier 1' when city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then 'Tier 2' else 'Tier 3' end as tier
      ,sum(total_order_delivered) kpi 
      ,sum(total_order_delivered) total
      
FROM  foody_bi_anlys.snp_foody_order_performance_tab     

WHERE 1=1 
and source = 'NowFood'
and created_date between date(current_date) - interval '35' day and date(current_date) - interval '1' day
and food_service <> 'Food - Others'
GROUP BY 1,2,3,4,5,6,7

UNION

SELECT '1. NowFood' as section 
      ,a.food_service as service 
      ,case when is_group_order = 1 then 'a. Group Order' 
            when is_group_order = 0 and is_stack_order = 1 then 'b. Stack Order'
            else 'c. Single Order' end as metrics 
      ,'Daily' as period_group
      ,cast(a.created_date as varchar) as period
      ,case when a.city_group in ('HCM','HN','DN') then a.city_group when a.city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then city_name else 'OTH' end as city_group
      ,case when a.city_group in ('HCM','HN','DN') then 'Tier 1' when a.city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then 'Tier 2' else 'Tier 3' end as tier
      ,sum(a.total_order_delivered) kpi 
      ,b.total
      
FROM  foody_bi_anlys.snp_foody_order_performance_tab a     

LEFT JOIN 
         (
         SELECT created_date as period
               ,food_service as service 
               ,case when city_group in ('HCM','HN','DN') then city_group when city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then city_name else 'OTH' end as city_group
               ,case when city_group in ('HCM','HN','DN') then 'Tier 1' when city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then 'Tier 2' else 'Tier 3' end as tier
               ,sum(total_order_delivered) total
         
         FROM foody_bi_anlys.snp_foody_order_performance_tab   
         WHERE 1=1 
         and source = 'NowFood'
         and food_service <> 'Food - Others'
         group by 1,2,3,4
         )b on a.created_date = b.period and a.food_service = b.service and case when a.city_group in ('HCM','HN','DN') then a.city_group when city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then city_name else 'OTH' end = b.city_group
                                                                        and case when a.city_group in ('HCM','HN','DN') then 'Tier 1' when a.city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then 'Tier 2' else 'Tier 3' end = b.tier
                                                                        
WHERE 1=1 
and source = 'NowFood'
and created_date between date(current_date) - interval '35' day and date(current_date) - interval '1' day
and food_service <> 'Food - Others'
GROUP BY 1,2,3,4,5,6,7,9

UNION

SELECT '2. NowShip' as section 
      ,'NowShip' as service 
      ,'1. Total Delivered' as metrics 
      ,'Daily' as period_group
      ,cast(created_date as varchar) as period
      ,case when city_group in ('HCM','HN','DN') then city_group when city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then city_name else 'OTH' end as city_group
      ,case when city_group in ('HCM','HN','DN') then 'Tier 1' when city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then 'Tier 2' else 'Tier 3' end as tier
      ,count(distinct case when order_status = 'Delivered' then uid else null end) kpi 
      ,count(distinct case when order_status = 'Delivered' then uid else null end) total 
      
FROM  foody_bi_anlys.snp_foody_nowship_performance_tab     

WHERE 1=1 
and created_date between date(current_date) - interval '35' day and date(current_date) - interval '1' day

GROUP BY 1,2,3,4,5,6,7

UNION

SELECT '2. NowShip' as section 
      ,'NowShip' as service 
      ,case when is_group_order = 1 then 'a. Group Order' 
            when is_group_order = 0 and is_stacked = 1 then 'b. Stack Order'
            else 'c. Single Order' end as metrics 
      ,'Daily' as period_group
      ,cast(a.created_date as varchar) as period
      ,case when a.city_group in ('HCM','HN','DN') then a.city_group when a.city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then a.city_name else 'OTH' end as city_group
      ,case when a.city_group in ('HCM','HN','DN') then 'Tier 1' when a.city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then 'Tier 2' else 'Tier 3' end as tier
      ,count(distinct case when a.order_status = 'Delivered' then uid else null end) kpi 
      ,b.total
      
FROM  foody_bi_anlys.snp_foody_nowship_performance_tab a    

LEFT JOIN 
         (
         SELECT created_date as period
               ,case when city_group in ('HCM','HN','DN') then city_group when city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then city_name else 'OTH' end as city_group
               ,case when city_group in ('HCM','HN','DN') then 'Tier 1' when city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then 'Tier 2' else 'Tier 3' end as tier
               ,count(distinct case when order_status = 'Delivered' then uid else null end) total
         
         FROM foody_bi_anlys.snp_foody_nowship_performance_tab   
         WHERE 1=1 
         group by 1,2,3
         )b on a.created_date = b.period and case when a.city_group in ('HCM','HN','DN') then a.city_group when a.city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then a.city_name else 'OTH' end = b.city_group
                                         and case when a.city_group in ('HCM','HN','DN') then 'Tier 1' when a.city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then 'Tier 2' else 'Tier 3' end = b.tier
         
WHERE 1=1 
and created_date between date(current_date) - interval '35' day and date(current_date) - interval '1' day

GROUP BY 1,2,3,4,5,6,7,9

UNION

---weekly

SELECT '1. NowFood' as section 
      ,food_service as service 
      ,'1. Total Delivered' as metrics 
      ,'Weekly' as period_group
      ,cast(created_year_week as varchar) as period
      ,case when city_group in ('HCM','HN','DN') then city_group when city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then city_name else 'OTH' end as city_group
      ,case when city_group in ('HCM','HN','DN') then 'Tier 1' when city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then 'Tier 2' else 'Tier 3' end as tier
      ,sum(total_order_delivered)*1.00/7 kpi 
      ,sum(total_order_delivered)*1.00/7 total
      
FROM  foody_bi_anlys.snp_foody_order_performance_tab     

         
WHERE 1=1 
and source = 'NowFood'
and created_year_week > YEAR(date(current_date) - interval '35' day)*100 + WEEK(date(current_date) - interval '35' day)
and created_year_week < YEAR(date(current_date))*100 + WEEK(date(current_date))
and food_service <> 'Food - Others'
GROUP BY 1,2,3,4,5,6,7

UNION 

SELECT '1. NowFood' as section 
      ,a.food_service as service 
      ,case when is_group_order = 1 then 'a. Group Order' 
            when is_group_order = 0 and is_stack_order = 1 then 'b. Stack Order'
            else 'c. Single Order' end as metrics
      ,'Weekly' as period_group
      ,cast(a.created_year_week as varchar) as period
      ,case when a.city_group in ('HCM','HN','DN') then a.city_group when a.city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then a.city_name else 'OTH' end as city_group
      ,case when a.city_group in ('HCM','HN','DN') then 'Tier 1' when a.city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then 'Tier 2' else 'Tier 3' end as tier
      ,sum(a.total_order_delivered)*1.00/7 kpi 
      ,b.total
      
FROM  foody_bi_anlys.snp_foody_order_performance_tab a     

LEFT JOIN 
         (
         SELECT created_year_week as period
               ,food_service as service 
               ,case when city_group in ('HCM','HN','DN') then city_group when city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then city_name else 'OTH' end as city_group
               ,case when city_group in ('HCM','HN','DN') then 'Tier 1' when city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then 'Tier 2' else 'Tier 3' end as tier
               ,sum(total_order_delivered)*1.00/7 total
         
         FROM foody_bi_anlys.snp_foody_order_performance_tab   
         WHERE 1=1 
         and source = 'NowFood'
         and food_service <> 'Food - Others'
         group by 1,2,3,4
         )b on a.created_year_week = b.period and a.food_service = b.service and case when a.city_group in ('HCM','HN','DN') then a.city_group when city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then city_name else 'OTH' end = b.city_group
                                                                             and case when a.city_group in ('HCM','HN','DN') then 'Tier 1' when a.city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then 'Tier 2' else 'Tier 3' end = b.tier
         
WHERE 1=1 
and source = 'NowFood'
and created_year_week > YEAR(date(current_date) - interval '35' day)*100 + WEEK(date(current_date) - interval '35' day)
and created_year_week < YEAR(date(current_date))*100 + WEEK(date(current_date))
and food_service <> 'Food - Others'
GROUP BY 1,2,3,4,5,6,7,9

UNION

SELECT '2. NowShip' as section 
      ,'NowShip' as service
      ,'1. Total Delivered' as metrics 
      ,'Weekly' as period_group
      ,cast(created_year_week as varchar) as period
      ,case when city_group in ('HCM','HN','DN') then city_group when city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then city_name else 'OTH' end as city_group
      ,case when city_group in ('HCM','HN','DN') then 'Tier 1' when city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then 'Tier 2' else 'Tier 3' end as tier
      ,count(distinct case when order_status = 'Delivered' then uid else null end)*1.00/7 kpi 
      ,count(distinct case when order_status = 'Delivered' then uid else null end)*1.00/7 total
      
FROM  foody_bi_anlys.snp_foody_nowship_performance_tab     

WHERE 1=1 
and created_year_week > YEAR(date(current_date) - interval '35' day)*100 + WEEK(date(current_date) - interval '35' day)
and created_year_week < YEAR(date(current_date))*100 + WEEK(date(current_date))

GROUP BY 1,2,3,4,5,6,7

UNION

SELECT '2. NowShip' as section 
      ,'NowShip' as service
      ,case when is_group_order = 1 then 'a. Group Order' 
            when is_group_order = 0 and is_stacked = 1 then 'b. Stack Order'
            else 'c. Single Order' end as metrics
      ,'Weekly' as period_group
      ,cast(created_year_week as varchar) as period
      ,case when a.city_group in ('HCM','HN','DN') then a.city_group when city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then city_name else 'OTH' end as city_group
      ,case when a.city_group in ('HCM','HN','DN') then 'Tier 1' when city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then 'Tier 2' else 'Tier 3' end as tier
      ,count(distinct case when order_status = 'Delivered' then uid else null end)*1.00/7 kpi 
      ,b.total
      
FROM  foody_bi_anlys.snp_foody_nowship_performance_tab a    

LEFT JOIN 
         (
         SELECT created_year_week as period
               ,case when city_group in ('HCM','HN','DN') then city_group when city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then city_name else 'OTH' end as city_group
               ,case when city_group in ('HCM','HN','DN') then 'Tier 1' when city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then 'Tier 2' else 'Tier 3' end as tier
               ,count(distinct case when order_status = 'Delivered' then uid else null end)*1.00/7 total
         
         FROM foody_bi_anlys.snp_foody_nowship_performance_tab   
         WHERE 1=1 
         group by 1,2,3
         )b on a.created_year_week = b.period and case when a.city_group in ('HCM','HN','DN') then a.city_group when a.city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then a.city_name else 'OTH' end = b.city_group
                                              and case when a.city_group in ('HCM','HN','DN') then 'Tier 1' when a.city_name in ('Hai Phong City','Hue City','Can Tho City','Dong Nai','Vung Tau','Binh Duong') then 'Tier 2' else 'Tier 3' end = b.tier
         
WHERE 1=1 
and created_year_week > YEAR(date(current_date) - interval '35' day)*100 + WEEK(date(current_date) - interval '35' day)
and created_year_week < YEAR(date(current_date))*100 + WEEK(date(current_date))

GROUP BY 1,2,3,4,5,6,7,9

)

SELECT * 

FROM db

UNION 

SELECT d1.section 
      ,d1.service 
      ,d1.metrics
      ,'WoW' as period_group
      ,'WoW' as period 
      ,d1.city_group
      ,d1.tier
      ,try((d1.kpi - d2.kpi)*1.0000/ d2.kpi) as kpi
      ,try((d1.kpi - d2.kpi)*(d1.total*d2.total)*1.000000/(d2.kpi*(d1.kpi*d2.total-d2.kpi*d1.total))) as total                                                                                                                                                                                                                                            

FROM 
        (
        SELECT * 
        
        FROM db
        
        WHERE period_group = 'Weekly'
        and cast(period as bigint) = (YEAR(date(current_date))*100 + WEEK(date(current_date)) - 1)
        )d1 
        
LEFT JOIN 
        (
        SELECT * 
        
        FROM db
        
        WHERE period_group = 'Weekly'
        and cast(period as bigint) = (YEAR(date(current_date))*100 + WEEK(date(current_date)) - 2)
        )d2 on concat(d1.section, d1.service ,d1.metrics, d1.period_group,d1.city_group,d1.tier) = concat(d2.section, d2.service ,d2.metrics, d2.period_group,d2.city_group,d2.tier) and cast(d1.period as bigint) =  cast(d2.period as bigint) + 1