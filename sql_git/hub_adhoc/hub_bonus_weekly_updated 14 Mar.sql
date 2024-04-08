WITH params(report_date,uid) AS 
(VALUES 
(date'2023-03-07',18918351)
)
,base AS
(SELECT
       YEAR(a.date_)*100 + WEEK(a.date_) AS created_year_week 
      ,a.uid as shipper_id
      ,a.shipper_name 
      ,a.city_name
      ,a.hub_type  
      ,SUM(a.registered_) AS total_reg
      ,COUNT(DISTINCT CASE WHEN total_order > 0 then (a.date_,a.slot_id) ELSE NULL END) AS working_day  
      ,SUM(final_kpi) AS total_kpi
      ,SUM(CASE WHEN date_format(a.date_,'%a') = 'Sun' THEN a.kpi ELSE NULL END) AS kpi_sun
      ,SUM(case when date_format(a.date_,'%a') = 'Sun' then a.total_order else null end) as is_work_sun        
      ,ROW_NUMBER()OVER(PARTITION BY a.uid order by SUM(a.registered_) DESC) AS rank  
FROM 
(SELECT a.*
       ,CASE 
             WHEN hub_type = '3 hour shift' then 0
             WHEN p.uid is not null then 1  
             ELSE a.kpi END AS final_kpi    
FROM dev_vnfdbi_opsndrivers.phong_hub_driver_metrics a 

LEFT JOIN params p 
    on p.uid = a.uid 
    and p.report_date = a.date_ 

)a


WHERE 1 = 1 
                                                          
AND date_ between date_trunc('week',current_date)- interval '7' day and date_trunc('week',current_date) - interval '1' day
GROUP BY 1,2,3,4,5,a.uid
)
,summary as 
(SELECT 
       base.created_year_week 
      ,base.shipper_id
      ,base.shipper_name
      ,base.city_name
      ,base.hub_type
        ,case          
              when created_year_week >= 202222 and created_year_week <= 202237
                                              and is_work_sun > 0 and kpi_sun = 1
                                              and total_kpi >= 4 
                                              and total_reg = working_day then 50000
              when created_year_week > 202237 and hub_type in ('8 hour shift', '10 hour shift') and  kpi_sun = 1 then 50000
              when created_year_week > 202237 and hub_type = '5 hour shift' and  kpi_sun = 1 then 30000
              when created_year_week > 202237 and hub_type = '3 hour shift' and  kpi_sun = 1 then 20000    
                                              else 0 end as sunday_bonus      
      ,CASE 
              WHEN base.total_kpi >= 6 and base.city_name in ('HCM City','Ha Noi City')
              and  base.working_day = base.total_reg 
                                                             
              then 300000
              WHEN base.total_kpi = 5 and base.city_name in ('HCM City','Ha Noi City')
              and  base.working_day = base.total_reg 
                                                            
              then 150000 
              WHEN base.total_kpi = 4 and base.city_name in ('HCM City','Ha Noi City')
              and  base.working_day = base.total_reg 
                                                             
              then 100000
              WHEN base.total_kpi >= 6 and base.city_name = 'Hai Phong City' then 200000
              WHEN base.total_kpi >= 4 and base.city_name = 'Hai Phong City' then 80000
              else 0 end as weekly_bonus_value
        ,base.working_day
        ,base.total_reg
        ,base.total_kpi              
        ,coalesce(act.value,0) as weekly_bonus_paid
        ,coalesce(act_s.value,0) as sunday_bonus_paid                   

FROM base 

LEFT JOIN 
        (select 
                  user_id
                 ,note
                 ,sum(balance/cast(100 as double)) as value    



        from shopeefood.foody_accountant_db__partner_transaction_tab__reg_daily_s0_live

        where 1 = 1  
                                 
                                                                         
        and note like '%HUB_MODEL_Thuong tai xe guong mau tuan 27/02 - 05/03%'
        group by 1,2
        ) act on act.user_id = base.shipper_id 
LEFT JOIN 
        (select 
                  user_id
                 ,note
                 ,sum(balance/cast(100 as double)) as value    



        from shopeefood.foody_accountant_db__partner_transaction_tab__reg_daily_s0_live

        where 1 = 1  
                             
                                                                         
        and note like '%HUB_MODEL_Thuong tai xe guong mau chu nhat tuan 27/02 - 05/03%'
        group by 1,2
        ) act_s on act_s.user_id = base.shipper_id 

WHERE rank = 1 
)

SELECT 
        *
                                                                 

FROM summary

WHERE 1 = 1
;

SELECT
        created_year_week
       ,shipper_id
       ,shipper_name
       ,city_name
       ,MAX_BY(hub_type,sunday_bonus) as hub_type 
       ,MAX(sunday_bonus) AS sunday_bonus
FROM
(SELECT
       (YEAR(a.date_)*100 + WEEK(a.date_)) AS created_year_week 
      ,a.uid as shipper_id
      ,a.shipper_name 
      ,a.city_name
      ,a.hub_type  
        ,case          
              when (YEAR(a.date_)*100 + WEEK(a.date_)) > 202237 and hub_type in ('8 hour shift', '10 hour shift') and  kpi = 1 then 50000
              when (YEAR(a.date_)*100 + WEEK(a.date_)) > 202237 and hub_type = '5 hour shift' and  kpi = 1 then 30000
              when (YEAR(a.date_)*100 + WEEK(a.date_)) > 202237 and hub_type = '3 hour shift' and  kpi = 1 then 20000    
                                              else 0 end as sunday_bonus    

FROM 
(select *
from dev_vnfdbi_opsndrivers.phong_hub_driver_metrics a 
) a 

WHERE 1 = 1 

-- AND date_ between date_trunc('week',current_date)- interval '7' day and date_trunc('week',current_date) - interval '1' day
AND a.date_ = date'2023-03-12'
)
WHERE sunday_bonus > 0
GROUP BY 1,2,3,4                                              

