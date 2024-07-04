SELECT
        created_year_week
       ,shipper_id
       ,shipper_name
       ,MAX_BY(city_name,sunday_bonus) AS city_name 
       ,MAX_BY(hub_type,sunday_bonus) AS hub_type 
       ,MAX(sunday_bonus) AS sunday_bonus
       ,MAX(sunday_bonus_paid) AS sunday_bonus_paid

FROM
(SELECT
       (YEAR(a.date_)*100 + WEEK(a.date_)) AS created_year_week 
      ,a.uid as shipper_id
      ,a.shipper_name 
      ,a.city_name
      ,a.hub_type_original AS hub_type  
        ,case          
              when (YEAR(a.date_)*100 + WEEK(a.date_)) > 202237 and hub_type_original in ('8 hour shift', '10 hour shift') and  kpi_final = 1 then 50000
              when (YEAR(a.date_)*100 + WEEK(a.date_)) > 202237 and hub_type_original = '5 hour shift' and  kpi_final = 1 then 30000
              when (YEAR(a.date_)*100 + WEEK(a.date_)) > 202237 and hub_type_original = '3 hour shift' and  kpi_final = 1 then 20000    
                                              else 0 end as sunday_bonus
      ,COALESCE(act_s.value,0) AS sunday_bonus_paid                                                    

FROM 
(select *
        ,CASE WHEN 
                    a.uid in
                    (40317413,
13248381,
23103237,
23174828,
22564245,
20935540,
20797967,
19498453,
40681210,
20678389,
22414469,
40045862,
21197855,
12072920,
20240524,
15158724,
21630009,
22415015,
21811723,
40357741,
16377464,
20834051,
22411538,
15619461) and date_ = date'2023-03-05' then 1 else kpi end as kpi_final

from dev_vnfdbi_opsndrivers.phong_hub_driver_metrics a 
) a 

LEFT JOIN 
        (select 
                  user_id
                 ,note
                 ,sum(balance/cast(100 as double)) as value    



        from shopeefood.foody_accountant_db__partner_transaction_tab__reg_daily_s0_live

        where 1 = 1  
                             
                                                                         
        and note like '%HUB_MODEL_Thuong tai xe guong mau chu nhat tuan 27/02 - 05/03%'
        group by 1,2
        ) act_s on act_s.user_id = a.uid 
        
WHERE 1 = 1 

                                                                                                                             
AND a.date_ = date'2023-03-05'
)
WHERE sunday_bonus > 0
AND shipper_id in 
(40317413,
13248381,
23103237,
23174828,
22564245,
20935540,
20797967,
19498453,
40681210,
20678389,
22414469,
40045862,
21197855,
12072920,
20240524,
15158724,
21630009,
22415015,
21811723,
40357741,
16377464,
20834051,
22411538,
15619461)
GROUP BY 1,2,3