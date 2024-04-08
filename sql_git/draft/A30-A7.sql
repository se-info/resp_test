with a as 
        (select 1 as mapping
               ,cast(grass_date as date) as report_date 
         from shopeefood.foody_mart__fact_gross_order_join_detail 
         where cast(grass_date as date) between date(current_date) - interval '1' day and date(current_date)- interval '1' day 
         group by 1,2
        )

, b as 
        (select 1 as mapping
               ,report_date
               ,shipper_id
               ,city_group
               ,sum(cnt_total_order_delivered) total_del
               
         from  vnfdbi_opsndrivers.snp_foody_shipper_daily_report
         
         where 1=1
         and report_date between date(current_date) - interval '65' day and date(current_date)- interval '1' day
         group by 1,2,3,4
        )

SELECT  base1.report_date
       ,base1.city_group
       ,base1.tier
       ,count(distinct case when coalesce(base1.a30_del,0) > 0 then base1.shipper_id else null end) a30_shipper
       ,count(distinct case when coalesce(base1.a7_del,0) > 0 then base1.shipper_id else null end) a7_shipper


FROM
        (
        SELECT base.report_date
             ,base.shipper_id
             ,base.city_group
             ,case when base.city_group in ('HCM','HN') then coalesce(bonus.current_driver_tier,'No Tier') else 'No Tier' end as tier
             ,case when si.begin_work_date > 0 then date_diff('second',date_trunc('day',from_unixtime(si.begin_work_date)), date_trunc('day',cast(date(base.report_date) as TIMESTAMP)))/(3600*24) else -1 end as seniority
             --- A7 or A30 
             ,sum(case when base.date_ between date(base.report_date) - interval '29' day and date(base.report_date) then base.total_del else 0 end) a30_del
             ,sum(case when base.date_ between date(base.report_date) - interval '6' day and date(base.report_date) then base.total_del else 0 end) a7_del
        
        FROM
                (
                Select s1.report_date
                      ,s2.report_date as date_
                      ,s2.shipper_id
                      ,s2.city_group
                      ,coalesce(s2.total_del,0) total_del
                
                FROM (select * from a) s1 
                
                LEFT JOIN (select * from b) s2 on s1.mapping = s2.mapping and s2.report_date <= s1.report_date
                
                WHERE 1 = 1 
                and city_group is not null
                )base 
                
        LEFT JOIN 
                (SELECT cast(from_unixtime(bonus.report_date - 60*60) as date) as report_date
                        ,bonus.uid as shipper_id
                        ,driver_type.shipper_type_id
    
                        
                        ,case when driver_type.shipper_type_id = 12 then 'Hub' 
                            when bonus.tier in (1,6,11) then 'T1' -- as current_driver_tier
                            when bonus.tier in (2,7,12) then 'T2'
                            when bonus.tier in (3,8,13) then 'T3'
                            when bonus.tier in (4,9,14) then 'T4'
                            when bonus.tier in (5,10,15) then 'T5'
                            else null end as current_driver_tier

                
                FROM shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus
                left join 
                            (SELECT base.shipper_id
                                ,base.report_date
                                ,base.shipper_type_id
                            
                                From
                                (SELECT shipper_id
                                    ,city_name
                                    ,case when grass_date = 'current' then date(current_date)
                                        else cast(grass_date as date) end as report_date
                                    ,shipper_type_id    
                                
                                    from shopeefood.foody_mart__profile_shipper_master
                                    
                                    where 1=1
                                   -- and (grass_date = 'current' OR cast(grass_date as date) >= date(current_date) - interval '60' day)
                                    and shipper_type_id <> 3
                                    and shipper_status_code = 1
                                    and shipper_type_id = 12 -- hub driver
                                )base 
                                GROUP BY 1,2,3
                            
                            )driver_type on driver_type.shipper_id = bonus.uid and driver_type.report_date =  cast(from_unixtime(bonus.report_date - 60*60) as date)
                
                )bonus on base.report_date = bonus.report_date and base.shipper_id = bonus.shipper_id   
        
        LEFT JOIN shopeefood.foody_internal_db__shipper_info_personal_tab__reg_daily_s2_live si on si.uid = base.shipper_id
        
        GROUP BY 1,2,3,4,5
        )base1
where city_group is not null
GROUP BY 1,2,3