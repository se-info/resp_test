with kpi as 
(select uid
       ,json_extract(extra_data,'$.passed_conditions') as passed_conditions
       ,date(from_unixtime(report_date - 3600)) as report_date
       ,case when t.test_1 = 1 then 'Online in shift'
             when t.test_1 = 2 then 'Online peak hour'
             when t.test_1 = 3 then 'Denied'
             when t.test_1 = 4 then 'Ignore'
             when t.test_1 = 6 then 'Auto Accept'
             when t.test_1 = 5 then 'Min service level rate'
             when t.test_1 = 7 then 'Non checkout bad weather'            
            else null end as passed_conditions__v2  
        ,extra_data     




from shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live 

cross join unnest 
(
cast(json_extract(extra_data,'$.passed_conditions') as array<int>)
) t(test_1)

)

,cond(compare_condition) as(
VALUES
(array['Min service level rate','Auto Accept','Ignore','Denied','Online peak hour','Online in shift'])
)

,kpi_v2 as 
(
select 
        uid
       ,report_date
       ,extra_data
       ,array_agg(passed_conditions__v2) as conditions_pass


       from kpi 

where report_date between current_date - interval '7' day and current_date - interval '1' day

group by 1,2,3 
 )      

 select  a.uid 
        ,sm.shipper_name
        ,sm.city_name
        ,a.report_date 
        ,a.conditions_pass
        ,array_except(cond.compare_condition,a.conditions_pass) as kpi_failed 
        -- ,element_at(cond.compare_condition) as test



from kpi_v2 a 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = a.uid and try_cast(sm.grass_date as date) = a.report_date 

cross join cond 