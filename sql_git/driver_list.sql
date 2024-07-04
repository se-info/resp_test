with profile as 
(select 
        sm.shipper_id
       ,pf.shopee_uid
    --    ,length(cast(pf.uid as varchar)) as check_2
    --    ,substr(cast(sm.shipper_id as varchar ),length(cast(pf.uid as varchar)) - 3,length(cast(pf.uid as varchar))) as check_3
       ,LOWER(TRIM(pf.last_name))||' '||LOWER(TRIM(pf.first_name))||' '||substr(cast(sm.shipper_id as varchar ),length(cast(pf.uid as varchar)) - 3,length(cast(pf.uid as varchar))) as check_1 
       ,sm.shipper_name
       ,sm.city_name
       ,substr(cast(pf.birth_date as varchar),1,4) as year_of_birth 
       ,2022 - cast(substr(cast(pf.birth_date as varchar),1,4) as bigint) as age
       ,pf.gender
       ,date(from_unixtime(pf.create_time - 3600)) as onboard_date  
       ,date_diff('day',date(from_unixtime(pf.create_time - 3600)),current_date ) as seniority
       ,CASE WHEN sm.shipper_type_id = 11 then 'Non Hub'
             WHEN pf.working_time_id = 1 then '5 hour shift'
             WHEN pf.working_time_id = 2 then '8 hour shift'
             WHEN pf.working_time_id = 3 then '10 hour shift'
             else 'Others' end as driver_type
    --    ,pf.main_phone
    --    ,pf.secondary_phone              

from shopeefood.foody_mart__profile_shipper_master sm 

left join shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live pf on pf.uid = sm.shipper_id

-- select * from shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live

where sm.grass_date = 'current'

and sm.city_name not like '%Test%'

and sm.shipper_status_code = 1 
)
select * from profile
