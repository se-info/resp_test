with data_checking as 
(select name
       ,TRIM(regexp_replace(name,'0|1|2|3|4|5|6|7|8|9')) ||'-'||dob as lookup_value_1
       ,TRIM(regexp_replace(name,'0|1|2|3|4|5|6|7|8|9')) ||'-'||id_card as lookup_value_2
       ,TRIM(regexp_replace(name,'0|1|2|3|4|5|6|7|8|9')) ||'-'||email as lookup_value_3
       ,TRIM(regexp_replace(name,'0|1|2|3|4|5|6|7|8|9')) ||'-'||phone as lookup_value_4    



from dev_vnfdbi_opsndrivers.phong_checking_onboard
)

,driver_list as 
(
select uid,date(from_unixtime(create_time - 3600)) as onboard_date
       ,TRIM(regexp_replace(full_name,'0|1|2|3|4|5|6|7|8|9')) ||'-'||substr(cast(birth_date as varchar),7,2)||'/'||substr(cast(birth_date as varchar),5,2)||'/'||substr(cast(birth_date as varchar),1,4) as lookup_value_1
       ,TRIM(regexp_replace(full_name,'0|1|2|3|4|5|6|7|8|9')) ||'-'||national_id_number as lookup_value_2
       ,TRIM(regexp_replace(full_name,'0|1|2|3|4|5|6|7|8|9')) ||'-'||personal_email as lookup_value_3
       ,TRIM(regexp_replace(full_name,'0|1|2|3|4|5|6|7|8|9')) ||'-'||main_phone as lookup_value_4 

from shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live 
-- where uid = 40128602
)
,final_filter as 
(select 
        a.* 
        ,case when b.uid is not null then b.uid 
              when c.uid is not null then c.uid  
              when d.uid is not null then d.uid  
              when e.uid is not null then e.uid          
        else null end as driver_id
        ,case when b.uid is not null then b.onboard_date 
              when c.uid is not null then c.onboard_date
              when d.uid is not null then d.onboard_date  
              when e.uid is not null then e.onboard_date          
        else null end as onboard_date


from data_checking a 

left join driver_list b on a.lookup_value_1 = b.lookup_value_1

left join driver_list c on a.lookup_value_2 = c.lookup_value_2

left join driver_list d on a.lookup_value_3 = d.lookup_value_3

left join driver_list e on a.lookup_value_4 = e.lookup_value_4
)
select
      name
    --  ,quit_work_date 
    --  ,map(array['driver_id','onboard_date','quit_work_date','quit_work_reason'],array[driver_id,onboard_date,quit_work_date,quit_work_reason]) as mapping_data   
     ,map(array['driver_id','onboard_date'],array[driver_id,onboard_date]) as onboard_data   
     ,map(array['quit_work_date','quit_work_reason'],array[quit_work_date,quit_work_reason]) quit_work_data

from 
(select 
        a.name
       ,array_agg(distinct coalesce(cast(date(from_unixtime(quit.create_time - 3600)) as varchar),null)) as quit_work_date  
       ,array_agg(distinct coalesce(q_name.name_en,null)) as quit_work_reason         
       ,array_agg(distinct cast(a.driver_id as varchar)) as driver_id 
       ,array_agg(distinct cast(a.onboard_date as varchar)) as onboard_date
    --    ,map(array['driver_id','onboard_date'],array[cast(driver_id as varchar),cast(onboard_date as varchar)]) as mapping_data 
    --    ,map_agg(cast(id_driver as varchar),cast(onboard_date as varchar)) as driverid_map_onboard 
    --    ,array_join(array_agg(distinct is_fresh),',') as checking_driver_profile 

from final_filter a        

LEFT JOIN shopeefood.foody_internal_db__shipper_quit_request_tab__reg_daily_s0_live quit on quit.uid = a.driver_id 

LEFT JOIN shopeefood.foody_internal_db__shipper_quit_request_reason_tab__reg_daily_s0_live q_name on q_name.id = quit.reason_id

group by 1
)


-- where name = 'Phạm Xuân Hiệp 1997'


