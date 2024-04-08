with raw_1 as 
( 
select 
         date(from_unixtime(a.create_time - 3600)) as pending_date
        ,sm.city_name
        ,IF(sm.shipper_type_id = 12,'Hub','Non Hub') as working_group  
        ,case when from_value_text = 'Normal' and to_value_text in ('Stop','Pending') then 1 
              when from_value_text = '(Status old) - NORMAL' and to_value_text = '(Status new) - PENDING' then 1
              else 0 end as conditions_
        ,from_value_text
        ,to_value_text
        ,uid 
        ,create_user_name
        -- ,row_number()over(partition by uid order by create_time desc) as rank 
        -- ,count(distinct uid) as total_drivers


from shopeefood.foody_internal_db__shipper_log_change_tab__reg_daily_s0_live a 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = a.uid and try_cast(sm.grass_date as date) = date(from_unixtime(a.create_time - 3600))

where 1 = 1 

)

,raw_2 as 
( 
select 
         date(from_unixtime(a.create_time - 3600)) as open_date
        ,sm.city_name
        ,IF(sm.shipper_type_id = 12,'Hub','Non Hub') as working_group  
        ,case when  from_value_text in ('Stop','Pending') and to_value_text = 'Normal' then 1 
              when  from_value_text = '(Status old) - PENDING' and to_value_text = '(Status new) - NORMAL' then 1
              else 0 end as conditions_
        ,from_value_text
        ,to_value_text
        ,uid 
        ,create_user_name
        -- ,count(distinct uid) as total_drivers


from shopeefood.foody_internal_db__shipper_log_change_tab__reg_daily_s0_live a 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = a.uid and try_cast(sm.grass_date as date) = date(from_unixtime(a.create_time - 3600))

where 1 = 1 

)
-- select * from raw_2 where uid = 17550544 and created_date = date'2022-07-04'

,final_1 as
(select * 
        ,row_number()over(partition by uid order by pending_date desc) as rank 



from raw_1 


where 1 = 1 


and conditions_ = 1 
)
,final_2 as  
(
select *
        ,row_number()over(partition by uid order by open_date desc) as rank 



from raw_2 

where 1 = 1 


and conditions_ = 1 

)

,final_v2 as 
(select  a.* 
        ,case when a.pending_date <= b.open_date then 1 else 0 end as is_valid
        ,b.open_date
        ,date_diff('day',a.pending_date,b.open_date) as diff_pending_to_normal


from final_1 a 

left join final_2 b on b.uid = a.uid and b.rank = a.rank



where 1 = 1 

-- and a.rank = 1 

-- and a.create_user_name <> 'System'

-- and b.create_user_name <> 'System'

-- and a.uid = 9965039

-- and a.created_date = date'2022-07-04'

-- having (case when a.created_date <= b.created_date then 1 else 0 end) =
)



select * from final_v2 


where is_valid = 1 

-- and a.pending_date between ${start_date} and ${end_date}