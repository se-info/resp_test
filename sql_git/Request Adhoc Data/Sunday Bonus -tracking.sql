with sunday_bonus as
(select 
                  user_id
                 ,year(date_parse((CAST(cast(split(note,' ') as array<json>)[11] as varchar)||'/'||'22'),'%d/%m/%y'))*100 + week(date_parse((CAST(cast(split(note,' ') as array<json>)[11] as varchar)||'/'||'22'),'%d/%m/%y')) as year_week                
                --  ,date_parse((CAST(cast(split(note,' ') as array<json>)[9] as varchar)||'/'||'22'),'%d/%m/%y') as start_date 
                 ,date_parse((CAST(cast(split(note,' ') as array<json>)[11] as varchar)||'/'||'22'),'%d/%m/%y') as end_date 
                 ,balance/cast(100 as double) as value_bonus   


        from (select * ,case when user_id in (20754253,6872389,21722008,18521518,18448800,21634564,7999552,22285257) and create_time = 1655866978 then 0 else 1 end as is_valid
              from shopeefood.foody_accountant_db__partner_transaction_tab__reg_daily_s0_live 
              where 1 = 1 
              and txn_type = 520
              and date(from_unixtime(create_time - 3600)) >= current_date - interval '30' day
              and note like '%HUB_MODEL_Thuong tai xe guong mau chu nhat tuan%'  
             )
        where 1 = 1  
        and is_valid = 1 
)
,final_metrics as 
(select 
        year(hub.date_)*100 + week(hub.date_) as report_week
       ,hub.uid as shipper_id
       ,hub.shipper_name
       ,hub.city_name
       ,sb.value_bonus
       ,case when hub.hub_type = '10 hour shift' then 10
             when hub.hub_type = '8 hour shift' then 8
             when hub.hub_type = '5 hour shift' then 5 
             when hub.hub_type = '3 hour shift' then 3 
             end as shift_hour    
       ,array_agg(distinct hub.hub_type) as hub_type
       ,sum(hub.in_shift_online_time)/cast(count(distinct hub.date_) as double) as avg_online_time
       ,count(distinct hub.date_) as working_day_exclude_sunday
    --    ,date_format(hub.date_,'%a') as day_of_week
         


from dev_vnfdbi_opsndrivers.phong_hub_driver_metrics hub 

left join sunday_bonus sb on sb.user_id = hub.uid and sb.year_week = year(hub.date_)*100 + week(hub.date_)

where date_format(hub.date_,'%a') != 'Sun'
-- and hub.uid = 2996387
and hub.date_ between date_trunc('week',current_date ) - interval '14' day and date_trunc('week',current_date ) - interval '1' day 
group by 1,2,3,4,5,6
)

select 
         report_week
        ,shipper_id
        ,shipper_name
        ,city_name
        ,hub_type
        ,value_bonus
        ,avg_online_time as avg_online_time_exclude_sunday
        ,avg_online_time/cast(shift_hour as double) as pct_online_w_shift
        ,working_day_exclude_sunday + 1 as total_working_day
        

from final_metrics


where value_bonus > 0 
