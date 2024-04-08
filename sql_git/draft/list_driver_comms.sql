with hub AS
(SELECT shipper_id
,min(case when grass_date = 'current' then date(current_date) else cast(grass_date as date) end) first_day_in_hub
,max(case when grass_date = 'current' then date(current_date) else cast(grass_date as date) end) last_day_in_hub
from shopeefood.foody_mart__profile_shipper_master

where 1=1
and shipper_type_id = 12 and grass_region = 'VN'
group by 1)

select a.shipper_id
      ,sm.city_name 
      ,a.first_day_in_hub
      ,a.last_day_in_hub
      ,case
         
        WHEN st.shift_category = 1 then '5 hour shift'
        WHEN st.shift_category = 2 then '8 hour shift'
        WHEN st.shift_category = 3 then '10 hour shift'
        WHEN st.shift_category = 4 then '3 hour shift'
        ELSE 'Part-time' END AS hub_type
      ,coalesce(date(from_unixtime(hub.report_date - 3600)),null) as last_active_date  
      ,case when sm.shipper_type_id = 12 then 'Hub' else 'Non Hub' end as current_working_group
      ,case when sm.shipper_status_code = 1 then 'Working' else 'Off' end as working_status          


from hub a 

LEFT JOIN shopeefood.foody_internal_db__shipper_info_work_tab__reg_daily_s0_live st on st.uid = a.shipper_id

left join (select *,row_number()over(partition by uid order by report_date desc ) as rank 
            from shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live) hub on hub.uid = a.shipper_id and hub.rank = 1

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = a.shipper_id and sm.grass_date = 'current'


