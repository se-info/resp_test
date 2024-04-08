with shift_type as 
(select 
       year(cast(grass_date as date)) * 100 + week(cast(grass_date as date)) as year_week
       ,cast(grass_date as date) as date_
      ,sm.shipper_id
      ,coalesce(slot.shift_hour,(sm.shipper_shift_end_timestamp - sm.shipper_shift_start_timestamp)/3600) as shift_hour 
    --   ,sm.shipper_name 
      ,sm.city_name
    --   ,coalesce(current.current_driver_tier,'Others') as tier_type
      ,case when sm.shipper_type_id = 12 then 'Hub' else 'Non Hub' end as working_group
      ,dense_rank()over(partition by shipper_id,year(cast(grass_date as date)) * 100 + week(cast(grass_date as date)) order by cast(grass_date as date) desc) as rank



from
shopeefood.foody_mart__profile_shipper_master sm

left join 
(select 
         uid
        ,date_ts  
        ,year(date(from_unixtime(date_ts - 3600))) *100 + week(date(from_unixtime(date_ts - 3600))) as year_week
        ,(end_time - start_time)/3600 as shift_hour 
        ,dense_rank()over(partition by uid,date(from_unixtime(date_ts - 3600)) order by date_ts desc) rank 





from shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live

where date(from_unixtime(date_ts - 3600)) between date'2022-05-23' and date'2022-06-10'
and registration_status != 2) slot on slot.uid = sm.shipper_id and slot.rank = 1 



where grass_date != 'current'
-- and shipper_id = 2996387
and cast(grass_date as date) between date'2022-05-23' and date'2022-06-19'
)

,hub_onboard AS
(SELECT
      shipper_id
    , shipper_ranking - type_ranking AS groupx_
    , MIN(report_date) AS first_join_hub
    , MAX(report_date) AS last_drop_hub
FROM
    (SELECT
        shipper_id
        , shipper_type_id
        , DATE(grass_date) AS report_date
        , RANK() OVER (PARTITION BY shipper_id ORDER BY DATE(grass_date)) AS shipper_ranking
        , RANK() OVER (PARTITION BY shipper_id, shipper_type_id ORDER BY DATE(grass_date)) AS type_ranking
    FROM shopeefood.foody_mart__profile_shipper_master
    WHERE shipper_type_id IN (12, 11)
    AND grass_date != 'current'
    )
WHERE shipper_type_id = 12
GROUP BY 1,2
)

,base_driver as 
(select 
       year(cast(grass_date as date)) * 100 + week(cast(grass_date as date)) as year_week
       ,cast(grass_date as date) as date_ 
    
      ,sm.shipper_id
    --   ,sm.shipper_name 
      ,sm.city_name
      ,concat(cast(b.shift_hour as varchar ),' -',' hour shift') as shift_type
    --   ,coalesce(current.current_driver_tier,'Others') as tier_type
      ,case when sm.shipper_type_id = 12 then 'Hub' else 'Non Hub' end as working_group
      ,row_number()over(partition by sm.shipper_id,year(cast(grass_date as date)) * 100 + week(cast(grass_date as date)) order by cast(grass_date as date) desc) as rank 
      ,case when date_diff('day',ob.first_join_hub,cast(grass_date as date)) <= 30 then 'New Driver' else 'Existing Driver' end as seniority
    --   ,count(distinct sm.shipper_id) as total_driver
      --,min(case when ado.report_date between current_date - interval '29' day and current_date - interval '1' day then ado.report_date else null end) as min_date
    --   ,coalesce(sum(case when ado.report_date between current_date - interval '7' day and current_date - interval '1' day then ado.total_order else null end),0) as total_order_l7d
    --   ,coalesce(sum(case when ado.report_date between current_date - interval '30' day and current_date - interval '1' day then ado.total_order else null end),0) as total_order_l30d
    --   ,coalesce(sum(case when ado.report_date between current_date - interval '60' day and current_date - interval '1' day then ado.total_order else null end),0) as total_order_l60d
    --   ,coalesce(sum(case when ado.report_date between current_date - interval '90' day and current_date - interval '1' day then ado.total_order else null end),0) as total_order_l90d



from
shopeefood.foody_mart__profile_shipper_master sm

left join hub_onboard ob on ob.shipper_id = sm.shipper_id and ob.first_join_hub <= cast(grass_date as date)

---ado
-- left join
-- (SELECT date(from_unixtime(dot.real_drop_time - 3600)) as report_date
-- ,year(date(from_unixtime(dot.real_drop_time - 3600)))*100 + week(date(from_unixtime(dot.real_drop_time - 3600))) as create_year_week
-- ,dot.uid
-- , count(dot.ref_order_code) as total_order
-- FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
-- where dot.order_status = 400

-- --and dot.uid = 8644811
-- and date(from_unixtime(dot.real_drop_time - 3600)) >= date((current_date) - interval '90' day)
-- --and date(from_unixtime(dot.real_drop_time - 3600)) < date(current_date)
-- --and date(from_unixtime(dot.real_drop_time - 3600)) between date('2021-10-27') and date('2021-11-10')

-- group by 1,2,3)ado on ado.uid = sm.shipper_id

---tier 
-- LEFT JOIN
-- (SELECT cast(from_unixtime(bonus.report_date - 60*60) as date) as report_date
-- ,bonus.uid as shipper_id
-- ,case when hub.shipper_type_id = 12 then 'Hub'
-- when bonus.tier in (1,6,11) then 'T1' when bonus.tier in (2,7,12) then 'T2'
-- when bonus.tier in (3,8,13) then 'T3'
-- when bonus.tier in (4,9,14) then 'T4'
-- when bonus.tier in (5,10,15) then 'T5'
-- else null end as current_driver_tier
-- ,bonus.total_point
-- ,bonus.daily_point

-- FROM shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus

-- LEFT JOIN
-- (SELECT shipper_id
-- ,shipper_type_id
-- ,case when grass_date = 'current' then date(current_date)
-- else cast(grass_date as date) end as report_date

-- from shopeefood.foody_mart__profile_shipper_master

-- where 1=1
-- and (grass_date = 'current' OR cast(grass_date as date) >= date('2019-01-01'))
-- GROUP BY 1,2,3
-- )hub on hub.shipper_id = bonus.uid and hub.report_date = cast(from_unixtime(bonus.report_date - 60*60) as date)

-- where cast(from_unixtime(bonus.report_date - 60*60) as date) = date(current_date) - interval '1' day
-- )current on sm.shipper_id = current.shipper_id


left join shift_type b on b.shipper_id = sm.shipper_id and b.rank = 1 and b.year_week = (year(cast(grass_date as date)) * 100 + week(cast(grass_date as date)))

where 1 = 1 
and sm.shipper_status_code = 1 
-- and sm.shipper_id = 2996387
and sm.city_name not like '%Test%'
and grass_date  != 'current' 
and cast(grass_date as date) between current_date - interval '45' day and current_date - interval '1' day

)

select  
        year_week
       ,city_name
       ,shift_type
       ,working_group
       ,count(distinct shipper_id) as total_driver 
       ,seniority

from base_driver 


where  1 = 1

and city_name in ('Ha Noi City','HCM City')

and rank = 1 

group by 1,2,3,4,6



-- limit 200
