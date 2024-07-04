WITH year_week as 
(SELECT distinct 
    year(report_date)*100 + week(report_date) as year_week
    ,min(report_date) as min_date
    ,max(report_date) as max_date 

FROM
    ((SELECT sequence(date'2022-05-23',date'2022-06-19') bar)
CROSS JOIN
    unnest (bar) as t(report_date)
)

group by 1
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
,final_onboard as 
(select a.shipper_id
        ,a.first_join_hub
        ,sm.city_id
        ,case when sm.shipper_type_id <> 12 then last_drop_hub else null end as last_drop_hub
        ,week(a.first_join_hub)*100 + week(a.first_join_hub) as week_join
        ,date_diff('day',first_join_hub,last_drop_hub) as seniority
        ,concat(cast(coalesce(slot.shift_hour,(sm.shipper_shift_end_timestamp - sm.shipper_shift_start_timestamp)/3600) as varchar),' - ','hour shift') as shift_hour 



from hub_onboard a 

--Take driver shift
left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = a.shipper_id and try_cast(sm.grass_date as date) = a.first_join_hub

left join 
(select uid 
        ,(end_time - start_time)/3600 as shift_hour 
        ,row_number()over(partition by uid order by date_ts desc) as rank 

from shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live 
)slot on slot.uid = a.shipper_id and slot.rank = 1 

where first_join_hub <= date'2022-06-19'
)

select y.year_week
      ,y.min_date
      ,y.max_date
      ,a.shift_hour
      ,count(distinct a.shipper_id) as total_driver 
      ,sum(a.seniority)*1.00/count(distinct a.shipper_id) as seniority

 
from final_onboard a 

inner join year_week y on 
    (
    (a.first_join_hub <= y.min_date and (last_drop_hub is null 
                                            or 
                                        last_drop_hub >= y.min_date
                                        )
    )

    
     or (a.first_join_hub between y.min_date and y.max_date 
                                             and (last_drop_hub is null 
                                                    or 
                                                  last_drop_hub >= y.min_date
                                                 )
     )
    )

where city_id in (217,218,220)
group by 1,2,3,4