
with hub_income as
(select 
    date(from_unixtime(hub.report_date - 3600)) as report_date
    ,uid as shipper_id
    ,cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) as shift_category_name
    ,cast(json_extract(hub.extra_data,'$.total_order') as bigint) as total_order_inshift
    ,case when cast(json_extract(hub.extra_data,'$.is_apply_fixed_amount') as VARCHAR) = 'true' then (cast(json_extract(hub.extra_data,'$.total_income') as bigint) - cast(json_extract(hub.extra_data,'$.calculated_shipping_shared') as bigint))
    else 0 end as extra_ship
    ,cast(json_extract(hub.extra_data,'$.is_apply_fixed_amount') as VARCHAR) as is_apply_fixed_amount -- check driver has order <= threshold and pass all kpi >> dieu kien de duoc bu min
    ,cast(json_extract(hub.extra_data,'$.total_income') as bigint) as total_income
    ,cast(json_extract(hub.extra_data,'$.calculated_shipping_shared') as bigint) as calculated_shipping_shared
    ,hub.extra_data
    ,cast(json_extract(hub.extra_data,'$.lasted_shipper_info.city_id') as bigint) as city_id
    ,case 
    	when cast(json_extract(hub.extra_data,'$.lasted_shipper_info.city_id') as bigint) not in (217,218,220) then 999 
    	else cast(json_extract(hub.extra_data,'$.lasted_shipper_info.city_id') as bigint) end as dummy_city_id
    ,cast(json_extract(hub.extra_data,'$.hub_ids') as array<int>) as hub_id
from shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hub
-- where date(from_unixtime(hub.report_date - 3600)) between current_date - interval '30' day and current_date - interval '1' day
)
    ,list_shipper_hub_id as
    (select 
        hub.report_date
        ,hub.shipper_id
        ,hub.hub_id as hub_array
        ,a.hub_id
    from hub_income hub
    cross join unnest (hub_id) a(hub_id)
    )

,final_hub_name_array as
(select
    b.report_date
    ,b.shipper_id
    ,hub_array
    ,array_agg(distinct inf.hub_name) as hub_name_array
from list_shipper_hub_id b
left join shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live inf on inf.id = b.hub_id
group by 1,2,3
)

,base as
(select 
    hub.*
    ,case when hub.total_order_inshift < cast(config.min_order as int) then 1 else 0 end as is_under_min
    ,case when is_apply_fixed_amount = 'true' and hub.total_order_inshift < cast(config.min_order as int) then 1 else 0 end is_compensated
    ,cast(config.min_order as int) as min_order
from hub_income hub
left join vnfdbi_opsndrivers.ingest_hub_config_min_order config
	on lower(hub.shift_category_name) = lower(config.shift_category_name) and hub.dummy_city_id = cast(config.city_id as bigint) and hub.report_date between cast (config.start_date as date)  and  coalesce(try_cast(config.end_date as date) ,current_date) 
)
,compensate_driver as
(select 
    b.*
    ,case when sip.working_status = 1 then 'WORKING' 
        when sip.working_status = 2 then 'OFF'
        when sip.working_status = 3 then 'WAITING'
        else null end as working_status
    ,case when sm.shipper_type_id = 12 then 'hub' else 'non-hub' end as current_shipper_type
    ,hub_name.hub_name_array
from base b
left join shopeefood.foody_internal_db__shipper_info_personal_tab__reg_continuous_s0_live sip
    on sip.uid = b.shipper_id
left join shopeefood.foody_mart__profile_shipper_master sm
    on b.shipper_id = sm.shipper_id and sm.grass_date = 'current'
left join final_hub_name_array as hub_name
    on b.shipper_id = hub_name.shipper_id and b.report_date = hub_name.report_date

where shift_category_name in ('10 hour shift','8 hour shift')
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
,hub_driver as
(select 
    *
from hub_onboard
where current_date - interval '7' day between first_join_hub and last_drop_hub
or current_date - interval '6' day between first_join_hub and last_drop_hub
or current_date - interval '5' day between first_join_hub and last_drop_hub
or current_date - interval '4' day between first_join_hub and last_drop_hub
or current_date - interval '3' day between first_join_hub and last_drop_hub
or current_date - interval '2' day between first_join_hub and last_drop_hub
or current_date - interval '1' day between first_join_hub and last_drop_hub
)
-- select 
--     *
-- from hub_driver
-- where shipper_id = 23132367
-- select 
--     *
-- from compensate_driver
-- where shift_category_name is not null
-- limit 10
,final as
(select 
    s.report_date
    ,s.shipper_id
    ,s.current_driver_tier
    ,s.cnt_total_order
    ,s.cnt_total_order_delivered
    ,s.cnt_delivered_order_non_free_pick
    ,s.cnt_cancelled_order
    ,s.cnt_quit_order
    ,s.cnt_total_order_delivered_food_arrive_merchant_ontime
    ,s.cnt_total_order_delivered_food_arrive_buyer_ontime
    ,s.cnt_total_order_del_for_lead_time_completion
    ,s.sum_total_leadtime_completion
    ,s.cnt_total_order_delivered_late_sla
    ,s.cnt_total_assign_order
    ,s.cnt_total_assign_order_excl_stack
    ,s.cnt_total_incharge
    ,s.cnt_auto_accept_order
    ,s.cnt_deny_total
    ,s.cnt_deny_non_acceptable
    ,s.cnt_deny_acceptable
    ,s.cnt_ignore_total
    ,s.cnt_ignore_single
    ,s.cnt_ignore_stack
    ,s.cnt_ignore_group
    ,s.total_online_time
    ,s.total_working_time
    ,s.peak_online_time
    ,s.peak_work_time
    ,coalesce(cd.extra_ship,0) extra_ship
    ,coalesce(cd.total_order_inshift,0) total_order_inshift
    ,coalesce(cd.is_compensated,0) is_compensated
    ,date_diff('day',hub.first_join_hub,current_date - interval '1' day) as day_join_hub

from vnfdbi_opsndrivers.snp_foody_shipper_daily_report s
inner join compensate_driver cd
    on s.shipper_id = cd.shipper_id and s.report_date = cd.report_date
left join hub_driver hub
    on s.shipper_id = hub.shipper_id
where s.report_date between current_date - interval '7' day and current_date - interval '1' day
and s.current_driver_tier = 'Hub'
and s.total_online_time > 0
)
,dataset as
(select 
    shipper_id
    ,current_driver_tier
    ,day_join_hub
    ,avg(cnt_total_order_delivered) as avg_cnt_total_order_delivered
    ,case when sum(cnt_total_order_delivered) = 0 then 0 else sum(sum_total_leadtime_completion)/sum(cnt_total_order_delivered) end as avg_completion_time
    ,avg(total_online_time) as avg_total_online_time
    ,avg(total_working_time) as avg_total_working_time
    ,avg(extra_ship) as avg_extra_ship
    ,avg(total_order_inshift) as avg_total_order_inshift
    ,sum(is_compensated) total_day_compensate

from final
group by 1,2,3
)
,dataset_pivot as
(select
    -- date(t1.report_month) report_month
    t1.shipper_id
    ,t1.current_driver_tier
    ,t2.metric
    ,t2.value
from dataset t1
cross join unnest ( array['day_join_hub','avg_completion_time','avg_extra_ship','avg_total_order_inshift','total_day_compensate']
        , array[day_join_hub,avg_completion_time,avg_extra_ship,avg_total_order_inshift,total_day_compensate]) t2 (metric, value)

)
,summary_metric as
(select 
    metric
    ,min(value) as min_
    ,max(value) as max_
from dataset_pivot
group by 1
)
,driver_profile_ranking as 
(select 
    d.shipper_id
    ,d.metric
    ,d.value
    ,s.min_
    ,s.max_
    ,case 
        when d.metric in ('avg_total_order_inshift','day_join_hub') then (d.value - s.min_)/(s.max_ - s.min_) 
        else 1 - (d.value - s.min_)/(s.max_ - s.min_) end as scaled_value
from dataset_pivot d
left join summary_metric s
    on d.metric = s.metric
)
select 
    *
from driver_profile_ranking
