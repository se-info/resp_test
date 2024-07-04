with order_point as
(select 
        raw.id,
        raw.shipper_id,
        raw.order_status,
        delivered_timestamp,
        date(delivered_timestamp) as report_date,
        raw.city_name as order_city_name,
        sm.city_name as shipper_city_name,
        sm.city_id as shipper_city_id,
        -- case 
        -- when raw.city_id != sm.city_id then 0
        -- else point.point end as point,
        point,
        driver_policy


from driver_ops_raw_order_tab raw 

left join shopeefood.foody_mart__profile_shipper_master sm 
        on sm.shipper_id = raw.shipper_id
        and try_cast(sm.grass_date as date) = date(raw.delivered_timestamp)     

left join shopeefood.foody_partner_db__order_point_log_tab__reg_daily_s0_live point 
        on raw.id = point.order_id
        and raw.order_type = point.order_type 
where 1 = 1 
and raw.order_status in ('Delivered','Quit')
)
,metrics as 
(select report_date,
        shipper_id,
        shipper_city_id,
        sum(point) as daily_point

from order_point 
group by 1,2,3
)
,final_metrics as 
(select 
        try_cast(m.grass_date as date) as report_date,
        m.shipper_id,
        m.city_id as shipper_city_id,
        case 
        when m.shipper_type_id = 12 then 'hub'
        else 'non hub' end as working_type,
        coalesce(m_1.daily_point,0) as daily_point, 
        try_cast(m.grass_date as date) - interval '1' day as last_period,
        try_cast(m.grass_date as date) - interval '30' day as first_period,
        sum(coalesce(m_2.daily_point,0)) as l30d_point

from shopeefood.foody_mart__profile_shipper_master m

left join metrics m_1 
        on m_1.shipper_id = m.shipper_id
        and m_1.report_date = try_cast(m.grass_date as date)

left join metrics m_2 
        on m_2.shipper_id = m.shipper_id
        and m_2.report_date between try_cast(m.grass_date as date) - interval '30' day and try_cast(m.grass_date as date) - interval '1' day

        
where 1 = 1 
and regexp_like(lower(m.city_name),'test|dien bien|smoke|live') = false
and try_cast(m.grass_date as date) != current_date
and try_cast(m.grass_date as date) between date'2022-01-01' and current_date - interval '1' day
group by 1,2,3,4,5,6
)
select 
        f.*,
        ti.tier_id,
        case 
        when f.working_type = 'hub' then 'hub'
        else coalesce(ti.tier_name,'Other') end as tier


from final_metrics f 

left join shopeefood.foody_internal_db__shipper_tier_config_tab__reg_daily_s0_live ti 
        on ti.city_id = f.shipper_city_id
        and f.l30d_point between ti.from_total_point and ti.to_total_point

where f.shipper_id = 41422167
and f.report_date between current_date - interval '30' day and current_date - interval '1' day
;





