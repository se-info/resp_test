with order_raw as 
(select 
        date(delivered_timestamp) as report_date,
        order_code,
        id,
        shipper_id,
        row_number()over(partition by shipper_id order by id desc) as rank_


from driver_ops_raw_order_tab
where date(delivered_timestamp) between date'2024-05-01' and date'2024-05-01'
and order_status = 'Delivered'
-- and source = 'order_food'
)

,raw as 
(select  
        report_date,
        city_name,
        shipper_tier,
        shipper_id,
        total_order_food as total_order,
        sla_rate


from driver_ops_driver_performance_tab
where 1 = 1 
and report_date between date'2024-04-28' and date'2024-05-01'
and total_order_food > 0 
)
,eligible_driver as 
(select *
from
(select t1.*,
        case when t1.city_name not in ('Dak Lak','Thanh Hoa','Binh Thuan','Binh Dinh','Long An','Tien Giang','An Giang','Nam Dinh City','Hai Duong') 
        and t1.sla_day_qualified >= 4 and t1.working_day_qualified >= 4 then 
        (case 
        when total_order between 48 and 79 then 90000
        when total_order > 79 then 200000 else 0 end) 

        when city_name in ('Dak Lak','Thanh Hoa','Binh Thuan','Binh Dinh','Long An','Tien Giang','An Giang','Nam Dinh City','Hai Duong')
        and t1.working_day_qualified >= 4 then 
        (case 
        when total_order between 48 and 79 then 90000
        when total_order > 79 then 200000 else 0 end) else 0 end as bonus_value
from 
(select 
        shipper_id,
        max_by(city_name,report_date) as city_name,
        sum(total_order) as total_order,
        cardinality(filter(array_agg(distinct case when total_order >= 4 then report_date else null end),x->x is not null)) as working_day_qualified,
        cardinality(filter(array_agg(distinct case when sla_rate >= 90 then report_date else null end),x->x is not null)) as sla_day_qualified,
        map_agg(report_date,sla_rate) as sla_info,
        map_agg(report_date,total_order) as order_info
from raw
group by 1 
) t1 
inner join vnfdbi_opsndrivers.phong_test_table t2 on t1.shipper_id = cast(t2.shipper_id as bigint)
)
where bonus_value > 0 
)
select 
        raw.*,  
        ed.total_order,
        -- ed.bonus_value*1.00/(if(r.rank_ < 10,4,10)) as bonus_value,
        ed.bonus_value*1.00/4 as bonus_value,
        r.rank_ as rank_max,
        sm.shipper_name,
        sm.city_name,
        'spf_do_0002_ Thuong_dai_le _'||date_format(raw.report_date,'%d/%m/%Y')||'_'||cast(raw.id as varchar) as note_

from order_raw raw  

left join 
(select 
        shipper_id,
        max(rank_) as rank_
from order_raw 
group by 1 
) r on r.shipper_id = raw.shipper_id

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = raw.shipper_id and sm.grass_date = 'current'

inner join eligible_driver ed on ed.shipper_id = raw.shipper_id 

-- where raw.rank_ <= if(r.rank_ < 10,4,10)
where raw.rank_ <= 4

