with raw as 
(select * from dev_vnfdbi_opsndrivers.driver_ops_support_team_ticket_tab
where 
(ticket_category_v1 like '%Merchant Is Too Far Away%'
or 
ticket_category_v2 like '%Merchant Is Too Far Away%')
and date(created) = date'2023-11-25'
)
,metrics as 
(select 
        sa.*,
        ST_Distance(to_spherical_geography(ST_Point(cast(split(location,',')[2] as double),cast(split(location,',')[1] as double)))
                    ,to_spherical_geography(ST_Point(CAST(dot.sender_location[2] AS DOUBLE),CAST(dot.sender_location[1] AS DOUBLE)))
                    ) as assign_to_sender_distance,
        row_number()over(partition by order_id order by ref_order_id asc) as rank_group_order


from (select * from driver_ops_order_assign_log_tab where cardinality(split(location,',')) >= 2) sa

LEFT JOIN 
(SELECT 
      *
     ,(ARRAY[pick_latitude,pick_longitude]) AS sender_location
FROM driver_ops_raw_order_tab
) dot on dot.id = sa.ref_order_id 
      and dot.order_type = sa.order_category    
)
select 
        raw.*,
        m.assign_type,
        m.order_type,
        m.assign_to_sender_distance,
        m.order_category,
        m.rank_group_order
from raw 
left join metrics m 
    on m.order_code = raw.order_code
    and m.driver_id = raw.driver_id







