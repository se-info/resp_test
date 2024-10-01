-- select distinct end_type from
select distinct code
from
(select 
        json_extract("data",'$.end_type') as end_type,
        cast(json_extract("data",'$.now_driver_uid') as bigint) as uid,
        cast(json_extract("data",'$.order_code') as varchar) as code,
        "data" 

from shopeefood.shopeefood_mart_dwd_vn_merchant_driver_traffic_civ_di	
where page_section like '%free_pick%'
and date(dt) >= date'2024-09-25'
and operation = 'action_view_end'
and page_type = 'foody_driver_home'
)
-- where cast(end_type as varchar) = 'Screenshot Freepick Page'
where code = '240925E93YBB'
group by 1 
-- order by 2 desc 