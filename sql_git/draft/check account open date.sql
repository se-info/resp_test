with base as
(select 
    uid
    ,date(min(from_unixtime(create_time-3600))) account_open_date
    -- ,*
from shopeefood.foody_internal_db__shipper_log_change_tab__reg_daily_s0_live
and change_type = 'IsActive'
group by 1
)
select 
    *
from base