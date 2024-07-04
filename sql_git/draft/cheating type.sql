with base as
(select 
    from_unixtime(create_time-3600) create_time
    ,id
    -- ,previous_balance/100+ previous_deposit/100 as total_balance
    ,user_id
    ,sum(balance/100) over (partition by user_id order by id asc rows between unbounded preceding and current row) as balance
    ,sum(deposit/100) over (partition by user_id order by id asc rows between unbounded preceding and current row) as deposit
    ,sum(balance/100) over (partition by user_id order by id asc rows between unbounded preceding and current row) + sum(deposit/100) over (partition by user_id order by id asc rows between unbounded preceding and current row) as total_balance

from shopeefood.foody_accountant_db__partner_transaction_tab__reg_daily_s0_live
-- where user_id = 18512834
)
,base_balance as
(select 
    create_time
    ,user_id
    ,id
    ,total_balance
    ,case 
    when total_balance <0 then 'negative'
    when total_balance >=0 then 'positive'
    else null end type_balance
from base
)
,rank_group as
(select 
    create_time
    ,id
    ,total_balance
    ,type_balance
    ,user_id
    ,rank () over(partition by user_id order by id desc) as row_user_id
    ,rank () over(partition by user_id,type_balance order by id desc) as row_type
    ,rank () over(partition by user_id order by id desc) - rank() over(partition by user_id,type_balance order by id desc) as group_rank
from base_balance
)
,balance_change_tab as
(select 
    user_id
    ,type_balance
    ,group_rank
    ,min(create_time) as start_time
    ,max(create_time) as end_time
    ,min(id) as id_txn_start
    ,max(id) as id_txn_end
from rank_group
group by 1,2,3
)
,balance_change_tab_v2 as
(select 
    user_id
    ,type_balance
    ,group_rank
    ,start_time
    ,end_time
    ,id_txn_start
    ,id_txn_end
    ,row_number() over (partition by user_id order by id_txn_start desc) as row_num
from balance_change_tab
)
select 
    t1.user_id
    ,t1.type_balance
    ,t1.group_rank
    ,t1.start_time
    -- ,t1.end_time
    ,coalesce(t2.start_time,current_date) as end_time
    ,t1.id_txn_start
    ,t1.id_txn_end
    ,bl_st.total_balance as balance_start_period
    ,bl_en.total_balance as balance_end_period
    ,case when t2.start_time is null then 'current' else 'past' end as type_period
    
from balance_change_tab_v2 t1
left join balance_change_tab_v2 t2
    on t1.user_id = t2.user_id and t1.row_num-1 = t2.row_num
left join base as bl_st
    on t1.user_id = bl_st.user_id and t1.id_txn_start = bl_st.id
left join base as bl_en
    on t1.user_id = bl_en.user_id and t1.id_txn_end = bl_en.id

where t1.user_id in (18652602,18512834)