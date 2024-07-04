with raw as 
(select 
        uid
       ,code  
       ,try_cast(submit_at as timestamp) as submit_ 
       ,try_cast(pick_at as timestamp) as pick_ 
       ,coalesce(try_cast(returning_status_update_at as timestamp),null) as returning_ 
       ,try_cast(return_failed_at as timestamp) as return_failed  
       ,dense_rank()over(partition by uid order by cast(submit_at as timestamp) asc) as rank_ 
from vnfdbi_opsndrivers.phong_test_table a 
)

select a.*
      ,map_agg(b.rank_,b.code) as second_order_col_L
      ,map_agg(c.rank_,c.code) as second_order_col_M   
      ,map_agg(d.rank_,d.code) as second_order_col_N 


from raw a 

left join raw b on a.uid = b.uid and b.rank_ != 1 and b.pick_ > a.submit_

left join raw c on a.uid = c.uid and c.rank_ != 1 and c.pick_ > a.submit_ and c.pick_ < a.returning_

left join raw d on a.uid = d.uid and d.rank_ != 1 and d.pick_ > a.returning_ 

where a.rank_ = 1 
-- and a.uid = '1006700903'

group by 1,2,3,4,5,6,7
