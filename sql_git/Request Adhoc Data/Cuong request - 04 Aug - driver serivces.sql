with metrics(id,service_name) as(
    VALUES
('-1','All service')
,('0','Delivery')
,('1','Express')
,('2','Shopee')
,('3','Now Moto')
,('4','Now Ship')
,('5','Ship Merchant')
,('6','Ship Shopee')
,('7','Same Day')
,('8','Ship Multi Drop')

)
,final as 
(select 
         a.uid as shipper_id 
        ,sm.shipper_name 
        ,sm.city_name 
        ,sm.shipper_status_code
        ,case when sm.shipper_type_id = 12 then 'Hub' else 'Non Hub' end as working_type 
        ,a.service_id
        ,b.service_name



from (
(select uid,split(coalesce(services,'-1'),',') as t 
from
    shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live
)
    
CROSS JOIN 
          unnest (t) as t(service_id)

          
    ) a    

left join metrics b on a.service_id = b.id 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = a.uid and try_cast(sm.grass_date as date) = current_date - interval '1' day

)    

select 
-- * from final where shipper_id = 21070211
         shipper_id
        ,shipper_name
        ,city_name
        ,working_type
        ,array_agg(service_name) as service_name 



from final         

where shipper_status_code = 1 
-- where 1 = 1 
-- and shipper_id = 21070211
                                              

group by 1,2,3,4
