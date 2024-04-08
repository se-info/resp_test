select date_ 
       ,shipper_id
       ,array_agg(hub_name) as hub_name  


from 
(SELECT a1.date_
      ,a1.uid as shipper_id
      ,a1.hub_id_
      ,inf.hub_name

FROM
(
select uid,date(from_unixtime(hub.report_date - 3600)) as date_ ,a.hub_id_ 

from shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hub

CROSS JOIN UNNEST 
(
    cast(json_extract(hub.extra_data,'$.hub_ids') as array<int>)
) a(hub_id_)

)a1 

left join shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live inf on inf.id = a1.hub_id_

)


group by 1,2
