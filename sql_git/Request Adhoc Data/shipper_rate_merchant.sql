SELECT a.create_time as report_date
      ,a.merchant_id
      ,mm.merchant_name
      ,mm.city_name 
      ,count(DISTINCT IF(a.merchant_rate = 1, a.order_id, null)) as merchant_rate_1
      ,count(DISTINCT IF(a.merchant_rate = 2, a.order_id, null)) as merchant_rate_2
      ,count(DISTINCT IF(a.merchant_rate = 3, a.order_id, null)) as merchant_rate_3
      ,count(DISTINCT IF(a.merchant_rate = 4, a.order_id, null)) as merchant_rate_4
      ,count(DISTINCT IF(a.merchant_rate > 4, a.order_id, null)) as merchant_rate_5


FROM 
(SELECT a.shipper_uid
      ,a.order_id
      ,merchant_id
      ,a.merchant_rate
      ,date(from_unixtime(a.create_time - 3600)) as create_time 
      ,b.description_en
      ,b.description



from shopeefood.foody_partner_db__shipper_feedback_order_tab__reg_daily_s0_live a 

left join shopeefood.foody_partner_db__shipper_feedback_order_reason_mapping_tab__reg_daily_s0_live c on c.feedback_id = a.id 
left join shopeefood.foody_internal_db__shipper_feedback_order_reason_tab__reg_daily_s0_live b on b.id = c.reason_id
where 1 = 1
and date(from_unixtime(a.create_time - 3600)) between current_date - interval '30' day and current_date - interval '1' day
order by create_time desc 
) a 
LEFT JOIN shopeefood.foody_mart__profile_merchant_master mm on mm.merchant_id = a.merchant_id and try_cast(mm.grass_date as date) = a.create_time
group by 1,2,3,4
