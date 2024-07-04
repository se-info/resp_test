
with data_checking as 
(select full_name,uid as driver_id_curent
       ,national_id_number AS id_card_current
       ,national_id_issue_place as id_place_current
       ,TRIM(regexp_replace(LOWER(full_name),'0|1|2|3|4|5|6|7|8|9')) ||'-'||CAST(dob as VARCHAR) as lookup_value_1
       ,TRIM(regexp_replace(LOWER(full_name),'0|1|2|3|4|5|6|7|8|9')) ||'-'||CAST(national_id as VARCHAR) as lookup_value_2
       ,TRIM(regexp_replace(LOWER(full_name),'0|1|2|3|4|5|6|7|8|9')) ||'-'||CAST(personal_email as VARCHAR) as lookup_value_3
       ,TRIM(regexp_replace(LOWER(full_name),'0|1|2|3|4|5|6|7|8|9')) ||'-'||CAST(main_phone as VARCHAR) as lookup_value_4    

from (SELECT *
             ,substr(cast(birth_date as varchar),7,2)||'/'||substr(cast(birth_date as varchar),5,2)||'/'||substr(cast(birth_date as varchar),1,4) AS dob    
      FROM shopeefood.foody_internal_db__shipper_profile_tab__reg_continuous_s0_live)
WHERE DATE(FROM_UNIXTIME(create_time - 3600)) = DATE'2023-02-23'    
)

,driver_list as 
(
select uid,date(from_unixtime(create_time - 3600)) as onboard_date
       ,TRIM(regexp_replace(LOWER(full_name),'0|1|2|3|4|5|6|7|8|9')) ||'-'||substr(cast(birth_date as varchar),7,2)||'/'||substr(cast(birth_date as varchar),5,2)||'/'||substr(cast(birth_date as varchar),1,4) as lookup_value_1
       ,TRIM(regexp_replace(LOWER(full_name),'0|1|2|3|4|5|6|7|8|9')) ||'-'||national_id_number as lookup_value_2
       ,TRIM(regexp_replace(LOWER(full_name),'0|1|2|3|4|5|6|7|8|9')) ||'-'||personal_email as lookup_value_3
       ,TRIM(regexp_replace(LOWER(full_name),'0|1|2|3|4|5|6|7|8|9')) ||'-'||main_phone as lookup_value_4 

from shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live
-- where uid = 40128602
WHERE DATE(FROM_UNIXTIME(create_time - 3600)) != DATE'2023-02-23' 
)
,final_filter as 
(select 
        a.* 
        ,case when b.uid is not null then b.uid 
              when c.uid is not null then c.uid  
              when d.uid is not null then d.uid  
              when e.uid is not null then e.uid          
        else null end as driver_id
        ,case when b.uid is not null then b.onboard_date 
              when c.uid is not null then c.onboard_date
              when d.uid is not null then d.onboard_date  
              when e.uid is not null then e.onboard_date          
        else null end as onboard_date


from data_checking a 

left join driver_list b on a.lookup_value_1 = b.lookup_value_1

left join driver_list c on a.lookup_value_2 = c.lookup_value_2

left join driver_list d on a.lookup_value_3 = d.lookup_value_3

left join driver_list e on a.lookup_value_4 = e.lookup_value_4
)
,ticket_check as 
(SELECT 
            shipper_id 
            ,title
            ,count(distinct ticket_id) as total_case
FROM    
(SELECT              ht.id as ticket_id 
                    ,tot.order_code
                      ,case when ht.status = 1 then '1. Open'
                            when ht.status = 2 then '2. Pending'
                            when ht.status = 3 then '3. Resolved'
                            when ht.status = 5 then '4. Completed'
                            when ht.status = 4 then '5. Closed'
                            else null end as status
                      ,case when ht.incharge_team = 1 then 'CC'
                            when ht.incharge_team = 2 then 'PROJECTOR'
                            when ht.incharge_team = 3 then 'EDITOR'
                            when ht.incharge_team = 4 then 'GOFAST'
                            when ht.incharge_team = 5 then 'PRODUCT SUPPORT'
                            when ht.incharge_team = 6 then 'AGENT'
                            when ht.incharge_team = 7 then 'AGENT MANAGER'
                            else null end as incharge_team 
                      ,case when ht.ticket_type = 1 then 'VIOLATION_OF_RULES'
                            when ht.ticket_type = 2 then 'CHANGE_SHIPPER_INFO'
                            when ht.ticket_type = 3 then 'FRAUD'
                            when ht.ticket_type = 4 then 'CUSTOMER_FEEDBACK'
                            when ht.ticket_type = 5 then 'CC_FEEDBACK'
                            when ht.ticket_type = 6 then 'NOW_POLICE'
                            when ht.ticket_type = 7 then 'MERCHANT_FEEDBACK'
                            when ht.ticket_type = 8 then 'PARTNER_SIGNATURE_NOTE'
                            when ht.ticket_type = 9 then 'REQUEST_CHANGE_DRIVER_INFO'
                            else null end as ticket_type
                      ,ht.title
                      ,case when ht.city_id = 217 then 'HCM'
                            when ht.city_id = 218 then 'HN'
                            when ht.city_id = 219 then 'DN'
                            ELSE 'OTH' end as city_group
                      ,from_unixtime(ht.create_time - 60*60) as created_timestamp
                      ,DATE(from_unixtime(ht.create_time - 60*60)) as created_date
                      ,COALESCE(htl.label,'NO_ACTION') resolution
                      ,htu.uid as shipper_id 
                      ,sm.shipper_name
                    , cast(json_extract(ht.extra_data, '$.reporter')as varchar) created_by
                    ,concat('Note', coalesce(trim(CAST(json_extract(ht.extra_data, '$.description') AS varchar)),'N/A')) description

                FROM shopeefood.foody_internal_db__hr_tick_tab__reg_daily_s0_live ht 
                LEFT JOIN shopeefood.foody_internal_db__hr_tick_label_tab__reg_daily_s0_live htl on htl.tick_id = ht.id
                LEFT JOIN shopeefood.foody_internal_db__hr_tick_user_tab__reg_daily_s0_live htu on htu.tick_id = ht.id
                LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = htu.uid and sm.grass_date = 'current'
                LEFT JOIN shopeefood.foody_internal_db__hr_tick_order_tab__reg_daily_s0_live tot ON (ht.id = tot.tick_id)
)
WHERE 1 = 1 
AND title in 
('[Fraud] Gian lận định vị GPS',
'[Fraud] Gian lận phí ship',
'[Fraud] Share app',
'[Fraud] Gian lận cài đặt App đối thủ',
'Vi phạm nội quy: Vi phạm nghiêm trọng',
'[Fraud] Gian lận thu nhập Driver HUB',
'[Fraud] Tạo đơn hàng rủi ro',
'[Fraud] Gian lận chiết khấu Quán',
'[Fraud] Gian lận phí gửi xe',
'Vi phạm nội quy: Thái độ với Khách hàng',
'Vi phạm nội quy: Thái độ với Admin',
'Vi phạm nội quy: Thái độ với Quán',
'Vi phạm nội quy: Làm phiền Khách sau khi hoàn thành đơn',
'Vi phạm nội quy: Gây mất trật tự công cộng',
'[Fraud] Sai quy trình khi Return (NS) - Gian lận',
'[Fraud] Gian lận giá trị chiết khấu/phí dịch vụ/phí ship',
'Vi phạm nội quy: Hoàn thành đơn trước khi giao - Gian lận',
'[Fraud] Sai quy trình đã đến điểm lấy',
'[Fraud] Sai quy trình khi Complete',
'[Fraud] Sai quy trình đã đến điểm giao',
'[Fraud] Sai quy trình khi Pick',
'[Fraud] Sai quy trình khi Return (NS)',
'Vi phạm nội quy: Đồng phục',
'Vi phạm nội quy: Trách nhiệm đơn hàng',
'Vi phạm nội quy: Từ chối đơn trên 5 phút',
'Vi phạm nội quy: Từ chối đơn sai lý do',
'Vi phạm nội quy: Tự ý yêu cầu Khách hủy đơn',
'Vi phạm nội quy: Mua sai Quán',
'Vi phạm nội quy: Giao thiếu món',
'Vi phạm nội quy: Chỉnh sửa đơn hàng',
'Vi phạm nội quy: Giao đơn đổ bể',
'Vi phạm nội quy: Hoàn thành đơn trước khi giao',
'Vi phạm nội quy: Không giao hàng tận cửa',
'Vi phạm nội quy: Thu sai tiền trên đơn',
'Vi phạm nội quy: Không liên hệ được Tài xế',
'Vi phạm nội quy: Chở người và các vật dụng không liên quan',
'Vi phạm nôi quy: Sai quy trình Quán làm món lâu')

GROUP BY 1,2
)
SELECT * 

FROM
(select
      driver_id_curent
     ,full_name
     ,id_card_current
     ,id_place_current
    --  ,quit_work_date 
    --  ,map(array['driver_id','onboard_date','quit_work_date','quit_work_reason'],array[driver_id,onboard_date,quit_work_date,quit_work_reason]) as mapping_data   
     ,map(array['driver_id','onboard_date'],array[driver_id,onboard_date]) as onboard_data   
     ,map(array['quit_work_date','quit_work_reason'],array[quit_work_date,quit_work_reason]) quit_work_data
     ,map(array['id_card_check','id_place_check'],array[id_card_check,id_place_check]) identify_card_info
     ,map(array['summary_ticket'],array[summary_ticket_title]) summary_ticket

from 
(select 
        a.full_name
       ,a.driver_id_curent 
       ,a.id_card_current
       ,a.id_place_current
       ,array_agg(distinct coalesce(cast(date(from_unixtime(quit.create_time - 3600)) as varchar),null)) as quit_work_date  
       ,array_agg(distinct coalesce(q_name.name_en,null)) as quit_work_reason         
       ,array_agg(distinct cast(a.driver_id as varchar)) as driver_id 
       ,array_agg(distinct cast(a.onboard_date as varchar)) as onboard_date
       ,array_agg(distinct cast(check.national_id_number as varchar)) as id_card_check
       ,array_agg(distinct cast(check.national_id_issue_place as varchar)) as id_place_check
       ,array_agg(distinct cast(tc.title||cast(tc.total_case as varchar) as varchar)) as summary_ticket_title
    --    ,array_agg(distinct cast(tc.total_case as varchar)) as summary_ticket_case
    --    ,map(array['driver_id','onboard_date'],array[cast(driver_id as varchar),cast(onboard_date as varchar)]) as mapping_data 
    --    ,map_agg(cast(id_driver as varchar),cast(onboard_date as varchar)) as driverid_map_onboard 
    --    ,array_join(array_agg(distinct is_fresh),',') as checking_driver_profile 

from final_filter a        

LEFT JOIN ticket_check tc on tc.shipper_id = a.driver_id
LEFT JOIN shopeefood.foody_internal_db__shipper_quit_request_tab__reg_daily_s0_live quit on quit.uid = a.driver_id 

LEFT JOIN shopeefood.foody_internal_db__shipper_quit_request_reason_tab__reg_daily_s0_live q_name on q_name.id = quit.reason_id
LEFT JOIN shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live check on check.uid = a.driver_id

group by 1,2,3,4
)
)
WHERE 1 = 1 
AND LENGTH(onboard_data['onboard_date'][1]) > 0  
;
