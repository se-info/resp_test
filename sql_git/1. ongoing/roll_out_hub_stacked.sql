with phase_raw as 
(select ARRAY['HCM_ Go Vap C','HCM_ Go Vap D','HCM_ Binh Thanh A','HCM_Q2 B','HCM_ Binh Thanh B','HCM_ Go Vap A','HCM_ Go Vap B','HCM_Q12 B','HCM_Q12 A','HCM_Q12 C','HCM_ Binh Thanh C','HCM_ Phú Nhuận','HCM_ Q11','HCM_Tan Binh B','Bình Thạnh X','Gò Vấp X','Bình Thạnh Y','Phú Nhuận X','Tân Bình X','Tân Bình Y'] as phase1
)
,hcm_phase2 as
(select ARRAY['HCM_Q5','HCM_Q6 A','Quận 6 X','Tân Phú X','HCM_Q8 A','HCM_Q8 B','HCM_Binh Tan D','HCM_Tan Binh A','HCM_Tan Phu C','HCM_ Tan Phu A','HCM_Binh Tan F','HCM_ Tan Phu B','HCM_Binh Tan E','HCM_Binh Tan A','HCM_Binh Tan B','HCM_Q6 B'] as phase2)
,hn_phase2 as 
(select ARRAY['Tay Ho C','Bac Tu Liem B','Bac Tu Liem D','Long Bien A','Long Biên B','Long Biên C','Cau Giay A','Cau Giay B','Nam Tu Liem B','Nam Từ Liêm A'] as phase2)
select * from
(select 
        hm.uid,
        sp.shopee_uid,
        sm.shipper_name,
        sm.city_name,
        sm.shipper_status_code,
        array_join(array_agg(hi.hub_name),',') as hub_name_list,
        array_join(array_intersect(hn_phase2.phase2,array_agg(hi.hub_name)),',') as checked,
        if(cardinality(array_intersect(hn_phase2.phase2,array_agg(hi.hub_name))) > 0,1,0) as is_phase2





from shopeefood.foody_internal_db__shipper_hub_mapping_tab__reg_continuous_s0_live hm 

left join shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live hi on hm.hub_id = hi.id 

left join shopeefood.foody_internal_db__shipper_profile_tab__reg_continuous_s0_live sp on sp.uid = hm.uid

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = hm.uid and sm.grass_date = 'current'

cross join hn_phase2 

where 1 = 1 
and sm.shipper_status_code = 1 
-- and sm.shipper_type_id = 12 
-- and sm.city_id = 220
group by 1,2,3,4,5,hn_phase2.phase2
)
where 1 = 1 
-- and is_phase2 = 1 