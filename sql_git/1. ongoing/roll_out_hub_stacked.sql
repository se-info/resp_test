with phase_raw as 
(select ARRAY['HCM_ Go Vap C','HCM_ Go Vap D','HCM_ Binh Thanh A','HCM_Q2 B','HCM_ Binh Thanh B','HCM_ Go Vap A','HCM_ Go Vap B','HCM_Q12 B','HCM_Q12 A','HCM_Q12 C','HCM_ Binh Thanh C','HCM_ Phú Nhuận','HCM_ Q11','HCM_Tan Binh B','Bình Thạnh X','Gò Vấp X','Bình Thạnh Y','Phú Nhuận X','Tân Bình X','Tân Bình Y'] as phase1
)
select * from
(select 
        hm.uid,
        sm.shipper_name,
        sm.city_name,
        sm.shipper_status_code,
        array_join(array_agg(hi.hub_name),',') as hub_name_list,
        array_join(array_intersect(phase_raw.phase1,array_agg(hi.hub_name)),',') as checked,
        if(cardinality(array_intersect(phase_raw.phase1,array_agg(hi.hub_name))) > 0,1,0) as is_phase1





from shopeefood.foody_internal_db__shipper_hub_mapping_tab__reg_continuous_s0_live hm 

left join shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live hi on hm.hub_id = hi.id 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = hm.uid and sm.grass_date = 'current'

cross join phase_raw 

where 1 = 1 
and sm.shipper_status_code = 1 
group by 1,2,3,4,phase_raw.phase1
)
where is_phase1 = 1 