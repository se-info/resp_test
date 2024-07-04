select 
         hour(a.start_time) as hour_
        ,a.* 
        ,b.total_driver_online_overall
        ,b.total_driver_working_overall
        ,coalesce(b.total_driver_online_hub,0) total_driver_online_hub
        ,coalesce(b.total_driver_working_hub,0) total_driver_working_hub
        ,b.total_driver_online_non_hub
        ,b.total_driver_working_non_hub
        ,c.total_ongoing_order
        ,c.total_cancel_ongoing_order



from dev_vnfdbi_opsndrivers.phong_assign_type_by_hour a 

LEFT JOIN dev_vnfdbi_opsndrivers.phong_online_working_by_hour b on b.date_ = a.date_ 
                                                                and b.start_time = a.start_time
                                                                and b.end_time = a.end_time
                                                                and b.city_group = a.city_group    

LEFT JOIN dev_vnfdbi_opsndrivers.phong_on_going_orders_by_hour c on c.date_ = a.date_ 
                                                                and c.current_vn_datetime_from = a.start_time
                                                                and c.current_vn_datetime_to = a.end_time
                                                                and c.city_group = a.city_group 
