select count(distinct case when a.is_active_flag = 1 then a.merchant_id else null end) nb_mex
    , count(distinct case when a.is_active_flag = 1 and b.parking_fee > 0 then a.merchant_id else null end) nb_mex_having_pf
from shopeefood.foody_mart__profile_merchant_master a 
left join shopeefood.foody_merchant_db__delivery_tab__reg_daily_s0_live b on a.merchant_id = b.restaurant_id
where a.now_service_category_id = 1
and a.city_id not in (0,238,468,469,470,471,472)
and a.grass_date = 'current'
