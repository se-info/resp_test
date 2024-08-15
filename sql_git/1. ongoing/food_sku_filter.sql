select * 
from
(select  store_id,merchant_name,array_agg(distinct final_cate_l2_name) as dish_agg
from shopeefood_search.foody_search_dish_category_prediction_result_vn 
where 1 = 1 
and regexp_like(final_cate_l2_name,'Bánh kem|Pizza|Món Nhật|Kem|Món Hàn') = true 
group by 1,2)