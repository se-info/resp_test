select project_name,*

from data_metamart.dws_dashboard_usage_1d_vn_view

where project_name like 'vnfdbi%'
and date(grass_date) = date(current_date - interval '1' day )
and owner = 'duphong.hua@foody.vn'
order by 1, 3,2