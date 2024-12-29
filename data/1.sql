select
 area
click_product_id,
product_name,
city_name,
count(*)
from
(select
     click_product_id,
     city_id
 from user_visit_action
 where click_product_id != -1
 ) a
 join (
 select
    product_id,
    product_name
 from product_info
 ) p on a.click_product_id = p.product_id
 join (
 select
    city_id,
    city_name,
    area
 from city_info
 )c on a.city_id = c.city_id
 limit 10