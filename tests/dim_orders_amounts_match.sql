with 

base as (

    select * from {{ ref('dim_orders') }}

)

select 
    *   

from base  
where order_status = 'completed' and 
    (order_total_amount_cents != paid_total_amount_plus_discount_cents
    and paid_total_amount_plus_discount_cents > 0)