with 

base as (

    select * from {{ ref ('stg_interview__orders') }} 

),


final as (

    select
        user_id,
        min(order_id) as first_order_id

    from base
    where order_status != 'cancelled'
    group by 1

)

select * from final