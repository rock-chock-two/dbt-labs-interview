with 

base as (

    select
        order_id,
        user_id,
        created_at,
        updated_at,
        shipped_at,
        currency,
        status as order_status,

        if(
            status in ('paid', 'completed','shipped'),
            'completed',
            status
        ) as order_status_category,
        
        shipping_method,
        amount_total_cents

    from 
        `dbt-public.interview_task.orders`  

)

select * from base