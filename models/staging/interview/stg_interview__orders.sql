with 

source as (

    select * from {{ source('interview', 'orders') }}

),

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
            'paid',
            status
        ) as order_status_category,
        
        shipping_method,
        round(amount_total_cents, 2) as order_total_amount_cents

    from source

)

select * from base 