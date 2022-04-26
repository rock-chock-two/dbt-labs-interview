with 

source as (

    select * from {{ source('interview', 'payments') }}

),

base as (

    select 
        payment_id,
        created_at,
        order_id,
        status as payment_status,
        if(
            status in ('paid', 'completed','shipped'),
            'paid',
            status
        ) as payment_status_category,
        
        payment_type,
        
        round(
            if(payment_type = 'coupon', 0, amount_cents)
            , 2
        ) as amount_cents,
        round(amount_shipping_cents, 2) as shipping_amount_cents,
        round(tax_amount_cents, 2) as tax_amount_cents,

        if(payment_type = 'coupon', amount_cents, 0) as discount_amount_cents
    
    from source

)

select * from base