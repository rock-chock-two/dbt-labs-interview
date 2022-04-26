{% set amounts = [
    "paid_amount",
    "paid_tax_amount",
    "paid_shipping_amount",
    "discount_amount",
    "paid_total_amount",
    "paid_total_amount_plus_discount"
    ] 
%}

with 

stg_interview__orders as (

    select * from {{ ref('stg_interview__orders') }}

),

stg_interview__addresses as (

    select * from {{ ref('stg_interview__addresses') }}

),

devices__filtered as (

    select * from {{ ref('devices__filtered') }}

),

orders__filtered as (

    select * from {{ ref('orders__filtered') }}

),

payments__grouped as (

    select * from {{ ref('payments__grouped') }}

),

final as (

    select
        stg_interview__orders.order_id,

        stg_interview__orders.user_id,
        if(
            orders__filtered.first_order_id = stg_interview__orders.order_id,
            'new',
            'repeat'
        ) as user_type,
        devices__filtered.device_type as purchase_device_type,
        devices__filtered.device as purchase_device,

        stg_interview__orders.created_at,
        stg_interview__orders.updated_at,
        stg_interview__orders.shipped_at,
        stg_interview__orders.shipping_method,

        stg_interview__orders.order_status,
        stg_interview__orders.order_status_category,

        stg_interview__orders.currency,
        stg_interview__addresses.country_type,

        payments__grouped.payments,
        payments__grouped.completed_payments,

        round(stg_interview__orders.order_total_amount_cents / 100, 2) as order_total_amount,

        -- Use jinja to convert cents to dollars via looping through the anount fields from payments__grouped model

        {%- for amount in amounts %}

            round(
                payments__grouped.{{amount}}_cents / 100,
                2
            ) as {{amount}}

            {%- if not loop.last %},{% endif -%}
        {%- endfor %}
        
    from stg_interview__orders
    left join devices__filtered
        on stg_interview__orders.order_id = devices__filtered.order_id
    left join orders__filtered
        on stg_interview__orders.user_id = orders__filtered.user_id
    left join stg_interview__addresses
        on stg_interview__orders.order_id = stg_interview__addresses.order_id
    left join payments__grouped
        on stg_interview__orders.order_id = payments__grouped.order_id

)

select * from final


