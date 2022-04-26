{% set amount_types = ["amount_cents", "tax_amount_cents", "shipping_amount_cents"] %}

with 

base as (

    select * from {{ ref('stg_interview__payments') }}

),

grouped as (

  select
    order_id,

    count(payment_id) as payments,
    sum(
        if(
            payment_status_category = 'paid', 
            1, 
            0
        )
    ) as completed_payments,

    round(sum(discount_amount_cents), 2) as discount_amount_cents,

    -- use jinja to remove repetitive code
    {%- for amount_type in amount_types %}    

        round(
            sum(if(payment_status_category = 'paid', {{amount_type}}, 0)),
            2
        ) as paid_{{amount_type}},
    
    {%- endfor %}


  from base
  group by 1

),

final as (

    select 
        *,

        round(
            paid_amount_cents + paid_tax_amount_cents + paid_shipping_amount_cents,
            2
        ) as paid_total_amount_cents,

        round(
            if(
                (paid_amount_cents + paid_tax_amount_cents + paid_shipping_amount_cents) > 0, 
                paid_amount_cents + paid_tax_amount_cents + paid_shipping_amount_cents + discount_amount_cents, 
                0
            ),
            2
        ) as paid_total_amount_plus_discount_cents

        
    from grouped
)

select * from final
