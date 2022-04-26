with 

source as (

    select * from {{ source('interview', 'payments') }}

),

base as (

    select 
        *,
        status as payment_status 
    
    from source

)

select * from base
