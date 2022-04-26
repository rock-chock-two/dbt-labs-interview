with 

base as (

    select 
        *,
        status as payment_status 
    
    from `dbt-public.interview_task.payments`

)

select * from base
