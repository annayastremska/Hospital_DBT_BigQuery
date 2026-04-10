with source as (

    select * from {{ source('hospital_raw', 'medicines') }}

),

renamed as (

    select
        medicine_id,
        medicine_name,
        category,
        route,
        unit_cost

    from source

)

select * from renamed