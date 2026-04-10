with source as (

    select * from {{ source('hospital_raw', 'procedure_costs') }}

),

renamed as (

    select
        procedure_code,
        procedure_description,
        procedure_base_cost,
        cost_category

    from source

)

select * from renamed