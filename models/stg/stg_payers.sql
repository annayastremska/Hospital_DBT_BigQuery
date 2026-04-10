with source as (

    select * from {{ source('hospital_raw', 'payers') }}

),

renamed as (

    select
        id                      as payer_id,
        name                    as payer_name,
        address,
        city,
        state_headquartered     as state,
        zip,
        phone

    from source

)

select * from renamed