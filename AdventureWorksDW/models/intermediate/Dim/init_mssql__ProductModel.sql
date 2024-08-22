with stag_data as (
    select
        *
    from
        {{ ref('stg_mssql__ProductModel') }}
)

select * from stag_data