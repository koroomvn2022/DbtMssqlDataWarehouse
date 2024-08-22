with stag_data as (
    select
        *
    from
        {{ ref('stg_mssql__ProductDescription') }}
)

select * from stag_data