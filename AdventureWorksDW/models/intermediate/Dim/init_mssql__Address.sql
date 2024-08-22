with stag_data as(
    select
        *
    from
        {{ ref('stg_mssql__Address') }}
)

select * from stag_data