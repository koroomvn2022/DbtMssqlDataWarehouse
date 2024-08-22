with Dim_Address as (
    select 
        *
    from
        {{ ref('Dim_Address') }}
)

, Dim_Customer as (
    select
        *
    from
        {{ ref('Dim_Customer') }}
)

, stag_data as (
    select
        a.AddressKey
        , c.CustomerKey
        , ca.*
    from
        {{ ref('stg_mssql__CustomerAddress') }} as ca
    left join
        Dim_Address as a on ca.AddressID = a.AddressID
    left join
        Dim_Customer as c on ca.CustomerID = c.CustomerID
)

select * from stag_data