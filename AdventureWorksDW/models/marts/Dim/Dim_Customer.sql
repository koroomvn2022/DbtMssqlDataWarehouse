{{
    config(
        materialized='view'
    )
}}

select
    dbt_scd_id as CustomerKey
    , CustomerID
    , NameStyle
    , Title
    , FirstName
    , MiddleName
    , LastName
    , FullName
    , Suffix
    , CompanyName
    , SalesPerson
    , EmailAddress
    , Phone
    , dbt_valid_from as RowEffectiveDate
    , dbt_valid_to as RowExpirationDate
    , iif(dbt_valid_to is null, 'Current', 'Expired') as CurrentRowIndicator 
from
    {{ ref('SNSH_Dim_Customer') }}