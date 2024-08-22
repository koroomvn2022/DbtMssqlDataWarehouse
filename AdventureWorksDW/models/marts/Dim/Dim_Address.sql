{{
    config(
        materialized='view'
    )
}}

select
    dbt_scd_id as AddressKey
    , AddressID
    , AddressLine1
    , AddressLine2
    , City
    , StateProvince
    , CountryRegion
    , PostalCode
    , dbt_valid_from as RowEffectiveDate
    , dbt_valid_to as RowExpirationDate
    , iif(dbt_valid_to is null, 'Current', 'Expired') as CurrentRowIndicator 
from
    {{ ref('SNSH_Dim_Address') }}