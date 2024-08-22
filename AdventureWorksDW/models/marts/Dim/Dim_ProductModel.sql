{{
    config(
        materialized='view'
    )
}}

select
    dbt_scd_id as ProductModelKey
    , ProductModelID
    , ProductModelName
    , CatalogDescription
    , dbt_valid_from as RowEffectiveDate
    , dbt_valid_to as RowExpirationDate
    , iif(dbt_valid_to is null, 'Current', 'Expired') as CurrentRowIndicator 
from
    {{ ref('SNSH_Dim_ProductModel')}}