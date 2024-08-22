{{
    config(
        materialized='view'
    )
}}

select
    dbt_scd_id as ProductKey
    , ProductID
    , ProductName
    , ProductNumber
    , Color
    , StandardCost
    , ListPrice
    , Size
    , Weight
    , SellStartDate
    , SellEndDate
    , DiscontinuedDate
    , ProductModelKey
    , ProductCategoryName
    , ParentProductCategoryName
    , dbt_valid_from as RowEffectiveDate
    , dbt_valid_to as RowExpirationDate
    , iif(dbt_valid_to is null, 'Current', 'Expired') as CurrentRowIndicator 
from
    {{ ref('SNSH_Dim_Product') }}