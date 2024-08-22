with Dim_ProductDescription as (
    select 
        *
    from
        {{ ref('Dim_ProductDescription')}}
)
, Dim_ProductModel as (
    select 
        *
    from
        {{ ref('Dim_ProductModel')}}
)
, stag_data as(
    select
        b.ProductDescriptionKey
        , c.ProductModelKey
        , a.*
    from
        {{ ref('stg_mssql__ProductModelProductDescription')}} as a
    left join
        Dim_ProductDescription as b on a.ProductDescriptionID = b.ProductDescriptionID
    left join
        Dim_ProductModel as c on a.ProductModelID = c.ProductModelID
)

select * from stag_data