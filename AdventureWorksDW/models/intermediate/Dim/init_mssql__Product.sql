with stag_data as(
    select
        *
    from
        {{ ref('stg_mssql__Product') }}
)
, stag_ProductCategory as (
    select
        *
    from
        {{ ref('stg_mssql__ProductCategory') }}
)
, Dim_ProductModel as (
    select
        *
    from
        {{ ref('Dim_ProductModel') }}
)
, joined as (
    select
        {{ dbt_utils.get_filtered_columns_in_relation(
            from=ref('stg_mssql__Product')
            , except=['ProductCategoryID', 'ProductModelID']
        ) | join(', ')}}
        , d.ProductModelKey as ProductModelKey
        , b.ProductCategoryName
        , c.ProductCategoryName as ParentProductCategoryName
    from
        stag_data as a
    left join 
        stag_ProductCategory as b on a.ProductCategoryID = b.ProductCategoryID
    left join
        stag_ProductCategory as c on b.ParentProductCategoryID = c.ProductCategoryID
    left join
        Dim_ProductModel as d on a.ProductModelID = d.ProductModelID
)

select * from joined