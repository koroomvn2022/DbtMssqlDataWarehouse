with raw_data as (
    select
        ProductID
        , isnull(Name, 'Unknown') as ProductName
        , isnull(ProductNumber, 'Unknown') as ProductNumber
        , isnull(Color, 'Unknown') as Color
        , isnull(StandardCost, -1) as StandardCost
        , isnull(ListPrice, -1) as ListPrice
        , isnull(Size, -1) as Size
        , isnull(Weight, -1) as Weight
        , isnull(ProductCategoryID, -1) as ProductCategoryID
        , isnull(ProductModelID, -1) as ProductModelID
        , isnull(SellStartDate, '1900-01-01 00:00:00') as SellStartDate
        , isnull(SellEndDate, '1900-01-01 00:00:00') as SellEndDate
        , isnull(DiscontinuedDate, '1900-01-01 00:00:00') as DiscontinuedDate
    from 
        {{ source('raw_data', 'Product') }}
)

, default_record as (
    select
        -1 as ProductID
        , 'Unknown' as ProductName
        , 'Unknown' as ProductNumber
        , 'Unknown' as Color
        , -1 as StandardCost
        , -1 as ListPrice
        , '-1' as Size
        , -1 as Weight
        , -1 as ProductCategoryID
        , -1 as ProductModelID
        , '1900-01-01 00:00:00' as SellStartDate
        , '1900-01-01 00:00:00' as SellEndDate
        , '1900-01-01 00:00:00' as DiscontinuedDate
)

, with_default_record as (
    select * from raw_data
    union all
    select * from default_record
)


select * from with_default_record