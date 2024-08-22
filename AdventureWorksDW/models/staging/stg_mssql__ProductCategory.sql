with raw_data as (
    select
        ProductCategoryID
        , isnull(ParentProductCategoryID, -1) as ParentProductCategoryID
        , isnull(Name, 'Unknown') as ProductCategoryName
    from 
        {{ source('raw_data', 'ProductCategory') }}
)

, default_record as (
    select
        -1 as ProductCategoryID
        , -1 as ParentProductCategoryID
        , 'Unknown' as ProductCategoryName
)

, with_default_record as (
    select * from raw_data
    union all
    select * from default_record
)

select * from with_default_record