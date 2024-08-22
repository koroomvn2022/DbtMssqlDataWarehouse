
with xmlnamespaces (
    'http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/ProductModelDescription' AS p1
    , 'http://www.w3.org/1999/xhtml' AS html
)

, raw_data as (
    select
        ProductModelID
        , isnull(Name, 'Unknown') as ProductModelName
        , isnull(
            CatalogDescription.value('(/p1:ProductDescription/p1:Summary/html:p)[1]', 'nvarchar(max)')
            , 'Unknown'
        ) as CatalogDescription
    from 
        {{ source('raw_data', 'ProductModel') }}
)
, default_record as (
    select
        -1 as ProductModelID
        , 'Unknown' as ProductModelName
        , 'Unknown' as CatalogDescription
)

, with_default_record as (
    select * from raw_data
    union all
    select * from default_record
)


select * from with_default_record