with raw_data as (
    select
        isnull(ProductModelID, -1) as ProductModelID
        , isnull(ProductDescriptionID, -1) as ProductDescriptionID
        , isnull(Culture, 'Unknown') as Culture
    from 
        {{ source('raw_data', 'ProductModelProductDescription') }}
)

select * from raw_data