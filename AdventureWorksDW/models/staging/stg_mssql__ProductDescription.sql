with raw_data as (
    select
        ProductDescriptionID
        , isnull(Description, 'Unknown') as Description
    from 
        {{ source('raw_data', 'ProductDescription') }}
)

, default_record as (
    select
        -1 as ProductDescriptionID
        , 'Unknown' as Description
)


, with_default_record as(
    select * from raw_data
    union all
    select * from default_record
)


select * from with_default_record