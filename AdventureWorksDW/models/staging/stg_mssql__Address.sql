with raw_data as (
    select
        AddressID
        , isnull(AddressLine1, 'Unknown') as AddressLine1
        , isnull(AddressLine2, 'Unknown') as AddressLine2
        , isnull(City, 'Unknown') as City
        , isnull(StateProvince, 'Unknown') as StateProvince
        , isnull(CountryRegion , 'Unknown') as CountryRegion
        , isnull(PostalCode, 'Unknown') as PostalCode
    from 
        {{ source('raw_data', 'Address') }}
)

, default_record as (
    select
        -1 as AddressID
        , 'Unknown' as AddressLine1
        , 'Unknown' as AddressLine2
        , 'Unknown' as City
        , 'Unknown' as StateProvince
        , 'Unknown' as CountryRegion
        , 'Unknown' as PostalCode
)
, with_default_record as (
    select * from raw_data
    union all
    select * from default_record
)


select * from with_default_record