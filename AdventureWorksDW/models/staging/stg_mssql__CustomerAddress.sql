with raw_data as (
    select
        isnull(CustomerID, -1) as CustomerID
        , isnull(AddressID, -1) as AddressID
        , isnull(AddressType, 'Unknown') as AddressType
    from 
        {{ source('raw_data', 'CustomerAddress') }}
)

select * from raw_data