with raw_data as (
    select
        CustomerID
        , isnull(NameStyle, 0) as NameStyle
        , isnull(Title, 'Unknown') as Title
        , isnull(FirstName, 'Unknown') as FirstName
        , isnull(MiddleName, 'Unknown') as MiddleName
        , isnull(LastName, 'Unknown') as LastName
        , isnull(Suffix, 'Unknown') as Suffix
        , isnull(CompanyName, 'Unknown') as CompanyName
        , isnull(SalesPerson, 'Unknown') as SalesPerson
        , isnull(EmailAddress, 'Unknown') as EmailAddress
        , isnull(Phone, 'Unknown') as Phone
    from 
        {{ source('raw_data', 'Customer') }}
)

, default_record as (
    select
        -1  as CustomerID
        , 0 as NameStyle
        , 'Unknown' as Title
        , 'Unknown' as FirstName
        , 'Unknown' as MiddleName
        , 'Unknown' as LastName
        , 'Unknown' as Suffix
        , 'Unknown' as CompanyName
        , 'Unknown' as SalesPerson
        , 'Unknown' as EmailAddress
        , 'Unknown' as Phone
)
, with_default_record as (
    select * from raw_data
    union all
    select * from default_record
)

select * from with_default_record