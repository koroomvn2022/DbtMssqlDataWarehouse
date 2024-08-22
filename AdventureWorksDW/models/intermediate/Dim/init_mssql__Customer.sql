with stag_data as(
    select
        CustomerID
        , NameStyle
        , Title
        , FirstName
        , MiddleName
        , LastName
        , iif(FirstName = 'Unknown', '', FirstName) 
            + iif(MiddleName = 'Unknown', '', ' ' + MiddleName)
            + iif(LastName = 'Unknown', '', ' ' + LastName) as FullName
        , Suffix
        , CompanyName
        , SalesPerson
        , EmailAddress
        , Phone
    from
        {{ ref('stg_mssql__Customer') }}
)

select * from stag_data