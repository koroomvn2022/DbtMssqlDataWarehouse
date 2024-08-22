with raw_data as (
    select
        SalesOrderID
        , isnull(RevisionNumber, -1) as RevisionNumber
        , cast(isnull(OrderDate, '1900-01-01') as date) as OrderDate
        , cast(isnull(DueDate, '1900-01-01') as date) as DueDate
        , cast(isnull(ShipDate, '1900-01-01') as date) as ShipDate
        , case Status
            when 1 then 'In process' 
            when 2 then 'Approved' 
            when 3 then 'Backordered' 
            when 4 then 'Rejected' 
            when 5 then 'Shipped' 
            when 6 then 'Canceled'
            else 'Unknown'
        end as Status
        , iif(OnlineOrderFlag = 1, 'Online Order', 'Offline Order') as OnlineOrderFlag
        , isnull(CustomerID, -1) as CustomerID
        , isnull(ShipToAddressID, -1) as ShipToAddressID
        , isnull(BillToAddressID, -1) as BillToAddressID
        , isnull(ShipMethod, 'Unknown') as ShipMethod
        , isnull(SubTotal, -1) as SubTotal
        , isnull(TaxAmt, -1) as TaxAmt
        , isnull(Freight, -1) as Freight
        , isnull(TotalDue, -1) as TotalDue
    from 
        {{ source('raw_data', 'SalesOrderHeader') }}
)

, hashed as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'SalesOrderID'
            , 'OrderDate'
            , 'DueDate'
            , 'ShipDate'
            , 'Status'
            , 'OnlineOrderFlag'
            , 'CustomerID'
            , 'ShipToAddressID'
            , 'BillToAddressID'
            , 'ShipMethod'
        ])}} as SalesOrderKey
        , *
    from
        raw_data
)

select * from hashed