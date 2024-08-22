with raw_data as (
    select
        SalesOrderDetailID
        , isnull(SalesOrderID, -1) as SalesOrderID
        , isnull(OrderQty, -1) as OrderQty
        , isnull(ProductID, -1) as ProductID
        , isnull(UnitPrice, -1) as UnitPrice
        , isnull(UnitPriceDiscount, -1) as UnitPriceDiscount
        , isnull(LineTotal, -1) as LineTotal
    from 
        {{ source('raw_data', 'SalesOrderDetail') }}
)

, SalesOrderHeader_data as (
    select
        SalesOrderID
        , cast(isnull(OrderDate, '1900-01-01') as date) as OrderDate
        , cast(isnull(DueDate, '1900-01-01') as date) as DueDate
        , cast(isnull(ShipDate, '1900-01-01') as date) as ShipDate
    from
        {{ source('raw_data', 'SalesOrderHeader') }}
)
, joined as (
    select
        {{dbt_utils.generate_surrogate_key([
            'SalesOrderDetailID'
            , 'a.SalesOrderID'
            , 'ProductID'
            , 'OrderDate'
            , 'DueDate'
            , 'ShipDate'
        ])}} as SalesOrderDetailKey
        , a.*
        , b.OrderDate
        , b.DueDate
        , b.ShipDate
    from
        raw_data as a
    left join
        SalesOrderHeader_data as b on a.SalesOrderID = b.SalesOrderID
)


select * from joined