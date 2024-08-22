with stag_data as (
    select
        *
    from 
        {{ ref('stg_mssql__SalesOrderDetail') }}
)

, stag_SalesOrderHeader as (
    select
        *
    from 
        {{ ref('stg_mssql__SalesOrderHeader') }}
)

, Dim_Product as (
    select
        *
    from
        {{ ref('Dim_Product') }}
)

, joined as(
    select
        a.SalesOrderDetailKey
        , a.SalesOrderDetailID
        , b.SalesOrderKey
        , convert(varchar(8), a.OrderDate, 112) as OrderDateKey
        , convert(varchar(8), a.DueDate, 112) as DueDateKey
        , convert(varchar(8), a.ShipDate, 112) as ShipDateKey
        , c.ProductKey
        , a.OrderQty
        , a.UnitPrice
        , a.UnitPriceDiscount
        , a.LineTotal
    from
        stag_data as a
    left join
        stag_SalesOrderHeader as b on a.SalesOrderID = b.SalesOrderID
    left join
        Dim_Product as c on a.ProductID = c.ProductID
)

select * from joined