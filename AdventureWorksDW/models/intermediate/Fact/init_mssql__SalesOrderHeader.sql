with stag_data as (
    select 
        *
    from
        {{ ref('stg_mssql__SalesOrderHeader')}}
)

, Dim_Customer as (
    select
        *
    from
        {{ ref('Dim_Customer') }}
)

, Dim_Address as (
    select
        *
    from
        {{ ref('Dim_Address') }}
)

, joined as (
    select
        a.SalesOrderKey
        , a.SalesOrderID
        , convert(varchar, a.OrderDate, 112) as OrderDateKey
        , convert(varchar, a.DueDate, 112) as DueDateKey
        , convert(varchar, a.ShipDate, 112) as ShipDateKey
        , b.CustomerKey as CustomerKey
        , c.AddressKey as ShipToAddressKey
        , d.AddressKey as BillToAddressKey
        , a.Status as Status_DD
        , a.ShipMethod as ShipMethod_DD
        , a.OnlineOrderFlag as OnlineOrderFlag_DD
        , sum(a.SubTotal) as SubTotal
        , sum(a.TaxAmt) as TaxAmt
        , sum(a.Freight) as Freight
        , sum(a.TotalDue) as TotalDue
    from
        stag_data as a
    left join
        Dim_Customer as b on a.CustomerID = b.CustomerID
    left join
        Dim_Address as c on a.BillToAddressID = c.AddressID
    left join
        Dim_Address as d on a.ShipToAddressID = d.AddressID
    group by
         a.SalesOrderKey
        , a.SalesOrderID
        , convert(varchar, a.OrderDate, 112)
        , convert(varchar, a.DueDate, 112)
        , convert(varchar, a.ShipDate, 112)
        , b.CustomerKey
        , c.AddressKey
        , d.AddressKey
        , a.Status
        , a.ShipMethod
        , a.OnlineOrderFlag
)

select * from joined