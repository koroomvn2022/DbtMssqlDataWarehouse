with stag_data as (
    select
        OrderDate as Date
    from
        {{ ref('stg_mssql__SalesOrderHeader') }}

    union

    select
        DueDate
    from
        {{ ref('stg_mssql__SalesOrderHeader') }}

    union

    select
        ShipDate
    from
        {{ ref('stg_mssql__SalesOrderHeader') }}
)
, with_additional_column as (
    select
        convert(varchar(8), Date, 112) as DateKey
        , Date
        , convert(varchar, Date, 0) as FullDateDescription
        , datename(dw, Date) as DayofWeek
        , month(date) as Month
        , datepart(q, date) as Quarter
        , year(date) as Year
        , iif(datename(dw, Date) not in ('Saturday', 'Sunday'), 'Weekday', 'Weekend') as WeekdayIndicator
    from
        stag_data
)

select * from with_additional_column