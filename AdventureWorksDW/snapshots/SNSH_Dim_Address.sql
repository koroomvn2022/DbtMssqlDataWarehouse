{% snapshot SNSH_Dim_Address %}
    {{
        config(
            target_schema='snapshot'
            , strategy='check'
            , unique_key='AddressID'
            , check_cols='all'
        )
    }}

    select 
        * 
    from 
        {{ ref('init_mssql__Address') }}

{% endsnapshot  %}