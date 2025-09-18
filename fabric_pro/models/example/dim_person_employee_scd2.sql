{{
  config(
    materialized='incremental',
    unique_key='SurrogateKey',

    schema='dbo',
    alias='DimPersonEmployee'
  )
}}

with source_data as (
    select * from {{ ref('load_stg_person_employee') }}
),

-- handle first run (when Dim table doesn't exist)
{% if is_incremental() %}
existing_dim_records as (
    select * from {{ this }} where IsCurrent = 1
)
{% else %}
existing_dim_records as (
    select 
        cast(null as varchar(100)) as FirstName,
        cast(null as varchar(100)) as LastName,
        cast(null as varchar(100)) as JobTitle,
        cast(null as varchar(32))  as SurrogateKey,
        cast(null as datetime2(6)) as ValidFrom,
        cast(null as datetime2(6)) as ValidTo,
        cast(null as bit) as IsCurrent
    where 1=0
)
{% endif %},

-- detect new / changed
new_and_changed_records as (
    select 
        sd.FirstName,
        sd.LastName,
        sd.JobTitle
    from source_data sd
    left join existing_dim_records edr
      on sd.FirstName = edr.FirstName 
     and sd.LastName  = edr.LastName
    where edr.FirstName is null        -- brand new person
       or (edr.JobTitle != sd.JobTitle) -- changed job
),

-- prepare new versions
new_records_prep as (
    select 
        CONVERT(VARCHAR(32), HASHBYTES('MD5', 
            cast(FirstName as varchar(100)) +
            cast(LastName  as varchar(100)) +
            convert(varchar(30), getdate(), 121)
        ), 2) as SurrogateKey,
        FirstName,
        LastName,
        JobTitle,
        cast(getdate() as datetime2(6)) as ValidFrom,
        cast('9999-12-31 23:59:59.999999' as datetime2(6)) as ValidTo,  -- ✅ full precision cast
        1 as IsCurrent
    from new_and_changed_records
),

-- end-date old versions
records_to_end_date as (
    select 
        edr.SurrogateKey,
        edr.FirstName,
        edr.LastName,
        edr.JobTitle,
        edr.ValidFrom,
        cast(getdate() as datetime2(6)) as ValidTo,   -- ✅ casted
        0 as IsCurrent
    from existing_dim_records edr
    join new_and_changed_records ncr
      on edr.FirstName = ncr.FirstName 
     and edr.LastName  = ncr.LastName
    where edr.JobTitle != ncr.JobTitle 
      and edr.IsCurrent = 1
),

combined_records as (
    select * from new_records_prep
    union all
    select * from records_to_end_date
),

unchanged_current_records as (
    select * from existing_dim_records
    where IsCurrent = 1 
      and not exists (
        select 1 from new_and_changed_records ncr
         where ncr.FirstName = existing_dim_records.FirstName
           and ncr.LastName  = existing_dim_records.LastName
    )
),

{% if is_incremental() %}
historical_records as (
    select * from {{ this }} where IsCurrent = 0
)
{% else %}
historical_records as (
    select 
        cast(null as varchar(32))     as SurrogateKey,
        cast(null as varchar(100))    as FirstName,
        cast(null as varchar(100))    as LastName,
        cast(null as varchar(100))    as JobTitle,
        cast(null as datetime2(6))    as ValidFrom,
        cast(null as datetime2(6))    as ValidTo,
        cast(null as bit)             as IsCurrent
    where 1=0
)
{% endif %}

select * from combined_records
union all
select * from unchanged_current_records
union all
select * from historical_records