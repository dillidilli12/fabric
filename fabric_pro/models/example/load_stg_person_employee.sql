{{
  config(
    materialized='table',
    schema='dbo',
    alias='StgPersonEmployee'
  )
}}

select 
  FirstName,
  LastName,
  JobTitle    -- rename for consistency
from {{ source('raw_data', 'person_employee') }}