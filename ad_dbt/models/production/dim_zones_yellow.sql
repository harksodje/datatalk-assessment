{{ config(materialized='table') }}

select 
    locationID as locationid,
    borough as borough,
    zone as zone,
    replace(service_zone, 'Boro', 'Yellow') as service_zone
from {{ ref( 'taxi_zone_lookup' ) }} 