with fhv_data as (

    select 
        * 
    from 
        {{ ref('stg_fhv_data') }}
    where 
        {{ date_trunc("year" , "pickup_datetime") }} = '2019-01-01'
),

dim_zones as (
    select 
        locationID as locationid,
        borough as borough,
        zone as zone,
        replace(service_zone, 'Boro', 'Green') as service_zone
    from {{ ref( 'taxi_zone_lookup' ) }}
    where
        borough != 'Unknown'
)

-- select 
--     fhv_data.
select 
    fhv_data.dispatching_base_num,
    fhv_data.pickup_datetime,
    fhv_data.dropoff_datetime,
    -- fhv_data.pulocationid,
    pickup_zone.service_zone as pickup_service_zone,
    dropoff_zone.service_zone as dropoff_service_zone,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone, 
    fhv_data.pickup_locationid,
    fhv_data.dropoff_locationid,
    fhv_data.sr_flag,
    fhv_data.affiliated_base_number

from fhv_data
inner join dim_zones as pickup_zone
on fhv_data.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_data.dropoff_locationid = dropoff_zone.locationid
