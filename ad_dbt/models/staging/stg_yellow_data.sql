with source_yellow 
    as 
(
    select 
        * ,
        row_number() over(partition by 
            cast(VendorID as int), cast(lpep_pickup_datetime as timestamp) ) as rn,
        'yellow' as service_type
    from
        {{ source('ny_data_taxi_source', 'yellow_taxi_data_2019_2020') }}
    where
        vendorid is not null
)

select 
    {{ dbt_utils.generate_surrogate_key (['VendorID', 'lpep_pickup_datetime']) }} as tripid,
    cast(VendorID as int) as vendorid,
    cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
    cast(PULocationID as int) as pickup_locationid,
    cast(DOLocationID as int) as dropoff_locationid,
    cast(passenger_count as int) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    cast(ratecodeID as int) as ratecodeid,
    coalesce(
        cast(payment_type as int), 0
    ) as payment_type,
    
    {{ get_payment_type_description('payment_type') }} as payment_type_description,
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(congestion_surcharge as numeric) as congestion_surcharge,
    cast(0 as numeric) as ehail_fee,
    1 as trip_type, -- 1 for yellow cab
    store_and_fwd_flag,
    service_type

from
    source_yellow

-- this may not be needed when runing full loads
-- {% if var('is_test_run', default=true) %}
--     {{ limit_number_return(var('test_run_limit', 100))}}
-- {% endif %}