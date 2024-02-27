select 
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp ) as dropoff_datetime,
    cast(PULocationID as int) as pickup_locationid,
    cast(DOLocationID as int) as dropoff_locationid,
    cast(SR_Flag as int) as sr_flag,
    cast(Affiliated_base_number as string) affiliated_base_number

from 
    {{ source('ny_data_taxi_source', 'fhv_tripdata_2019_2020') }}



