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

where 

     ({{ date_trunc("year" , "pickup_datetime") }} = '2019-01-01') 
    --  and
    -- (
    --     dispatching_base_num is not null and
    --     DOLocationID  is not null and  
    --     PULocationID is not null         and  
    --     SR_Flag is not null and
    --     Affiliated_base_number is not null
    -- )



-- {% if var('is_test_run', default=true) %}
--     {{ limit_number_return(var('test_run_limit', 100))}}
-- {% endif %}



