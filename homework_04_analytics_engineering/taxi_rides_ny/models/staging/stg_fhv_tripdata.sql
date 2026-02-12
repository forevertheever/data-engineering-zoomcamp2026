with source as (
    select * from {{ source('raw', 'fhv_tripdata') }} -- the name of table is defined in the load_data.py while ingesting data into the warehouse
),

renamed as (
    select
        -- identifiers
        cast(dispatching_base_num as string) as dispatching_base_num,

        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,  -- lpep = Licensed Passenger Enhancement Program (green taxis)
        cast(dropOff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,
        cast(SR_Flag as string) as sr_flag,
        cast(Affiliated_base_number as string) as affiliated_base_number

    from source
    -- Filter out records with null dispatching_base_num (data quality requirement)
    where dispatching_base_num is not null
)

select * from renamed