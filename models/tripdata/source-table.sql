with source_table as (select * from {{source('tripdata','green_tripdata_2021-01')}}
union all
select * from {{source('tripdata','green_tripdata_2021-02')}}
union all
select * from {{source('tripdata','green_tripdata_2021-03')}}
union all
select * from {{source('tripdata','green_tripdata_2021-04')}}
union all
select * from {{source('tripdata','green_tripdata_2021-05')}}
union all
select * from {{source('tripdata','green_tripdata_2021-06')}}
union all
select * from {{source('tripdata','green_tripdata_2021-07')}}
union all
select * from {{source('tripdata','green_tripdata_2021-08')}}
union all
select * from {{source('tripdata','green_tripdata_2021-09')}}
union all
select * from {{source('tripdata','green_tripdata_2021-10')}}
union all
select * from {{source('tripdata','green_tripdata_2021-11')}}
union all
select * from {{source('tripdata','green_tripdata_2021-12')}})

select * from source_table



