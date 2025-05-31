with monthly_performance as (select  EXTRACT(MONTH FROM DATE(lpep_pickup_datetime))  AS month,
        round(count(trip_distance),2)as total_pickup,
        round(sum(trip_distance),2) as total_trip_distance,
        round(avg(trip_distance),2) as average_trip_distance,
        sum(passenger_count) as total_passenger,
        round(avg(passenger_count),2) as average_passenger,
        round(sum (total_amount),2) as total_amount,
        round(avg(total_amount),2) as average_amount
from {{ref('source-table')}}
where passenger_count >=1 and EXTRACT(YEAR FROM DATE(lpep_pickup_datetime))>= 2021
group by month
order by month asc)
select * from monthly_performance
