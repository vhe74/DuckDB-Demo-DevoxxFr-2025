
select  start_station_id as station_id,
        start_station_name as station_name,
        avg(start_lat) as station_latitude,
        avg(start_lng) as station_longitude,
        count(*) as trip_count,
from {{ source('bike', 'trips_view') }}
where station_id is not null
    and station_name is not null
GROUP BY station_id, station_name

