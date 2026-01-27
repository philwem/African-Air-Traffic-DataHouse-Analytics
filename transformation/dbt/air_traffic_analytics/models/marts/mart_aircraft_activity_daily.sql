with d as (
    select *
    from {{ ref('int_flight_positions_daily') }}
)

select
    flight_date,
    aircraft_icao,
    position_reports
from d
order by 1 desc, 3 desc
