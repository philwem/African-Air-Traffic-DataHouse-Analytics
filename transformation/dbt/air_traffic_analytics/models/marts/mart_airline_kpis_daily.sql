with d as (
    select *
    from {{ ref('int_flight_positions_daily') }}
)

select
    flight_date,
    operator_icao_guess as operator_icao,
    sum(position_reports) as total_position_reports,
    count(distinct aircraft_icao) as active_aircraft
from d
where operator_icao_guess is not null
group by 1,2
order by 1 desc, 3 desc


