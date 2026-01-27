with fp as (
    select *
    from {{ ref('stg_flight_positions') }}
)

select
    date_trunc('day', event_ts) as flight_date,
    aircraft_icao,
    operator_icao_guess,
    count(*) as position_reports
from fp
group by 1,2,3
