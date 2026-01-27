with src as (
    select *
    from read_parquet('{{ var("lake_root") }}/silver/flight_positions/silver_flight_positions.parquet')
)

select
    lower(aircraft_icao) as aircraft_icao,
    cast(event_timestamp_utc as timestamp) as event_ts,
    cast(latitude_deg as double) as latitude_deg,
    cast(longitude_deg as double) as longitude_deg,
    nullif(trim(callsign), '') as callsign,
    nullif(trim(operator_icao_guess), '') as operator_icao_guess,
    cast(altitude_m as double) as altitude_m,
    cast(baro_altitude_m as double) as baro_altitude_m,
    cast(ground_speed_mps as double) as ground_speed_mps,
    cast(track_deg as double) as track_deg,
    cast(vertical_rate_mps as double) as vertical_rate_mps,
    cast(on_ground as boolean) as on_ground,
    squawk,
    source
from src
where aircraft_icao is not null
  and event_timestamp_utc is not null
  and latitude_deg is not null
  and longitude_deg is not null
