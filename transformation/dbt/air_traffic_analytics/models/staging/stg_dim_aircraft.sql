with src as (
    select *
    from read_parquet('{{ var("lake_root") }}/silver/dim_aircraft/dim_aircraft.parquet')
)
select
    lower(aircraft_icao) as aircraft_icao,
    registration,
    manufacturer,
    model,
    type_code,
    operator_name,
    operator_icao,
    country_of_registration,
    country_iso,
    source
from src
where aircraft_icao is not null
