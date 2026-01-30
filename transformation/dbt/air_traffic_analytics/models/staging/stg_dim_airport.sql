with src as (
    select *
    from read_parquet('{{ var("lake_root") }}/silver/dim_airport/dim_airport.parquet')
)
select
    upper(cast(airport_icao as varchar)) as airport_icao,
    cast(airport_iata as varchar) as airport_iata,
    airport_name,
    country,
    cast(latitude_deg as double) as latitude_deg,
    cast(longitude_deg as double) as longitude_deg,
    cast(elevation_ft as double) as elevation_ft
from src
where airport_icao is not null
  and latitude_deg is not null
  and longitude_deg is not null
