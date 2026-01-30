from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import duckdb


# ----------------------------
# Paths
# ----------------------------
# .../African-Air-Traffic-DataHouse-Analytics
PROJECT_ROOT = Path(__file__).resolve().parents[1]
LAKE_ROOT = PROJECT_ROOT / "lake"

BRONZE_OPENSKY_DIR = LAKE_ROOT / "bronze" / "adsb" / "opensky"
BRONZE_ADSBDB_AIRCRAFT_DIR = LAKE_ROOT / \
    "bronze" / "metadata" / "adsbdb" / "aircraft"
BRONZE_ADSBDB_AIRPORT_DIR = LAKE_ROOT / \
    "bronze" / "metadata" / "adsbdb" / "airport"

SILVER_FLIGHT_POS_DIR = LAKE_ROOT / "silver" / "flight_positions"
SILVER_DIM_AIRCRAFT_DIR = LAKE_ROOT / "silver" / "dim_aircraft"
SILVER_DIM_AIRPORT_DIR = LAKE_ROOT / "silver" / "dim_airport"

SILVER_FLIGHT_POS_PATH = SILVER_FLIGHT_POS_DIR / "silver_flight_positions.parquet"
SILVER_DIM_AIRCRAFT_PATH = SILVER_DIM_AIRCRAFT_DIR / "dim_aircraft.parquet"
SILVER_DIM_AIRPORT_PATH = SILVER_DIM_AIRPORT_DIR / "dim_airport.parquet"

DUCKDB_PATH = PROJECT_ROOT / "warehouse" / "air_traffic.duckdb"


# ----------------------------
# Helpers
# ----------------------------
def list_files(dir_path: Path, patterns: Tuple[str, ...] = ("*.json", "*.jsonl")) -> List[Path]:
    if not dir_path.exists():
        return []
    files: List[Path] = []
    for p in patterns:
        files.extend(dir_path.glob(p))
    return sorted(files, key=lambda x: x.stat().st_mtime)


def safe_load_json(path: Path) -> Any:
    txt = path.read_text(encoding="utf-8").strip()
    if not txt:
        return None
    return json.loads(txt)


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def epoch_to_utc_ts(epoch_seconds: Optional[int]) -> Optional[pd.Timestamp]:
    if epoch_seconds is None:
        return None
    try:
        return pd.to_datetime(int(epoch_seconds), unit="s", utc=True)
    except Exception:
        return None


# ----------------------------
# Build: flight_positions (from OpenSky raw payloads)
# ----------------------------
def parse_opensky_payload(payload: Dict[str, Any], source_file: str) -> List[Dict[str, Any]]:
    """
    OpenSky /states/all response structure:
      {
        "time": 1700000000,
        "states": [
          [icao24, callsign, origin_country, time_position, last_contact, longitude, latitude,
           baro_altitude, on_ground, velocity, true_track, vertical_rate, sensors, geo_altitude,
           squawk, spi, position_source],
          ...
        ]
      }
    """
    rows: List[Dict[str, Any]] = []
    if not payload or "states" not in payload:
        return rows

    event_ts = epoch_to_utc_ts(payload.get("time"))
    states = payload.get("states") or []

    for s in states:
        if not isinstance(s, list) or len(s) < 7:
            continue

        icao24 = s[0]
        callsign = (s[1] or "").strip() if len(s) > 1 else None
        lon = s[5] if len(s) > 5 else None
        lat = s[6] if len(s) > 6 else None

        # Skip points without coordinates (your dbt staging requires lat/lon)
        if lon is None or lat is None:
            continue

        rows.append(
            {
                "aircraft_icao": str(icao24).lower() if icao24 else None,
                "event_timestamp_utc": event_ts,
                "latitude_deg": float(lat) if lat is not None else None,
                "longitude_deg": float(lon) if lon is not None else None,
                "callsign": callsign if callsign else None,
                "operator_icao_guess": None,  # can be enriched later
                # geo_altitude preferred
                "altitude_m": s[13] if len(s) > 13 else s[7] if len(s) > 7 else None,
                "baro_altitude_m": s[7] if len(s) > 7 else None,
                "ground_speed_mps": s[9] if len(s) > 9 else None,
                "track_deg": s[10] if len(s) > 10 else None,
                "vertical_rate_mps": s[11] if len(s) > 11 else None,
                "on_ground": bool(s[8]) if len(s) > 8 and s[8] is not None else None,
                "squawk": s[14] if len(s) > 14 else None,
                "source": "opensky",
                "source_file": source_file,
            }
        )

    return rows


def build_flight_positions_from_opensky(limit_files: int = 50) -> Tuple[Path, int]:
    files = list_files(BRONZE_OPENSKY_DIR)
    if not files:
        print(
            f"[SILVER] No OpenSky bronze files found at: {BRONZE_OPENSKY_DIR}")
        ensure_parent(SILVER_FLIGHT_POS_PATH)
        # Write placeholder to avoid dbt 'No files found'
        pd.DataFrame(
            columns=[
                "aircraft_icao",
                "event_timestamp_utc",
                "latitude_deg",
                "longitude_deg",
                "callsign",
                "operator_icao_guess",
                "altitude_m",
                "baro_altitude_m",
                "ground_speed_mps",
                "track_deg",
                "vertical_rate_mps",
                "on_ground",
                "squawk",
                "source",
                "source_file",
            ]
        ).to_parquet(SILVER_FLIGHT_POS_PATH, index=False)
        return SILVER_FLIGHT_POS_PATH, 0

    # Use most recent N files to keep it fast
    files = files[-limit_files:]

    all_rows: List[Dict[str, Any]] = []
    for f in files:
        payload = safe_load_json(f)
        if isinstance(payload, dict):
            all_rows.extend(parse_opensky_payload(payload, source_file=f.name))

    df = pd.DataFrame(all_rows)
    if df.empty:
        print("[SILVER] flight_positions: parsed 0 usable rows (no lat/lon). Writing placeholder parquet.")
        ensure_parent(SILVER_FLIGHT_POS_PATH)
        df.to_parquet(SILVER_FLIGHT_POS_PATH, index=False)
        return SILVER_FLIGHT_POS_PATH, 0

    # Clean / types
    df["aircraft_icao"] = df["aircraft_icao"].astype("string").str.lower()
    df["callsign"] = df["callsign"].astype("string")
    df["operator_icao_guess"] = df["operator_icao_guess"].astype("string")
    df["source"] = df["source"].astype("string")
    df["source_file"] = df["source_file"].astype("string")

    # Ensure ts is UTC timestamp
    df["event_timestamp_utc"] = pd.to_datetime(
        df["event_timestamp_utc"], utc=True, errors="coerce")

    # Write parquet
    ensure_parent(SILVER_FLIGHT_POS_PATH)
    df.to_parquet(SILVER_FLIGHT_POS_PATH, index=False)

    return SILVER_FLIGHT_POS_PATH, int(len(df))


# ----------------------------
# Build: dim_aircraft (from ADSBDB aircraft metadata if present)
# ----------------------------
def normalize_adsbdb_aircraft_records(obj: Any) -> List[Dict[str, Any]]:
    """
    Tries to normalize various possible ADSBDB response shapes into list[dict].
    """
    if obj is None:
        return []
    if isinstance(obj, list):
        return [r for r in obj if isinstance(r, dict)]
    if isinstance(obj, dict):
        # common keys used by APIs
        for k in ("aircraft", "data", "results", "items"):
            v = obj.get(k)
            if isinstance(v, list):
                return [r for r in v if isinstance(r, dict)]
        # maybe it's already a single record
        return [obj]
    return []


def build_dim_aircraft(limit_files: int = 50) -> Tuple[Path, int]:
    files = list_files(BRONZE_ADSBDB_AIRCRAFT_DIR)
    records: List[Dict[str, Any]] = []

    if files:
        files = files[-limit_files:]
        for f in files:
            obj = safe_load_json(f)
            records.extend(normalize_adsbdb_aircraft_records(obj))

    # If still nothing, write placeholder (so dbt won't fail)
    if not records:
        ensure_parent(SILVER_DIM_AIRCRAFT_PATH)
        pd.DataFrame(
            columns=[
                "aircraft_icao",
                "registration",
                "manufacturer",
                "model",
                "type_code",
                "operator_name",
                "operator_icao",
                "country_of_registration",
                "country_iso",
                "source",
            ]
        ).to_parquet(SILVER_DIM_AIRCRAFT_PATH, index=False)
        print(
            f"[SILVER] dim_aircraft: no source data found. Wrote placeholder -> {SILVER_DIM_AIRCRAFT_PATH}")
        return SILVER_DIM_AIRCRAFT_PATH, 0

    df = pd.DataFrame(records)

    # Best-effort mapping (handles different key names)
    def pick(col_candidates: List[str]) -> pd.Series:
        for c in col_candidates:
            if c in df.columns:
                return df[c]
        return pd.Series([None] * len(df))

    out = pd.DataFrame(
        {
            "aircraft_icao": pick(["aircraft_icao", "icao", "icao24", "hex", "hex_code"]).astype("string").str.lower(),
            "registration": pick(["registration", "reg"]).astype("string"),
            "manufacturer": pick(["manufacturer", "manufacturer_name", "mfr"]).astype("string"),
            "model": pick(["model", "model_name"]).astype("string"),
            "type_code": pick(["type_code", "icao_type", "type"]).astype("string"),
            "operator_name": pick(["operator_name", "operator"]).astype("string"),
            "operator_icao": pick(["operator_icao", "operator_icao_code"]).astype("string"),
            "country_of_registration": pick(["country_of_registration", "country"]).astype("string"),
            "country_iso": pick(["country_iso", "country_code", "iso_country"]).astype("string"),
            "source": "adsbdb",
        }
    )

    out = out[out["aircraft_icao"].notna()]

    ensure_parent(SILVER_DIM_AIRCRAFT_PATH)
    out.to_parquet(SILVER_DIM_AIRCRAFT_PATH, index=False)
    return SILVER_DIM_AIRCRAFT_PATH, int(len(out))


# ----------------------------
# Build: dim_airport (optional; placeholder if no data yet)
# ----------------------------
def normalize_airport_records(obj: Any) -> List[Dict[str, Any]]:
    if obj is None:
        return []
    if isinstance(obj, list):
        return [r for r in obj if isinstance(r, dict)]
    if isinstance(obj, dict):
        for k in ("airports", "data", "results", "items"):
            v = obj.get(k)
            if isinstance(v, list):
                return [r for r in v if isinstance(r, dict)]
        return [obj]
    return []


def build_dim_airport(limit_files: int = 50) -> Tuple[Path, int]:
    files = list_files(BRONZE_ADSBDB_AIRPORT_DIR)
    records: List[Dict[str, Any]] = []

    if files:
        files = files[-limit_files:]
        for f in files:
            obj = safe_load_json(f)
            records.extend(normalize_airport_records(obj))

    if not records:
        # Placeholder to keep dbt happy (prevents: "No files found ... dim_airport.parquet")
        ensure_parent(SILVER_DIM_AIRPORT_PATH)
        pd.DataFrame(
            columns=[
                "airport_icao",
                "airport_iata",
                "airport_name",
                "country",
                "latitude_deg",
                "longitude_deg",
                "elevation_ft",
                "source",
            ]
        ).to_parquet(SILVER_DIM_AIRPORT_PATH, index=False)
        print(
            f"[SILVER] dim_airport: no source data found. Wrote placeholder -> {SILVER_DIM_AIRPORT_PATH}")
        return SILVER_DIM_AIRPORT_PATH, 0

    df = pd.DataFrame(records)

    def pick(col_candidates: List[str]) -> pd.Series:
        for c in col_candidates:
            if c in df.columns:
                return df[c]
        return pd.Series([None] * len(df))

    out = pd.DataFrame(
        {
            "airport_icao": pick(["airport_icao", "icao", "ident"]).astype("string").str.upper(),
            "airport_iata": pick(["airport_iata", "iata"]).astype("string").str.upper(),
            "airport_name": pick(["airport_name", "name"]).astype("string"),
            "country": pick(["country", "iso_country"]).astype("string"),
            "latitude_deg": pd.to_numeric(pick(["latitude_deg", "lat", "latitude"]), errors="coerce"),
            "longitude_deg": pd.to_numeric(pick(["longitude_deg", "lon", "longitude"]), errors="coerce"),
            "elevation_ft": pd.to_numeric(pick(["elevation_ft", "elevation"]), errors="coerce"),
            "source": "adsbdb",
        }
    )

    out = out[out["airport_icao"].notna() & out["latitude_deg"].notna()
              & out["longitude_deg"].notna()]

    ensure_parent(SILVER_DIM_AIRPORT_PATH)
    out.to_parquet(SILVER_DIM_AIRPORT_PATH, index=False)
    return SILVER_DIM_AIRPORT_PATH, int(len(out))


# ----------------------------
# Optional: register into DuckDB (nice for local querying)
# ----------------------------
def register_parquets_into_duckdb() -> None:
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DUCKDB_PATH))

    # Use parquet scanning (doesn't load into memory)
    con.execute(
        f"""
        CREATE OR REPLACE VIEW v_silver_flight_positions AS
        SELECT * FROM read_parquet('{SILVER_FLIGHT_POS_PATH.as_posix()}')
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE VIEW v_dim_aircraft AS
        SELECT * FROM read_parquet('{SILVER_DIM_AIRCRAFT_PATH.as_posix()}')
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE VIEW v_dim_airport AS
        SELECT * FROM read_parquet('{SILVER_DIM_AIRPORT_PATH.as_posix()}')
        """
    )
    con.close()


def main() -> None:
    SILVER_FLIGHT_POS_DIR.mkdir(parents=True, exist_ok=True)
    SILVER_DIM_AIRCRAFT_DIR.mkdir(parents=True, exist_ok=True)
    SILVER_DIM_AIRPORT_DIR.mkdir(parents=True, exist_ok=True)

    fp_path, fp_count = build_flight_positions_from_opensky()
    ac_path, ac_count = build_dim_aircraft()
    ap_path, ap_count = build_dim_airport()

    # Optional but helpful
    register_parquets_into_duckdb()

    print(f"[SILVER] flight_positions: {fp_path} (rows={fp_count})")
    print(f"[SILVER] dim_aircraft:     {ac_path} (rows={ac_count})")
    print(f"[SILVER] dim_airport:      {ap_path} (rows={ap_count})")


if __name__ == "__main__":
    main()
