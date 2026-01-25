# 🌍 African Air Traffic Analytics Platform

## Executive Summary

The **African Air Traffic Analytics Platform** is an end-to-end data engineering portfolio project designed to address a real data visibility gap in African airspace analytics. The platform ingests global ADS-B flight data, filters it to West African airspace, enriches it with aircraft and airline metadata, and transforms it into analytics-ready datasets powering geospatial dashboards and business insights.

This project demonstrates practical data engineering skills across **streaming and batch ingestion, geospatial processing, data modeling, orchestration, and analytics delivery**, using industry-standard tools and architectural patterns.

---

## Business Problem

Global flight tracking data exists, but **region-specific, analytics-ready insights for African airspace are sparse**. Aviation stakeholders, researchers, and analysts lack:

* Reliable visibility into air traffic volume by country
* Airline activity concentration in West Africa
* Time-based traffic patterns and congestion indicators

This project solves that problem by transforming raw, high-volume ADS-B signals into structured, decision-ready datasets and visualizations.

---

## Solution Overview

The platform implements a **Bronze–Silver–Gold data architecture** backed by both streaming and batch pipelines:

* **Bronze:** Raw ADS-B flight messages stored for replay and auditing
* **Silver:** Cleaned, validated, and enriched flight data with aircraft and airline metadata
* **Gold:** Aggregated fact tables optimized for analytics, reporting, and dashboards

The result is a scalable analytics foundation capable of powering real-time maps and historical air traffic analysis.

---

## High-Level Architecture

**Data Sources**

* Live ADS-B flight feeds (OpenSky / ADSBExchange / sample dumps)
* adsbdb API (aircraft and airline metadata)
* Open airport and airline datasets (ICAO/IATA)

**Pipeline Flow**

1. ADS-B messages ingested via Python or Kafka
2. Geospatial filtering to West African airspace
3. Enrichment using adsbdb metadata APIs
4. Transformation and aggregation using dbt
5. Analytics tables exposed to BI and map dashboards

---

## Technology Stack

| Layer          | Technologies                           |
| -------------- | -------------------------------------- |
| Ingestion      | Python, Kafka (optional), REST APIs    |
| Orchestration  | Apache Airflow                         |
| Processing     | Python, dbt, Spark (optional)          |
| Storage (Lake) | S3 / MinIO (Parquet, JSON)             |
| Warehouse      | PostgreSQL + PostGIS or ClickHouse     |
| Analytics      | SQL, dbt                               |
| Visualization  | Mapbox / Leaflet, Superset / Streamlit |

---


## 1. Project Purpose & Scope (README Content)

### Purpose

The African Air Traffic DataHouse & Analytics Platform is a data engineering and analytics system designed to ingest, process, store, and analyze aircraft movement data across the African continent. The platform supports real-time and batch aviation data use cases including traffic monitoring, congestion analysis, airline performance, and regulatory reporting.

### Scope (MVP)

**Geographic Scope:** Entire African continent

**MVP Capabilities:**

* Ingest ADS-B flight position data
* Ingest aircraft, airline, and airport metadata
* Store raw and curated datasets in a multi-layer data lake
* Transform data into analytics-ready models
* Serve curated datasets via a data warehouse
* Enable BI dashboards and geospatial analysis

### Out of Scope (Phase 1 MVP)

* Global (non-Africa) flight coverage
* Predictive modeling and ML
* Passenger-level data

---

## 2. Naming Conventions

### 2.1 General Principles

* Use lowercase only
* Use snake_case
* Names must be descriptive and unambiguous
* Avoid abbreviations unless industry-standard

---

### 2.2 Repository Naming

**Format:**

```
air-traffic-datahouse
```

---

### 2.3 Branch Naming

| Type        | Pattern                     | Example                |
| ----------- | --------------------------- | ---------------------- |
| Main        | main                        | main                   |
| Development | develop                     | develop                |
| Feature     | feature/<short-description> | feature/adsb-ingestion |
| Bugfix      | bugfix/<short-description>  | bugfix/schema-fix      |

---

### 2.4 Commit Messages

**Format:**

```
<type>: <short description>
```

**Types:**

* feat: new functionality
* fix: bug fix
* docs: documentation only
* refactor: non-breaking code change
* chore: tooling or housekeeping

**Example:**

```
feat: add adsb json schema
```

---

### 2.5 Data Layer Naming

| Layer         | Prefix  |
| ------------- | ------- |
| Raw / Landing | raw_    |
| Bronze        | bronze_ |
| Silver        | silver_ |
| Gold          | gold_   |

**Example:**

```
silver_flight_positions
```

---

### 2.6 Table Naming

**Format:**

```
<layer>_<entity>[_<granularity>]
```

**Examples:**

* bronze_adsb_messages
* silver_flights_daily
* gold_airport_congestion

---

### 2.7 Column Naming

* snake_case
* Units included when applicable

**Examples:**

* aircraft_icao
* latitude_deg
* altitude_ft
* event_timestamp_utc

---

### 2.8 Pipeline & DAG Naming

**Format:**

```
<layer>_<source>_<frequency>
```

**Examples:**

* ingestion_adsb_streaming
* silver_transform_daily

---

### 2.9 Environment Naming

| Environment | Name    |
| ----------- | ------- |
| Development | dev     |
| Staging     | staging |
| Production  | prod    |

---

## 3. Repository Folder Structure

### Authoritative Structure

```
air-traffic-datahouse/
│── ingestion/              # Data ingestion services
│   ├── adsb/
│   ├── metadata/
│
│── lake/                   # Data lake layers
│   ├── bronze/
│   ├── silver/
│
│── transformations/        # dbt models
│   ├── staging/
│   ├── marts/
│
│── orchestration/          # Airflow DAGs
│
│── warehouse/              # DWH schemas & loaders
│
│── analytics/              # BI models & queries
│
│── infra/                  # IaC, deployment configs
│
│── docs/                   # Architecture & decisions
│
│── README.md
│── NAMING_CONVENTIONS.md
```

---

## 4. Governance Rules

* No code is merged without following naming standards
* All new datasets must declare layer and ownership
* Breaking schema changes require documentation

---

## 5. Phase 0 Exit Checklist

* Naming conventions documented
* Repository initialized
* Folder structure committed
* README clearly explains purpose and scope

**Phase 0 Status: COMPLETE**
