# 🛫 Pactravel Data Warehouse

ELT pipeline for travel booking analytics — transforming raw flight and hotel booking data into a star schema data warehouse ready for BI and reporting.

## Architecture

```
Source (PostgreSQL)     →     Staging (dbt views)     →     Marts (dbt tables)
    ├── flight_bookings           ├── stg_flight_bookings       ├── fct_flight_bookings
    ├── hotel_bookings            ├── stg_hotel_bookings        ├── fct_hotel_bookings
    ├── customers                 ├── stg_customers             ├── dim_customers
    ├── airlines                  ├── stg_airlines              ├── dim_airlines
    ├── aircrafts                 ├── stg_aircrafts             ├── dim_aircrafts
    ├── airports                  ├── stg_airports              ├── dim_airports
    └── hotels                    └── stg_hotels                ├── dim_hotels
                                                                ├── dim_date
                                                                └── dim_time
```

## Data Model

![Data Warehouse Schema](dwh_pactravel%20-%20marts.png)

**Star Schema Design:**
- **Fact Tables:** `fct_flight_bookings`, `fct_hotel_bookings` — transactional booking records with surrogate keys to dimensions
- **Dimension Tables:** Customer, airline, aircraft, airport, hotel, date, and time dimensions
- **SCD Strategy:** Type 1 (overwrite) with deduplication via `row_number()`

## Tech Stack

| Layer | Tool |
|-------|------|
| Orchestration | Luigi |
| Transformation | dbt |
| Database | PostgreSQL |
| Containerization | Docker |

## Project Structure

```
dbt-pactravel/
├── dbt_pactravel/           # dbt project
│   ├── models/
│   │   ├── staging/         # Source views (stg_*)
│   │   └── marts/           # Final tables (dim_*, fct_*)
│   ├── seeds/               # dim_date, dim_time CSVs
│   └── dbt_project.yml
├── elt-pipeline.py          # Luigi orchestration
├── extract.py               # Source extraction
├── load.py                  # Load to staging
└── docker-compose.yaml      # Local PostgreSQL setup
```

## Getting Started

### Prerequisites
- Docker
- Python 3.9+
- dbt-core, dbt-postgres

### Setup

1. **Clone the repo**
   ```bash
   git clone https://github.com/fakhrimhd/dbt-pactravel.git
   cd dbt-pactravel
   ```

2. **Create `.env` file**
   ```
   SRC_POSTGRES_DB=pactravel
   SRC_POSTGRES_HOST=localhost
   SRC_POSTGRES_USER=postgres
   SRC_POSTGRES_PASSWORD=yourpassword
   SRC_POSTGRES_PORT=5433

   DWH_POSTGRES_DB=pactravel-dwh
   DWH_POSTGRES_HOST=localhost
   DWH_POSTGRES_USER=postgres
   DWH_POSTGRES_PASSWORD=yourpassword
   DWH_POSTGRES_PORT=5434
   ```

3. **Start databases**
   ```bash
   docker compose up -d
   ```

4. **Run the ELT pipeline**
   ```bash
   python elt-pipeline.py
   ```

   This will:
   - Extract data from source
   - Load to staging
   - Run dbt models (staging → marts)
   - Execute dbt tests

## dbt Commands

```bash
cd dbt_pactravel

dbt debug      # Test connection
dbt deps       # Install packages
dbt run        # Build models
dbt test       # Run tests
dbt docs generate && dbt docs serve  # View documentation
```

## Key Features

- **Surrogate Keys:** Generated via `dbt_utils.generate_surrogate_key()`
- **Deduplication:** `row_number()` window function ensures one record per entity
- **Referential Integrity:** Relationship tests between facts and dimensions
- **Audit Columns:** `created_at`, `updated_at` on all marts tables

## Author

**Fakhri Muhammad** — [GitHub](https://github.com/fakhrimhd) · [LinkedIn](https://linkedin.com/in/fakhrimhd)
