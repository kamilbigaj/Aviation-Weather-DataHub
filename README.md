# Aviation Weather DataHub ✈️ 

> End-to-end ETL pipeline that extracts flight arrival data from the AeroDataBox API, enriches it with historical weather observations from Meteostat, transforms the data into a relational format, and loads it into PostgreSQL. The entire workflow is orchestrated using Apache Airflow and fully containerized with Docker.

---

## Features

- Automated ETL workflow orchestrated with Apache Airflow
- Flight arrival data extraction from the AeroDataBox API
- Historical weather enrichment using Meteostat
- Flight delay calculation based on scheduled and actual arrival times
- JSON normalization into relational tables
- UTC timestamp standardization
- Idempotent data loading into PostgreSQL
- Environment-based configuration using `.env`
- Fully containerized deployment with Docker Compose

---

## Tech Stack

| Category | Technology |
|----------|------------|
| Language | Python |
| Data Processing | Pandas |
| Database | PostgreSQL |
| Orchestration | Apache Airflow |
| Containerization | Docker, Docker Compose |
| APIs | AeroDataBox (RapidAPI), Meteostat |

---

## Architecture

```text
                Apache Airflow
                      │
                      ▼
             Trigger ETL Pipeline
                      │
                      ▼
        Extract Flight Data (API)
                      │
                      ▼
          Transform & Clean Data
                      │
                      ▼
 Fetch Historical Weather (Meteostat)
                      │
                      ▼
        Merge Flight & Weather Data
                      │
                      ▼
          Load into PostgreSQL
```

---

## ETL Workflow

### 1. Extract

Flight arrival information is retrieved from the **AeroDataBox API** for selected European airports:

- EPWA – Warsaw Chopin Airport
- EGLL – London Heathrow Airport
- EDDF – Frankfurt Airport

### 2. Transform

The extracted data is processed by:

- Flattening nested JSON structures
- Standardizing timestamps to UTC
- Calculating flight delays
- Removing unnecessary fields
- Preparing normalized relational datasets

### 3. Enrich

Historical weather observations are retrieved from **Meteostat** using the airport location and flight arrival timestamp.

Collected weather metrics include:

- Air temperature
- Wind speed
- Precipitation

### 4. Load

The transformed datasets are loaded into PostgreSQL using an **idempotent loading strategy**, ensuring duplicate records are automatically ignored through unique constraints and conflict handling.

---

## Apache Airflow

The ETL pipeline is orchestrated using **Apache Airflow**.

The DAG is responsible for:

- Scheduling pipeline executions
- Managing task dependencies
- Monitoring workflow execution
- Retrying failed tasks
- Automating the complete ETL process

---

## Project Structure

```text
.
├── dags/
│   └── aviation_etl_dag.py      # Airflow DAG definition
├── docker-compose.yml           # Multi-container configuration
├── Dockerfile                   # Application image
├── requirements.txt             # Python dependencies
├── main.py                      # ETL pipeline implementation
├── .env                         # Environment variables (git-ignored)
├── clean_flights.csv            # Temporary staging file (git-ignored)
├── clean_weather.csv            # Temporary staging file (git-ignored)
└── README.md
```

---

## Getting Started

### Prerequisites

The only requirement is **Docker Desktop**.

No local installation of Python, PostgreSQL, or Apache Airflow is required.

### 1. Clone the repository

```bash
git clone https://github.com/kamilbigaj/Aviation-Weather-DataHub.git
cd Aviation-Weather-DataHub
```

### 2. Create a `.env` file

```env
AERO_API_KEY=your_rapidapi_key
POSTGRES_PASSWORD=your_secure_password
DATABASE_URL=postgresql+psycopg2://postgres:${POSTGRES_PASSWORD}@postgres_db:5432/postgres
```

### 3. Build and start the containers

```bash
docker compose up --build
```

### 4. Open Apache Airflow

Once all containers are running, open:

```
http://localhost:8080
```

Default credentials:

**Username**

```
admin
```

**Password**

```
admin
```

Locate the **aviation_weather_etl_pipeline** DAG, enable it, and trigger the workflow (or wait for the scheduled execution).

### 5. Stop the application

```bash
docker compose down -v
```

---

## Database

The pipeline stores normalized flight and weather information in **PostgreSQL**.

The database includes relational tables containing:

- Flight arrival information
- Calculated delay metrics
- Historical weather observations

Unique constraints and conflict handling ensure that repeated pipeline executions do not create duplicate records.

---

## Future Improvements

- Incremental data loading
- Automated data quality validation with Great Expectations
- Unit and integration testing
- CI/CD pipeline using GitHub Actions
- Cloud deployment (AWS)
- Data warehouse implementation
- Interactive dashboard using Power BI or Tableau

---

## License

This project is licensed under the MIT License.