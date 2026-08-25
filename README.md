# Aviation & Weather ETL Pipeline ✈️ 

> An end-to-end Data Engineering pipeline that extracts daily flight arrival and historical weather data, stores raw data in AWS S3, loads processed data into PostgreSQL, and transforms it into an analytical data mart using dbt. The entire workflow is orchestrated with Apache Airflow and containerized with Docker.

---

## Architecture

![Data Engineering Architecture](docs/architecture.png)

The pipeline follows a modern data engineering architecture combining Python, AWS, PostgreSQL, dbt, Apache Airflow, Docker, and Metabase.

---

## Features

* Automated daily ETL pipeline orchestrated with Apache Airflow
* Flight arrival data extraction from the AeroDataBox API
* Historical weather data retrieval using Meteostat
* Raw API responses archived in AWS S3
* Processed data stored in PostgreSQL on AWS RDS
* Initial data cleaning and transformation using Python and Pandas
* JSON normalization into relational structures
* Flight delay calculation based on scheduled and actual arrival times
* UTC timestamp standardization
* SQL-based transformations using dbt
* Analytical `flights_weather` data mart for downstream analytics
* Idempotent data loading to prevent duplicate records
* Environment-based configuration using `.env`
* Fully containerized Airflow environment using Docker Compose
* Automated task dependencies, scheduling, and retry handling through Airflow

---

## Tech Stack

| Category             | Technology             |
| -------------------- | ---------------------- |
| Programming Language | Python                 |
| Data Processing      | Pandas                 |
| Flight Data          | AeroDataBox API        |
| Weather Data         | Meteostat              |
| Data Lake            | AWS S3                 |
| Database             | PostgreSQL 15          |
| Cloud Database       | AWS RDS                |
| Transformation       | dbt                    |
| Orchestration        | Apache Airflow         |
| Containerization     | Docker, Docker Compose |
| Configuration        | Environment Variables  |
| Business Intelligence | Metabase |

---

## Pipeline Workflow

The pipeline is divided into two main Airflow tasks:

```text
run_main_python_script
        │
        ├── Extract flight data
        ├── Extract weather data
        ├── Clean & transform data
        ├── Upload raw JSON → AWS S3
        └── Load processed data → PostgreSQL
                         │
                         ▼
                 run_dbt_models
                         │
                         ├── Read source tables
                         ├── Transform data with SQL
                         ├── Join flight & weather data
                         └── Build analytical data mart
```

### 1. Extract

Flight arrival data is retrieved from the **AeroDataBox API** for selected European airports:

* **EPWA** — Warsaw Chopin Airport
* **EGLL** — London Heathrow Airport
* **EDDF** — Frankfurt Airport

Historical weather observations are retrieved from **Meteostat** based on the airport location and flight arrival timeframe.

---

### 2. Raw Data Storage

Raw API responses are preserved in **AWS S3** before further processing.

This provides:

* A historical archive of source data
* A backup of the original API responses
* Separation between raw and processed data
* The ability to reprocess historical data without calling the external APIs again

The S3 layer acts as the pipeline's **raw data storage / data lake layer**.

---

### 3. Transform

Initial data preparation is performed using **Python and Pandas**.

The transformation layer handles:

* Flattening nested JSON structures
* Removing unnecessary fields
* Standardizing column names
* Converting timestamps to UTC
* Calculating flight delays
* Preparing relational datasets
* Cleaning missing or invalid values
* Preparing data for PostgreSQL ingestion

---

### 4. Load

Processed datasets are loaded into **PostgreSQL**.

For the cloud environment, PostgreSQL is hosted using **AWS RDS**, providing a managed relational database service.

The loading process follows an **idempotent strategy**, ensuring that repeated pipeline executions do not create duplicate records.

---

### 5. Transform with dbt

After the Python ETL process completes, Airflow triggers the dbt transformation layer.

dbt performs SQL-based transformations directly against the PostgreSQL database.

The transformation layer is responsible for:

* Reading processed source tables
* Joining flight and weather datasets
* Applying business logic
* Creating analytical models
* Producing the final `flights_weather` data mart

This separates **data ingestion and initial processing** from **analytical SQL transformations**.

---

## Data Layers

The project follows a layered approach to data processing:

```text
┌─────────────────────────────────────┐
│            External APIs            │
│       AeroDataBox / Meteostat       │
└──────────────────┬──────────────────┘
                   │
                   ▼
┌─────────────────────────────────────┐
│             Raw Layer               │
│              AWS S3                 │
│          Original JSON data         │
└──────────────────┬──────────────────┘
                   │
                   ▼
┌─────────────────────────────────────┐
│          Processed Layer            │
│         PostgreSQL / RDS            │
│       Clean relational tables       │
└──────────────────┬──────────────────┘
                   │
                   ▼
┌─────────────────────────────────────┐
│        Transformation Layer        │
│                dbt                  │
│         SQL-based models            │
└──────────────────┬──────────────────┘
                   │
                   ▼
┌─────────────────────────────────────┐
│        Analytical Data Mart         │
│          flights_weather            │
│        BI / Analytics Ready         │
└─────────────────────────────────────┘
```

---

## AWS Architecture

AWS is used to provide persistent cloud storage for the pipeline.

### Amazon S3

S3 stores the original JSON payloads retrieved from external APIs.

The raw data layer provides a durable historical archive and allows the pipeline to preserve source data independently of the external APIs.

### Amazon RDS

Amazon RDS hosts the PostgreSQL database used by the pipeline.

It stores processed relational datasets that are later consumed by dbt.

```text
AWS
│
├── S3
│   └── Raw API JSON
│
└── RDS
    └── PostgreSQL
        ├── Flight data
        ├── Weather data
        └── dbt models
```

---

## dbt Transformation Layer

The project uses **dbt (Data Build Tool)** to implement SQL-based transformations inside PostgreSQL.

The dbt layer transforms the processed source tables into an analytical data mart.

The main output is:

```text
flights_weather
```

This model combines aviation and weather information into a single dataset designed for analytical workloads and potential BI integration.

The separation of responsibilities follows:

```text
Python
    ↓
Extraction & Initial Cleaning

PostgreSQL
    ↓
Processed Source Data

dbt
    ↓
Business Logic & Analytical Transformations

flights_weather
    ↓
Analytics / BI
```

---

## Apache Airflow

Apache Airflow orchestrates the complete pipeline and is responsible for its daily execution.

The DAG manages:

* Daily scheduling using `@daily`
* Task dependencies
* Pipeline execution order
* Automatic retries
* Workflow monitoring
* Error handling
* Triggering the dbt transformation layer after data ingestion

### Main Tasks

#### `run_main_python_script`

Responsible for:

1. Extracting data from external APIs
2. Processing the data with Python and Pandas
3. Uploading raw JSON responses to AWS S3
4. Loading processed datasets into PostgreSQL

#### `run_dbt_models`

Responsible for:

1. Starting the dbt transformation process
2. Reading processed source tables
3. Joining flight and weather data
4. Building the final analytical models

The dependency between the tasks ensures that dbt only runs after the ingestion process has completed successfully.

---

## Docker

The Airflow environment is fully containerized using Docker Compose.

The current setup includes:

* PostgreSQL 15
* Apache Airflow Webserver
* Apache Airflow Scheduler
* Airflow initialization service
* Persistent PostgreSQL volume
* Mounted Airflow DAGs
* Mounted Python ETL script
* Mounted dbt project

### Container Architecture

```text
Docker Compose
│
├── postgres_db
│   └── PostgreSQL 15
│
├── airflow-init
│   └── Airflow initialization
│
├── airflow-webserver
│   └── Airflow UI :8080
│
└── airflow-scheduler
    └── DAG scheduling & execution
```

---

## Data Sources

### Flight Data

**AeroDataBox API**

Provides flight arrival information including scheduled and actual arrival times.

The pipeline uses the data to calculate flight delays and associate each arrival with corresponding weather observations.

### Weather Data

**Meteostat**

Provides historical meteorological observations for the selected airports.

Current weather metrics include:

* Air temperature
* Wind speed
* Precipitation

---

## Database

The PostgreSQL database contains relational datasets used throughout the transformation process.

The data model separates aviation and weather information before they are combined by dbt into the final analytical model.

The pipeline uses unique constraints and conflict handling to support repeated executions without generating duplicate records.

---

## Project Structure

```text
.
├── dags/
│   └── aviation_etl_dag.py          # Airflow DAG
│
├── aviation_analytics/              # dbt project
│   ├── models/
│   │   ├── staging/
│   │   └── ...
│   ├── dbt_project.yml
│   └── ...
│
├── docker-compose.yml               # Docker services configuration
├── Dockerfile                       # Airflow image definition
├── requirements.txt                 # Python dependencies
├── main.py                          # Python ETL implementation
├── .env                             # Environment variables (git-ignored)
├── clean_flights.csv                # Temporary staging file (git-ignored)
├── clean_weather.csv                # Temporary staging file (git-ignored)
└── README.md
```

---

## Getting Started

### Prerequisites

The only local requirement is:

* **Docker Desktop**

No local installation of Python, PostgreSQL, or Apache Airflow is required.

You will also need valid credentials for:

* AeroDataBox / RapidAPI
* AWS S3
* AWS RDS PostgreSQL

---

### 1. Clone the repository

```bash
git clone https://github.com/kamilbigaj/Aviation-Weather-DataHub.git
cd Aviation-Weather-DataHub
```

---

### 2. Create a `.env` file

Create a `.env` file in the project root:

```env
AERO_API_KEY=your_rapidapi_key

POSTGRES_PASSWORD=your_secure_password

DATABASE_URL=postgresql+psycopg2://postgres:${POSTGRES_PASSWORD}@postgres_db:5432/postgres

AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_REGION=your_aws_region
S3_BUCKET_NAME=your_s3_bucket

# AWS RDS Credentials
RDS_HOST=your_rds_endpoint.amazonaws.com
RDS_PORT=5432
RDS_USER=your_rds_user
RDS_PASSWORD=your_rds_password
```

> Never commit your `.env` file or AWS credentials to Git.

---

### 3. Build and start the containers

```bash
docker compose up --build
```

Docker Compose will initialize the PostgreSQL database and start the Airflow services.

---

### 4. Open Apache Airflow

Once the containers are running, open:

```text
http://localhost:8080
```

Default development credentials:

```text
Username: admin
Password: admin
```

Locate the:

```text
aviation_weather_etl_pipeline
```

DAG and enable it.

The pipeline can then be triggered manually or executed according to its daily schedule.

---

### 5. Stop the application

```bash
docker compose down
```

---

## Configuration & Security

Sensitive configuration is managed using environment variables.

The following values should **never be committed to the repository**:

* API keys
* AWS access keys
* Database passwords
* Private credentials
* `.env` files

For local development, credentials should be stored in `.env` and excluded through `.gitignore`.

For production deployments, a dedicated secrets management solution should be used instead of storing credentials directly in environment files.

---

## Idempotency

The pipeline is designed to safely handle repeated executions.

Database constraints and conflict handling prevent duplicate records from being inserted when the same source data is processed multiple times.

This makes the pipeline more reliable when:

* Airflow tasks are retried
* A DAG is manually triggered
* A pipeline execution fails and needs to be restarted
* Historical data is processed again

---

## Analytical Use Case

The final `flights_weather` data mart combines aviation and weather information for analytical purposes.

This dataset can be used to investigate relationships between:

* Flight delays
* Arrival times
* Temperature
* Wind conditions
* Precipitation
* Airport
* Date and time

The final model can serve as a foundation for downstream BI tools such as **Power BI** or **Tableau**.

---

## Business Intelligence & Visualization

To make the analytical data mart actionable, the project integrates **Metabase** for interactive data visualization and reporting. 

![Metabase Dashboard](docs/dashboard.png)

The dashboard provides immediate business value by tracking:
* **Key Performance Indicators (KPIs):** Total flight volume and overall average delays.
* **Aviation Bottlenecks:** Identifying the worst-performing airlines and airports.
* **Time-Series Analysis:** Daily trends in flight delays, exposing operational gaps and weather impacts over time.

## Future Improvements

Potential future improvements include:

* Incremental dbt models
* dbt tests and data quality checks
* Source freshness monitoring
* Unit and integration testing
* CI/CD using GitHub Actions
* AWS Secrets Manager integration
* Improved Airflow monitoring and alerting
* Cloud-based Airflow deployment
* Migration from PostgreSQL/RDS to a dedicated cloud data warehouse such as Amazon Redshift
* Additional airports and weather metrics

---

## License

This project is licensed under the MIT License.