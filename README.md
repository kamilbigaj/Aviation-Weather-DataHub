# Aviation & Weather ETL Pipeline ✈️

![CI Pipeline](https://github.com/kamilbigaj/Aviation-Weather-DataHub/actions/workflows/ci.yml/badge.svg)

An end-to-end Data Engineering pipeline that extracts daily flight arrival and historical weather data, stores raw data in AWS S3, loads processed data into PostgreSQL, and transforms it into an analytical data mart using dbt. The entire workflow is orchestrated with Apache Airflow and containerized with Docker.

## Architecture

![Data Engineering Architecture](docs/architecture.png)

## Features

* **Automated ETL pipeline** orchestrated with Apache Airflow (scheduled `@daily`).
* **Data Extraction:** Flight arrival data from the AeroDataBox API and historical weather observations from Meteostat for selected European airports.
* **Data Lake Layer:** Raw API JSON responses archived in AWS S3 for historical backup and reprocessing.
* **Idempotent Data Loading:** Relational data loaded into AWS RDS (PostgreSQL) with conflict handling to prevent duplicates.
* **SQL Transformations:** Data modeling and analytical aggregations managed by `dbt`, including flight delay calculation and UTC timestamp standardization.
* **Containerized Environment:** Fully reproducible local stack using Docker & Docker Compose.

## Tech Stack

| Category | Technology |
|---|---|
| **Programming Language** | Python |
| **Data Processing** | Pandas |
| **Flight Data** | AeroDataBox API |
| **Weather Data** | Meteostat |
| **Data Lake** | AWS S3 |
| **Cloud Database** | AWS RDS (PostgreSQL 15) |
| **Transformation** | dbt (Data Build Tool) |
| **Orchestration** | Apache Airflow |
| **Containerization** | Docker, Docker Compose |
| **Business Intelligence** | Metabase |

## Project Structure

```text
.
├── dags/                    # Airflow DAG definitions
├── aviation_analytics/      # dbt project (models, staging, config)
├── tests/                   # Unit tests for ETL functions
├── docs/                    # Architecture & dashboard screenshots
├── docker-compose.yml
├── Dockerfile
├── main.py                  # Python ETL implementation
├── requirements.txt
└── README.md
```

## Business Intelligence & Visualization

To make the analytical data mart actionable, the project integrates **Metabase** for interactive data reporting.

![Metabase Dashboard](docs/dashboard.png)

The dashboard provides immediate business value by tracking:

* **Key Performance Indicators (KPIs):** Total flight volume and overall average delays.
* **Aviation Bottlenecks:** Identifying the worst-performing airlines and airports.
* **Time-Series Analysis:** Daily trends in flight delays, exposing operational gaps and weather impacts.

## Getting Started

**Prerequisites:** Docker Desktop installed. No local Python or PostgreSQL installation required. You will also need credentials for AeroDataBox (RapidAPI), AWS S3, and AWS RDS.

**1. Clone the repository**

```bash
git clone https://github.com/kamilbigaj/Aviation-Weather-DataHub.git
cd Aviation-Weather-DataHub
```

**2. Configure environment**

Create a `.env` file in the project root:

```env
AERO_API_KEY=your_rapidapi_key
POSTGRES_PASSWORD=your_secure_password
DATABASE_URL=postgresql+psycopg2://postgres:${POSTGRES_PASSWORD}@postgres_db:5432/postgres

AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_REGION=your_aws_region
S3_BUCKET_NAME=your_s3_bucket

RDS_HOST=your_rds_endpoint.amazonaws.com
RDS_PORT=5432
RDS_USER=your_rds_user
RDS_PASSWORD=your_rds_password
```

> Never commit your `.env` file or AWS credentials to Git.

**3. Run the application**

```bash
docker compose up --build
```

Access the Apache Airflow UI at `http://localhost:8080` (default login: `admin` / `admin`). Locate the `aviation_weather_etl_pipeline` DAG and trigger it.

**4. Stop the application**

```bash
docker compose down
```

## Testing

The project includes unit tests for core ETL functions. Tests are located in the `tests/` directory.

```bash
pip install -r requirements.txt
pytest tests/
```

## Future Improvements

* dbt tests and data quality checks
* CI/CD pipeline using GitHub Actions
* AWS Secrets Manager integration for credential management
* Cloud-based Airflow deployment
* Migration to a dedicated cloud data warehouse (e.g. Amazon Redshift)

## License

This project is licensed under the MIT License.