# ✈️ Aviation Weather DataHub (ETL Pipeline)

An automated **Extract, Transform, Load (ETL)** pipeline that integrates real-time aviation data with historical weather observations. The pipeline retrieves flight arrival information, enriches it with weather metrics corresponding to the exact arrival timeframe, transforms the data into a relational format, and stores it in PostgreSQL using an **idempotent loading strategy**.

The entire application is fully containerized with Docker, allowing it to run consistently across different environments.

---

## ✨ Features

- Automated ETL workflow
- Flight arrival data extraction from AeroDataBox API
- Historical weather enrichment using Meteostat
- Flight delay calculation
- JSON normalization into relational tables
- UTC timestamp standardization
- Idempotent database loading (duplicate-safe inserts)
- Dockerized deployment
- Environment-based configuration

---

## 🛠️ Tech Stack

| Category | Technology |
|----------|------------|
| Language | Python 3.10 |
| Data Processing | Pandas |
| Database | PostgreSQL 15 |
| Containerization | Docker, Docker Compose |
| APIs | AeroDataBox (RapidAPI), Meteostat |

---

## ⚙️ Architecture & Data Flow

```text
          AeroDataBox API
                 │
                 ▼
          Extract Flight Data
                 │
                 ▼
        Transform & Clean Data
                 │
                 ▼
      Fetch Historical Weather
                 │
                 ▼
      Merge Flight + Weather Data
                 │
                 ▼
        Load into PostgreSQL
```

### 1. Extract

The pipeline retrieves scheduled and actual arrival data for selected European airports:

- **EPWA** – Warsaw Chopin Airport
- **EGLL** – London Heathrow Airport
- **EDDF** – Frankfurt Airport

Data is fetched through the AeroDataBox API.

### 2. Transform

The extracted JSON data is transformed into a relational format by:

- Flattening nested JSON structures
- Parsing timestamps
- Standardizing all timestamps to UTC
- Computing flight delay durations
- Removing unnecessary fields
- Preparing staging datasets

### 3. Weather Integration

Historical weather observations are retrieved from Meteostat for the exact airport location and flight arrival timeframe, including:

- Air temperature
- Wind speed
- Precipitation

### 4. Load

The cleaned datasets are inserted into PostgreSQL.

The loading process is **idempotent**, meaning duplicate records are automatically ignored using unique constraints and conflict handling.

---

## 📂 Project Structure

```text
.
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env              # Local environment variables (git-ignored)
├── clean_flights.csv # Local staging data (git-ignored)
├── clean_weather.csv # Local staging data (git-ignored)
├── main.py           # Consolidated ETL pipeline logic
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites

Only Docker Desktop is required.

No local installation of Python or PostgreSQL is necessary.

### 1. Clone the repository

```bash
git clone https://github.com/kamilbigaj/Aviation-Weather-DataHub.git
cd Aviation-Weather-DataHub
```

### 2. Create a `.env` file

```text
AERO_API_KEY=your_rapidapi_key
POSTGRES_PASSWORD=your_secure_password
DATABASE_URL=postgresql://postgres:${POSTGRES_PASSWORD}@postgres_db:5432/postgres
```

### 3. Build and start the containers

```bash
docker compose up --build
```

The ETL pipeline will execute automatically.

### 4. Stop the application

```bash
docker compose down
```

---

## 🔧 Environment Variables

| Variable | Description |
|----------|-------------|
| `AERO_API_KEY` | RapidAPI key for AeroDataBox |
| `POSTGRES_PASSWORD` | PostgreSQL password |
| `DATABASE_URL` | PostgreSQL connection string |

---

## 🗄️ Database

The PostgreSQL database stores normalized flight and weather information.

Main entities include:

- **Flights** – Arrival records with calculated delay metrics
- **Weather** – Historical hourly meteorological observations

Unique constraints ensure that duplicate records are not inserted during repeated pipeline executions.

---

## 📊 Example Output

| Flight | Airport | Delay (min) | Temperature | Wind Speed |
|---------|----------|------------:|------------:|-----------:|
| LO279 | EPWA | 18 | 21.4°C | 13 km/h |
| LH905 | EDDF | 7 | 18.9°C | 11 km/h |
| BA846 | EGLL | 0 | 17.8°C | 9 km/h |

---

## 🎯 Future Improvements

- Apache Airflow orchestration
- Incremental loading
- Data quality validation with Great Expectations
- Automated unit and integration tests
- CI/CD with GitHub Actions
- Data visualization dashboard (Power BI or Tableau)

---

## 📄 License

This project is available under the MIT License.