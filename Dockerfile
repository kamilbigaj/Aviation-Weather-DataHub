# We use the official Apache Airflow image with Python 3.10
FROM apache/airflow:2.9.1-python3.10

# Copy our dependencies file
COPY requirements.txt .

# Install additional libraries (Pandas, Meteostat, etc.)
RUN pip install --no-cache-dir -r requirements.txt