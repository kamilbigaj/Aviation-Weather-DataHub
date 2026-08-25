# STAGE 1: Builder (Heavy image for installing dependencies)
FROM apache/airflow:2.7.2-python3.10 AS builder

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt .
# Install dependencies into a specific directory to copy later
RUN pip install --user --no-cache-dir -r requirements.txt

# STAGE 2: Production (Lightweight, secure runtime image)
FROM apache/airflow:2.7.2-python3.10

# Copy only the compiled Python packages from the builder stage
COPY --from=builder /home/airflow/.local /home/airflow/.local

# Copy the actual project files
COPY --chown=airflow:root . /opt/airflow/

# Set PATH so Airflow can find the installed dependencies
ENV PATH=/home/airflow/.local/bin:$PATH