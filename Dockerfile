# 1. Use the official lightweight Linux image with Python 3.10 pre-installed.
FROM python:3.10-slim

# 2. Set the working directory to /app inside the container.
WORKDIR /app

# 3. Copy the dependency list into the container.
COPY requirements.txt .

# 4. Install the required Python dependencies.
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy the remaining application files into the container.
COPY . .

# 6. Configure the container to execute the application automatically on startup.
CMD ["python", "main.py"]