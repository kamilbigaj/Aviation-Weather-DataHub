import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# 1. Connect to the cloud database
load_dotenv()
engine = create_engine(os.getenv('DATABASE_URL'))

# 2. Fetch the first 3 rows from the new table created by dbt
query = "SELECT flight_number, arrival_airport, temperature_c, wind_speed_kmh FROM flights_weather LIMIT 3;"
df = pd.read_sql(query, engine)

# 3. Print the results
print("\n✈️🌦️ OTO TWOJE POŁĄCZONE DANE Z CHMURY (ZŁOTA TABELA):\n")
print(df.to_string(index=False))
print("\n")