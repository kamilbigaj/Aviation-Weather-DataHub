import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Wczytanie zmiennych z pliku .env
load_dotenv()
db_url = os.getenv("DATABASE_URL")

print(f"🔍 Próbuję połączyć się z bazą pod adresem: {db_url.split('@')[-1]}")

try:
    # Próba połączenia z 10-sekundowym limitem czasu
    engine = create_engine(db_url, connect_args={'connect_timeout': 10})
    with engine.connect() as connection:
        print("✅ SUKCES! Twoja chmurowa baza żyje i przepuściła Cię przez zaporę!")
except Exception as e:
    print(f"❌ BŁĄD POŁĄCZENIA: {e}")