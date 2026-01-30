from pathlib import Path
from dotenv import load_dotenv
import os



PROJECT_ROOT = Path(__file__).resolve().parent
ENV_FILE = PROJECT_ROOT / '.env'

load_dotenv(dotenv_path=ENV_FILE)


class Settings:
    MONGODB_URL = os.getenv('MONGODB_URL', '')


settings = Settings()