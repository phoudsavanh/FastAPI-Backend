import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")
    NODE_IP = os.getenv("NODE_IP", "192.168.101.133")
    FIRMWARE_VERSION = os.getenv("FIRMWARE_VERSION", "v2.4.1-stable")

    # Read DATABASE_URL from .env
    DATABASE_URL = os.getenv("DATABASE_URL")

    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set in the .env file")

    DB_ECHO = os.getenv("DB_ECHO", "False").lower() == "true"

settings = Settings()