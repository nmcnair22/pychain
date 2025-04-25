import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Use in-memory database flag
USE_IN_MEMORY_DB = os.getenv('USE_IN_MEMORY_DB', 'true').lower() == 'true'

# CISSDM Database configuration
CISSDM_DB_CONFIG = {
    'host': os.getenv('CISSDM_DB_HOST', 'cissdm.cis.local'),
    'port': int(os.getenv('CISSDM_DB_PORT', 3306)),
    'database': os.getenv('CISSDM_DB_NAME', 'cissdm'),
    'user': os.getenv('CISSDM_DB_USER', 'view'),
    'password': os.getenv('CISSDM_DB_PASSWORD', 'Eastw00d'),
}

# Ticketing Database configuration
TICKETING_DB_CONFIG = {
    'host': os.getenv('TICKETING_DB_HOST', 'ticket10.cis.local'),
    'port': int(os.getenv('TICKETING_DB_PORT', 3306)),
    'database': os.getenv('TICKETING_DB_NAME', 'ticketing'),
    'user': os.getenv('TICKETING_DB_USER', 'view'),
    'password': os.getenv('TICKETING_DB_PASSWORD', 'Eastw00d'),
}

# OpenAI configuration
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
OPENAI_MODEL = os.getenv('OPENAI_MODEL', 'gpt-4o')

# Analysis database path (SQLite)
ANALYSIS_DB_PATH = os.getenv('ANALYSIS_DB_PATH', os.path.join(os.path.dirname(__file__), 'data', 'analysis.db'))

# SQLAlchemy connection strings
if USE_IN_MEMORY_DB:
    # SQLite in-memory database for local development
    CISSDM_DATABASE_URL = "sqlite:///:memory:"
    TICKETING_DATABASE_URL = "sqlite:///:memory:"
else:
    # Real MySQL database connections
    CISSDM_DATABASE_URL = f"mysql+mysqlconnector://{CISSDM_DB_CONFIG['user']}:{CISSDM_DB_CONFIG['password']}@{CISSDM_DB_CONFIG['host']}:{CISSDM_DB_CONFIG['port']}/{CISSDM_DB_CONFIG['database']}"
    TICKETING_DATABASE_URL = f"mysql+mysqlconnector://{TICKETING_DB_CONFIG['user']}:{TICKETING_DB_CONFIG['password']}@{TICKETING_DB_CONFIG['host']}:{TICKETING_DB_CONFIG['port']}/{TICKETING_DB_CONFIG['database']}" 

# Analysis database always uses SQLite for local storage
ANALYSIS_DATABASE_URL = f"sqlite:///{ANALYSIS_DB_PATH}" 