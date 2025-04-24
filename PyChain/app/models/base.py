from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from config import CISSDM_DATABASE_URL, TICKETING_DATABASE_URL, ANALYSIS_DATABASE_URL, USE_IN_MEMORY_DB

# Create SQLAlchemy engines
cissdm_engine = create_engine(CISSDM_DATABASE_URL, connect_args={"check_same_thread": False} if USE_IN_MEMORY_DB else {})
ticketing_engine = create_engine(TICKETING_DATABASE_URL, connect_args={"check_same_thread": False} if USE_IN_MEMORY_DB else {})
analysis_engine = create_engine(ANALYSIS_DATABASE_URL, connect_args={"check_same_thread": False})

# Create session factories
CISSDMSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=cissdm_engine)
TicketingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=ticketing_engine)
AnalysisSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=analysis_engine)

# Base class for SQLAlchemy models
Base = declarative_base()

def get_cissdm_db():
    """Get a CISSDM database session"""
    db = CISSDMSessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_ticketing_db():
    """Get a Ticketing database session"""
    db = TicketingSessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_analysis_db():
    """Get an Analysis database session"""
    db = AnalysisSessionLocal()
    try:
        yield db
    finally:
        db.close() 