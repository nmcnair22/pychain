from sqlalchemy import Column, Integer, String, Text, DateTime
from sqlalchemy.sql import func
from .base import Base

class User(Base):
    """User model for CISSDM database"""
    __tablename__ = "users"
    # This model maps to the CISSDM database
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(255), nullable=False, unique=True, index=True)
    email = Column(String(255), nullable=True)
    department = Column(String(255), nullable=True)
    role = Column(String(100), nullable=True)
    created_at = Column(DateTime, default=func.now())
    last_login = Column(DateTime, nullable=True)
    
    def __repr__(self):
        return f"<User(id={self.id}, username='{self.username}', department='{self.department}')>" 