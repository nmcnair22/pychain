from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean
from sqlalchemy.sql import func
from .base import Base

class Ticket(Base):
    """Ticket model for database representation of issue tickets from the ticketing system"""
    __tablename__ = "tickets"
    # This model maps to the ticketing database

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    status = Column(String(50), default="new")
    priority = Column(String(50), default="medium")
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    is_analyzed = Column(Boolean, default=False)
    analysis_result = Column(Text, nullable=True)

    def __repr__(self):
        return f"<Ticket(id={self.id}, title='{self.title}', status='{self.status}')>" 