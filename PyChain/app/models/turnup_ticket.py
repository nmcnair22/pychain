from sqlalchemy import Column, Integer, String, Text, DateTime, Float, ForeignKey
from sqlalchemy.sql import func
from .base import Base

class TurnupTicket(Base):
    """Model for turnup tickets from the ticketing database"""
    __tablename__ = "turnup_tickets"
    # This model maps to the ticketing database

    id = Column(Integer, primary_key=True, index=True)
    ticket_number = Column(String(50), nullable=False, unique=True, index=True)
    dispatch_ticket_number = Column(String(50), nullable=True, index=True)
    technician_name = Column(String(255), nullable=True)
    service_date = Column(DateTime, nullable=True)
    time_in = Column(DateTime, nullable=True)
    time_out = Column(DateTime, nullable=True)
    status = Column(String(50), nullable=True)
    work_performed = Column(Text, nullable=True)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Financial information
    technician_cost = Column(Float, nullable=True)
    materials_cost = Column(Float, nullable=True)
    
    def __repr__(self):
        return f"<TurnupTicket(ticket_number='{self.ticket_number}', dispatch_ticket='{self.dispatch_ticket_number}', service_date='{self.service_date}')>" 