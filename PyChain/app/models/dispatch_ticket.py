from sqlalchemy import Column, Integer, String, Text, DateTime, Float, ForeignKey
from sqlalchemy.sql import func
from .base import Base

class DispatchTicket(Base):
    """Model for dispatch tickets from the ticketing database"""
    __tablename__ = "dispatch_tickets"
    # This model maps to the ticketing database

    id = Column(Integer, primary_key=True, index=True)
    ticket_number = Column(String(50), nullable=False, unique=True, index=True)
    customer_name = Column(String(255), nullable=True)
    service_date = Column(DateTime, nullable=True)
    site_address = Column(String(500), nullable=True)
    site_city = Column(String(100), nullable=True)
    site_state = Column(String(50), nullable=True)
    site_zip = Column(String(20), nullable=True)
    service_type = Column(String(100), nullable=True)
    status = Column(String(50), nullable=True)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Financial information
    amount_billable = Column(Float, nullable=True)
    amount_payable = Column(Float, nullable=True)
    
    # Additional fields that might help with relationship analysis
    related_turnup_ticket = Column(String(50), nullable=True, index=True)
    accounting_status = Column(String(50), nullable=True)
    
    def __repr__(self):
        return f"<DispatchTicket(ticket_number='{self.ticket_number}', service_date='{self.service_date}', status='{self.status}')>" 