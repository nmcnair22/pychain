from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.sql import func
from .base import Base

class TicketChain(Base):
    """Model for ticket chain relationships from the ticketing database"""
    __tablename__ = "ticket_chains"
    # This model maps to the ticketing database

    id = Column(Integer, primary_key=True, index=True)
    ticket_number = Column(String(50), nullable=False, index=True)
    chain_hash = Column(String(100), nullable=False, index=True)
    ticket_type = Column(String(50), nullable=True)  # dispatch, turnup, shipping, project
    created_at = Column(DateTime, default=func.now())
    
    def __repr__(self):
        return f"<TicketChain(ticket_number='{self.ticket_number}', chain_hash='{self.chain_hash}', type='{self.ticket_type}')>" 