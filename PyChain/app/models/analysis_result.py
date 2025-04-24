from sqlalchemy import Column, Integer, String, Text, DateTime
from sqlalchemy.sql import func
from .base import Base

class AnalysisResult(Base):
    """Model for storing ticket chain analysis results"""
    __tablename__ = "analysis_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticket_id = Column(String(50), nullable=False, index=True)
    chain_hash = Column(String(100), nullable=True)
    ticket_count = Column(Integer, nullable=True)
    
    # Analysis sections
    timeline_events = Column(Text, nullable=True)
    relationship_map = Column(Text, nullable=True)
    anomalies_issues = Column(Text, nullable=True)
    service_summary = Column(Text, nullable=True)
    
    # Full analysis
    full_analysis = Column(Text, nullable=True)
    
    # Metadata
    created_at = Column(DateTime, default=func.now())
    
    def __repr__(self):
        return f"<AnalysisResult(ticket_id='{self.ticket_id}', chain_hash='{self.chain_hash}')>" 