from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional, Tuple
import re
from app.models.analysis_result import AnalysisResult

class AnalysisService:
    """Service to handle storage and retrieval of ticket chain analysis results"""
    
    @staticmethod
    def save_analysis(db: Session, ticket_id: str, chain_hash: str, ticket_count: int, 
                     full_analysis: str) -> AnalysisResult:
        """
        Save the analysis result to the database
        
        Args:
            db: Database session
            ticket_id: The ID of the ticket that was analyzed
            chain_hash: The chain hash for the ticket
            ticket_count: The number of tickets in the chain
            full_analysis: The full text of the analysis
            
        Returns:
            The created AnalysisResult object
        """
        # Parse the analysis into sections
        sections = AnalysisService._parse_analysis_sections(full_analysis)
        
        # Create a new analysis result
        analysis_result = AnalysisResult(
            ticket_id=ticket_id,
            chain_hash=chain_hash,
            ticket_count=ticket_count,
            timeline_events=sections.get('timeline', ''),
            relationship_map=sections.get('relationship', ''),
            anomalies_issues=sections.get('anomalies', ''),
            service_summary=sections.get('summary', ''),
            full_analysis=full_analysis
        )
        
        # Save to database
        db.add(analysis_result)
        db.commit()
        db.refresh(analysis_result)
        
        return analysis_result
    
    @staticmethod
    def get_all_analyses(db: Session, skip: int = 0, limit: int = 100) -> List[AnalysisResult]:
        """Get all analysis results from the database"""
        return db.query(AnalysisResult).order_by(AnalysisResult.created_at.desc()).offset(skip).limit(limit).all()
    
    @staticmethod
    def get_analysis_by_ticket_id(db: Session, ticket_id: str) -> Optional[AnalysisResult]:
        """Get an analysis result for a specific ticket ID"""
        return db.query(AnalysisResult).filter(AnalysisResult.ticket_id == ticket_id).first()
    
    @staticmethod
    def get_analyses_by_chain_hash(db: Session, chain_hash: str) -> List[AnalysisResult]:
        """Get all analysis results for a specific chain hash"""
        return db.query(AnalysisResult).filter(AnalysisResult.chain_hash == chain_hash).all()
    
    @staticmethod
    def _parse_analysis_sections(analysis_text: str) -> Dict[str, str]:
        """
        Parse the analysis text into separate sections
        
        Args:
            analysis_text: The full analysis text
            
        Returns:
            Dictionary containing sections of the analysis
        """
        sections = {}
        
        # Define section markers to search for
        section_patterns = [
            ('timeline', r'(?:##? ?1\.? ?TIMELINE OF EVENTS:?)(.*?)(?:##? ?2\.?|$)'),
            ('relationship', r'(?:##? ?2\.? ?RELATIONSHIP MAP:?)(.*?)(?:##? ?3\.?|$)'),
            ('anomalies', r'(?:##? ?3\.? ?ANOMALIES AND ISSUES:?)(.*?)(?:##? ?4\.?|$)'),
            ('summary', r'(?:##? ?4\.? ?SERVICE SUMMARY:?)(.*?)(?:$)')
        ]
        
        # Extract each section using regex with dotall flag to match across line breaks
        for section_name, pattern in section_patterns:
            match = re.search(pattern, analysis_text, re.DOTALL | re.IGNORECASE)
            if match:
                # Clean up the extracted text
                content = match.group(1).strip()
                sections[section_name] = content
            else:
                sections[section_name] = ""
        
        return sections 