from sqlalchemy.orm import Session
from app.models.ticket import Ticket
from .ai_service import AIService

class TicketService:
    """Service to handle ticket operations"""
    
    @staticmethod
    def get_all_tickets(db: Session, skip: int = 0, limit: int = 100):
        """Get all tickets from the database"""
        return db.query(Ticket).offset(skip).limit(limit).all()
    
    @staticmethod
    def get_ticket_by_id(db: Session, ticket_id: int):
        """Get a specific ticket by ID"""
        return db.query(Ticket).filter(Ticket.id == ticket_id).first()
    
    @staticmethod
    def create_ticket(db: Session, ticket_data: dict):
        """Create a new ticket"""
        ticket = Ticket(**ticket_data)
        db.add(ticket)
        db.commit()
        db.refresh(ticket)
        return ticket
    
    @staticmethod
    def update_ticket(db: Session, ticket_id: int, ticket_data: dict):
        """Update an existing ticket"""
        ticket = TicketService.get_ticket_by_id(db, ticket_id)
        if not ticket:
            return None
        
        for key, value in ticket_data.items():
            setattr(ticket, key, value)
        
        db.commit()
        db.refresh(ticket)
        return ticket
    
    @staticmethod
    def delete_ticket(db: Session, ticket_id: int):
        """Delete a ticket"""
        ticket = TicketService.get_ticket_by_id(db, ticket_id)
        if not ticket:
            return False
        
        db.delete(ticket)
        db.commit()
        return True
    
    @staticmethod
    def analyze_ticket(db: Session, ticket_id: int):
        """Analyze a ticket using AI service"""
        ticket = TicketService.get_ticket_by_id(db, ticket_id)
        if not ticket:
            return None
        
        analysis_result = AIService.analyze_ticket(ticket)
        
        # Update ticket with analysis results
        ticket.is_analyzed = True
        ticket.analysis_result = analysis_result
        db.commit()
        db.refresh(ticket)
        
        return ticket 