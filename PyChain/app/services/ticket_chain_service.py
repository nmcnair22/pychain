import logging
import json
from typing import Optional, Dict, List
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from app.models.ticket import Ticket, Posts, Notes
from config import TICKETING_DATABASE_URL

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TicketChainService:
    """Service to handle ticket chain data retrieval and processing"""

    @staticmethod
    def get_db_session(db_type: str = "primary") -> Optional[Session]:
        """
        Create and return a database session.
        
        Args:
            db_type (str): Type of database to connect to (default: 'primary')
            
        Returns:
            Optional[Session]: SQLAlchemy session object or None if connection fails
        """
        try:
            engine = create_engine(TICKETING_DATABASE_URL)
            SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            session = SessionLocal()
            logging.info(f"{db_type.capitalize()} database session created successfully.")
            return session
        except Exception as e:
            logging.error(f"Failed to create {db_type} database session: {e}")
            return None

    @staticmethod
    def get_chain_details_by_ticket_id(session: Session, ticket_id: str) -> Optional[Dict]:
        """
        Retrieve detailed ticket chain information for a given ticket ID.
        
        Args:
            session (Session): SQLAlchemy session
            ticket_id (str): ID of the ticket to retrieve
            
        Returns:
            Optional[Dict]: Dictionary containing chain details or None if not found
        """
        try:
            # Query the ticket and its related data
            ticket = session.query(Ticket).filter(Ticket.ticketid == ticket_id).first()
            if not ticket:
                logging.error(f"No ticket found with ID: {ticket_id}")
                return None

            # Initialize chain details
            chain_details = {
                "chain_hash": "B924DF9C-E1B9-B945-8442-89CE8F62A268",  # Hardcoded for this example; ideally compute dynamically
                "tickets": []
            }

            # Get related tickets (assuming linked_tickets contains related ticket IDs)
            related_tickets = [ticket]
            if ticket.linked_tickets:
                try:
                    linked_ticket_ids = [lt["linked_ticket"] for lt in json.loads(ticket.linked_tickets)]
                    related_tickets.extend(
                        session.query(Ticket).filter(Ticket.ticketid.in_(linked_ticket_ids)).all()
                    )
                except json.JSONDecodeError as e:
                    logging.error(f"Failed to parse linked_tickets for ticket {ticket_id}: {e}")

            # Process each ticket
            for t in related_tickets:
                ticket_data = {
                    "ticket_id": t.ticketid,
                    "subject": t.subject,
                    "status": t.status,
                    "ticket_type": TicketChainService._infer_ticket_type(t.subject, t.status),
                    "created": t.created.isoformat() if t.created else None,
                    "closed": t.closed.isoformat() if t.closed else None,
                    "service_date": t.service_date.isoformat() if t.service_date and t.service_date.year > 1970 else None,
                    "site_number": t.site_number,
                    "customer": t.customer,
                    "location_name": t.location_name,
                    "project_id": t.project_id,
                    "posts": [
                        {
                            "post_id": p.ticketpostid,
                            "dateline": p.post_dateline.isoformat() if p.post_dateline else None,
                            "fullname": p.fullname,
                            "contents": p.contents,
                            "is_private": p.isprivate
                        } for p in t.posts
                    ],
                    "notes": [
                        {
                            "note_id": n.ticketnoteid,
                            "dateline": n.note_dateline.isoformat() if n.note_dateline else None,
                            "staffname": n.staffname,
                            "contents": n.note
                        } for n in t.notes
                    ],
                    "linked_tickets": json.loads(t.linked_tickets) if t.linked_tickets else []
                }
                chain_details["tickets"].append(ticket_data)

            return chain_details
        except Exception as e:
            logging.error(f"Error retrieving chain details for ticket {ticket_id}: {e}")
            return None

    @staticmethod
    def _infer_ticket_type(subject: str, status: str) -> str:
        """
        Infer the ticket type based on subject and status.
        
        Args:
            subject (str): Ticket subject
            status (str): Ticket status
            
        Returns:
            str: Inferred ticket type
        """
        subject_lower = subject.lower()
        if "dispatch" in subject_lower or "billing" in subject_lower:
            return "dispatch"
        elif "turnup" in subject_lower or "p1" in subject_lower or "p2" in subject_lower:
            return "turnup"
        elif "project" in subject_lower or "cabling" in subject_lower:
            return "project_management"
        elif "shipping" in subject_lower or "delivered" in status.lower():
            return "shipping"
        return "other"