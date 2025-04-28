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
            ticket = session.query(Ticket).filter(Ticket.ticketid == int(ticket_id)).first()
            if not ticket:
                logging.error(f"No ticket found with ID: {ticket_id}")
                return None

            # Initialize chain details
            chain_details = {
                "chain_hash": "B924DF9C-E1B9-B945-8442-89CE8F62A268",  # Hardcoded; compute dynamically if needed
                "tickets": []
            }

            # Get related tickets (to be implemented with sw_ticketlinks)
            related_tickets = [ticket]  # Placeholder; add linked tickets later

            # Process each ticket
            for t in related_tickets:
                ticket_data = {
                    "ticket_id": str(t.ticketid),
                    "subject": t.subject,
                    "status": t.ticketstatustitle,
                    "ticket_type": TicketChainService._infer_ticket_type(t.subject, t.ticketstatustitle, t.tickettypeid, t.departmentid),
                    "created": t.dateline,
                    "closed": t.lastactivity,
                    "service_date": None,  # Not in sw_tickets; add via join if needed
                    "site_number": None,   # Not in sw_tickets; add via cis_customers
                    "customer": None,      # Not in sw_tickets; add via cis_customers
                    "location_name": None, # Not in sw_tickets; add via cis_customers
                    "project_id": None,    # Not in sw_tickets; add via cis_projects
                    "posts": [
                        {
                            "post_id": str(p.ticketpostid),
                            "dateline": p.dateline,
                            "fullname": p.fullname,
                            "contents": p.contents,
                            "is_private": bool(p.isprivate)
                        } for p in t.posts
                    ],
                    "notes": [
                        {
                            "note_id": str(n.ticketnoteid),
                            "dateline": n.dateline,
                            "staffname": n.staffname,
                            "contents": n.note
                        } for n in t.notes if n.linktypeid == t.ticketid  # Filter notes by ticketid
                    ],
                    "linked_tickets": []  # Placeholder; add via sw_ticketlinks
                }
                chain_details["tickets"].append(ticket_data)

            return chain_details
        except Exception as e:
            logging.error(f"Error retrieving chain details for ticket {ticket_id}: {e}")
            return None

    @staticmethod
    def _infer_ticket_type(subject: str, status: str, tickettypeid: int, departmentid: int) -> str:
        """
        Infer the ticket type based on subject, status, tickettypeid, and departmentid.
        
        Args:
            subject (str): Ticket subject
            status (str): Ticket status
            tickettypeid (int): Ticket type ID
            departmentid (int): Department ID
            
        Returns:
            str: Inferred ticket type
        """
        subject_lower = subject.lower()
        if "dispatch" in subject_lower or "billing" in subject_lower or departmentid in [/* Add dispatch department IDs */]:
            return "dispatch"
        elif "turnup" in subject_lower or "p1" in subject_lower or "p2" in subject_lower or tickettypeid in [/* Add turnup type IDs */]:
            return "turnup"
        elif "project" in subject_lower or "cabling" in subject_lower or departmentid in [/* Add project department IDs */]:
            return "project_management"
        elif "shipping" in subject_lower or "delivered" in status.lower():
            return "shipping"
        return "other"