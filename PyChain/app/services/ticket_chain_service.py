import logging
import json
from typing import Optional, Dict, List
from sqlalchemy import create_engine, select, func, text
from sqlalchemy.orm import sessionmaker, Session
from app.models.ticket import Ticket, Posts
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
            # Query the ticket
            ticket = session.query(Ticket).filter(Ticket.ticketid == int(ticket_id)).first()
            if not ticket:
                logging.error(f"No ticket found with ID: {ticket_id}")
                return None

            # Query custom fields from sw_customfieldvalues
            custom_fields_query = select([
                'fieldid', 'value'
            ]).select_from(text('sw_customfieldvalues')).where(
                text('ticketid') == int(ticket_id)
            )
            custom_fields_result = session.execute(custom_fields_query).fetchall()
            custom_fields = {row['fieldid']: row['value'] for row in custom_fields_result}

            # Map custom fields
            site_number = custom_fields.get(117)
            customer = custom_fields.get(104)
            state = custom_fields.get(122)
            city = custom_fields.get(121)
            project_id = custom_fields.get(248)
            location_name = ', '.join(filter(None, [
                custom_fields.get(118),  # Address
                custom_fields.get(121),  # City
                custom_fields.get(122),  # State
                custom_fields.get(123)   # ZIP
            ]))

            # Query last post
            last_post_query = select([text('contents')]).select_from(text('sw_ticketposts')).where(
                text('ticketpostid') == ticket.lastpostid
            )
            last_post_result = session.execute(last_post_query).scalar()

            # Query posts with GROUP_CONCAT
            posts_query = select([
                func.concat(
                    '[',
                    func.group_concat(
                        func.concat(
                            '{"ticketpostid":"', text('stp.ticketpostid'),
                            '", "post_dateline":"', func.from_unixtime(text('stp.dateline')),
                            '", "fullname":"', func.replace(text('stp.fullname'), '"', '\\"'),
                            '", "contents":"', func.replace(text('stp.contents'), '"', '\\"'),
                            '", "isprivate":"', text('stp.isprivate'),
                            '"}'
                        )
                    ),
                    ']'
                ).label('posts')
            ]).select_from(text('sw_ticketposts stp')).where(
                text('stp.ticketid') == int(ticket_id)
            )
            posts_result = session.execute(posts_query).scalar()
            posts = json.loads(posts_result) if posts_result else []

            # Query notes with GROUP_CONCAT
            notes_query = select([
                func.concat(
                    '[',
                    func.group_concat(
                        func.concat(
                            '{"ticketnoteid":"', text('n.ticketnoteid'),
                            '", "note_staffid":"', text('n.staffid'),
                            '", "note_dateline":"', func.from_unixtime(text('n.dateline')),
                            '", "staffname":"', func.replace(text('n.staffname'), '"', '\\"'),
                            '", "note":"', func.replace(text('n.note'), '"', '\\"'),
                            '"}'
                        )
                    ),
                    ']'
                ).label('notes')
            ]).select_from(text('sw_ticketnotes n')).where(
                text('n.linktypeid') == int(ticket_id)
            )
            notes_result = session.execute(notes_query).scalar()
            notes = json.loads(notes_result) if notes_result else []

            # Initialize chain details
            chain_details = {
                "chain_hash": "B924DF9C-E1B9-B945-8442-89CE8F62A268",
                "tickets": []
            }

            # Process ticket
            ticket_data = {
                "ticket_id": str(ticket.ticketid),
                "subject": ticket.subject,
                "site_number": site_number,
                "customer": customer,
                "state": state,
                "city": city,
                "service_date": ticket.duedate,
                "location_name": location_name,
                "location_id": str(ticket.locationid) if ticket.locationid else None,
                "status": ticket.ticketstatustitle,
                "last_post": last_post_result,
                "created": ticket.dateline,
                "closed": ticket.resolutiondateline,
                "project_id": project_id,
                "total_replies": ticket.totalreplies,
                "posts": posts,
                "notes": notes,
                "linked_tickets": []  # Placeholder; add via sw_ticketlinks
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