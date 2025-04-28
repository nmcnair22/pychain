import logging
import json
from typing import Optional, Dict, List, Any
from sqlalchemy import create_engine, select, func, text
from sqlalchemy.orm import sessionmaker, Session
from app.models.ticket import Ticket, Posts
from config import TICKETING_DATABASE_URL
import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TicketChainService:
    """Service to handle ticket chain operations and analysis"""

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
    def get_chain_hash_by_ticket_id(session: Session, ticket_id: str) -> Optional[str]:
        """
        Get the chain hash for a specific ticket ID
        
        Args:
            session: Database session
            ticket_id: The ticket ID to find the chain hash for
            
        Returns:
            The chain hash string or None if not found
        """
        query = text("""
            SELECT chainhash 
            FROM sw_ticketlinkchains 
            WHERE ticketid = :ticket_id
        """)
        
        result = session.execute(query, {"ticket_id": ticket_id}).first()
        
        if result:
            return result[0]  # Return the chainhash value
        return None

    @staticmethod
    def get_linked_tickets_by_hash(session: Session, chain_hash: str) -> List[Dict[str, Any]]:
        """
        Get all tickets linked by the given chain hash
        
        Args:
            session: Database session
            chain_hash: The chain hash to search for
            
        Returns:
            List of dictionaries with ticket information
        """
        query = text("""
            SELECT 
                tlc.ticketlinkchainid,
                tlc.dateline AS chain_dateline,
                t.ticketid,
                t.tickettypetitle,
                t.subject,
                t.ticketstatustitle,
                t.departmenttitle,
                t.fullname,
                t.dateline AS ticket_created,
                t.lastactivity,
                CASE 
                    WHEN t.departmenttitle IN ('FST Accounting', 'Dispatch', 'Pro Services') THEN 'Dispatch Tickets'
                    WHEN t.departmenttitle = 'Turnups' THEN 'Turnup Tickets'
                    WHEN t.departmenttitle = 'Turn up Projects' THEN 'Project Management'
                    ELSE 'Other'
                END AS TicketCategory
            FROM sw_ticketlinkchains tlc
            JOIN sw_tickets t 
                ON tlc.ticketid = t.ticketid
            WHERE tlc.chainhash = :chain_hash
                AND t.departmenttitle NOT IN ('Add to NPM', 'Helpdesk Tier 1', 'Helpdesk Tier 2', 'Helpdesk Tier 3', 'Engineering')
                AND t.tickettypetitle <> '3rd Party Turnup'
            GROUP BY t.ticketid
            ORDER BY TicketCategory, tlc.dateline
        """)
        
        result = session.execute(query, {"chain_hash": chain_hash}).fetchall()
        
        tickets = []
        for row in result:
            ticket = {
                "ticketlinkchainid": row.ticketlinkchainid,
                "chain_dateline": row.chain_dateline,
                "ticketid": row.ticketid,
                "tickettypetitle": row.tickettypetitle,
                "subject": row.subject,
                "ticketstatustitle": row.ticketstatustitle,
                "departmenttitle": row.departmenttitle,
                "fullname": row.fullname,
                "ticket_created": row.ticket_created,
                "lastactivity": row.lastactivity,
                "ticket_category": row.TicketCategory
            }
            
            if ticket["chain_dateline"]:
                ticket["chain_dateline_datetime"] = datetime.datetime.fromtimestamp(ticket["chain_dateline"])
            if ticket["ticket_created"]:
                ticket["ticket_created_datetime"] = datetime.datetime.fromtimestamp(ticket["ticket_created"])
            if ticket["lastactivity"]:
                ticket["lastactivity_datetime"] = datetime.datetime.fromtimestamp(ticket["lastactivity"])
                
            tickets.append(ticket)
        
        return tickets

    @staticmethod
    def get_ticket_details(session: Session, ticket_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information for a specific ticket, including custom fields, posts, and notes
        
        Args:
            session: Database session
            ticket_id: The ticket ID to retrieve details for
            
        Returns:
            Dictionary with ticket details or None if not found
        """
        try:
            # Query the ticket
            ticket = session.query(Ticket).filter(Ticket.ticketid == int(ticket_id)).first()
            if not ticket:
                logging.error(f"No ticket found with ID: {ticket_id}")
                return None

            # Query custom fields from sw_customfieldvalues
            custom_fields_query = select(
                text('fieldid'), text('value')
            ).select_from(text('sw_customfieldvalues')).where(
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
            last_post_query = select(
                text('contents')
            ).select_from(text('sw_ticketposts')).where(
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
            posts = json.loads(posts_result) if posts_result and posts_result != '[]' else []

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
            notes = json.loads(notes_result) if notes_result and notes_result != '[]' else []

            # Return ticket details
            return {
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
                "linked_tickets": []  # Placeholder; populated in get_chain_details_by_ticket_id
            }
        except Exception as e:
            logging.error(f"Error retrieving ticket details for ticket {ticket_id}: {e}")
            return None

    @staticmethod
    def get_chain_details_by_ticket_id(session: Session, ticket_id: str) -> Dict[str, Any]:
        """
        Get details for all tickets in the chain that contains the given ticket
        
        Args:
            session: Database session
            ticket_id: Any ticket ID in the chain
            
        Returns:
            Dictionary with chain details and all ticket information
        """
        # Get the chain hash
        chain_hash = TicketChainService.get_chain_hash_by_ticket_id(session, ticket_id)
        
        if not chain_hash:
            logging.error(f"No chain hash found for ticket ID {ticket_id}")
            return {"error": f"No chain hash found for ticket ID {ticket_id}"}
        
        # Get all linked tickets
        linked_tickets = TicketChainService.get_linked_tickets_by_hash(session, chain_hash)
        
        if not linked_tickets:
            logging.error(f"No linked tickets found for chain hash {chain_hash}")
            return {"error": f"No linked tickets found for chain hash {chain_hash}"}
        
        # Get detailed information for each ticket
        tickets_details = []
        for ticket in linked_tickets:
            ticket_details = TicketChainService.get_ticket_details(session, str(ticket["ticketid"]))
            if ticket_details:
                # Merge chain metadata with ticket details
                ticket_details.update({
                    "ticketlinkchainid": ticket["ticketlinkchainid"],
                    "chain_dateline": ticket["chain_dateline"],
                    "tickettypetitle": ticket["tickettypetitle"],
                    "departmenttitle": ticket["departmenttitle"],
                    "fullname": ticket["fullname"],
                    "ticket_category": ticket["ticket_category"],
                    "chain_dateline_datetime": ticket.get("chain_dateline_datetime"),
                    "ticket_created_datetime": ticket.get("ticket_created_datetime"),
                    "lastactivity_datetime": ticket.get("lastactivity_datetime")
                })
                tickets_details.append(ticket_details)
        
        return {
            "chain_hash": chain_hash,
            "ticket_count": len(tickets_details),
            "tickets": tickets_details
        }

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