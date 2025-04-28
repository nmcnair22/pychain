import logging
import json
from typing import Optional, Dict, List, Any
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from app.models.ticket import Ticket
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
            return result[0]
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
            WITH cte AS (
                SELECT 
                    st3.ticketlinkchainid,
                    st3.dateline AS chain_dateline,
                    st4.ticketid,
                    st4.locationid,
                    st4.tickettypetitle,
                    st4.subject,
                    st4.ticketstatustitle,
                    st4.departmenttitle,
                    st4.fullname,
                    st4.dateline AS ticket_created,
                    (SELECT fieldvalue FROM sw_customfieldvalues WHERE customfieldid = 117 AND typeid = st4.ticketid AND type = 1) AS site_number,
                    (SELECT fieldvalue FROM sw_customfieldvalues WHERE customfieldid = 104 AND typeid = st4.ticketid AND type = 1) AS customer,
                    (SELECT fieldvalue FROM sw_customfieldvalues WHERE customfieldid = 122 AND typeid = st4.ticketid AND type = 1) AS state,
                    (SELECT fieldvalue FROM sw_customfieldvalues WHERE customfieldid = 121 AND typeid = st4.ticketid AND type = 1) AS city,
                    FROM_UNIXTIME(st4.duedate) AS service_date,
                    (SELECT fieldvalue FROM sw_customfieldvalues WHERE customfieldid = 248 AND typeid = st4.ticketid AND type = 1) AS project_id,
                    CASE
                        WHEN st4.resolutiondateline > 0 THEN FROM_UNIXTIME(st4.resolutiondateline)
                        ELSE NULL
                    END AS closed,
                    st4.totalreplies AS total_replies,
                    CASE 
                        WHEN st4.departmenttitle IN ('FST Accounting', 'Dispatch', 'Pro Services') THEN 'Dispatch Tickets'
                        WHEN st4.departmenttitle = 'Turnups' THEN 'Turnup Tickets'
                        WHEN st4.departmenttitle IN ('Shipping', 'Outbound', 'Inbound') THEN 'Shipping Tickets'
                        WHEN st4.departmenttitle = 'Turn up Projects' THEN 'Project Management'
                        ELSE 'Other'
                    END AS TicketCategory,
                    FROM_UNIXTIME(first_post.dateline) AS first_post_date,
                    first_post.fullname AS first_posted_by,
                    first_post.contents AS first_post_content,
                    FROM_UNIXTIME(last_post.dateline) AS last_post_date,
                    last_post.fullname AS last_posted_by,
                    last_post.contents AS last_post_content,
                    st2.chainhash,
                    ROW_NUMBER() OVER (PARTITION BY st4.ticketid ORDER BY st3.dateline DESC) AS row_num
                FROM sw_tickets st
                JOIN sw_ticketlinkchains st2 
                    ON st2.ticketid = st.ticketid
                JOIN sw_ticketlinkchains st3 
                    ON st2.chainhash = st3.chainhash
                    AND st3.ticketid <> st2.ticketid
                JOIN sw_tickets st4 
                    ON st3.ticketid = st4.ticketid
                LEFT JOIN (
                    SELECT tp.ticketid, tp.dateline, tp.fullname, tp.contents
                    FROM sw_ticketposts tp
                    INNER JOIN (
                        SELECT ticketid, MIN(dateline) AS first_dateline
                        FROM sw_ticketposts
                        GROUP BY ticketid
                    ) fp ON tp.ticketid = fp.ticketid AND tp.dateline = fp.first_dateline
                ) first_post ON first_post.ticketid = st4.ticketid
                LEFT JOIN (
                    SELECT tp.ticketid, tp.dateline, tp.fullname, tp.contents
                    FROM sw_ticketposts tp
                    INNER JOIN (
                        SELECT ticketid, MAX(dateline) AS last_dateline
                        FROM sw_ticketposts
                        GROUP BY ticketid
                    ) lp ON tp.ticketid = lp.ticketid AND tp.dateline = lp.last_dateline
                ) last_post ON last_post.ticketid = st4.ticketid
                WHERE st2.chainhash = :chain_hash
                  AND st4.departmenttitle NOT IN ('Add to NPM', 'Helpdesk Tier 1', 'Helpdesk Tier 2', 'Helpdesk Tier 3', 'Engineering')
                  AND st4.tickettypetitle <> '3rd Party Turnup'
            )
            SELECT 
                ticketlinkchainid,
                chain_dateline,
                ticketid,
                locationid,
                tickettypetitle,
                subject,
                ticketstatustitle,
                departmenttitle,
                fullname,
                ticket_created,
                site_number,
                customer,
                state,
                city,
                service_date,
                project_id,
                closed,
                total_replies,
                TicketCategory,
                first_post_date,
                first_posted_by,
                first_post_content,
                last_post_date,
                last_posted_by,
                last_post_content,
                chainhash
            FROM cte 
            WHERE row_num = 1
            ORDER BY TicketCategory, chain_dateline
        """)
        
        result = session.execute(query, {"chain_hash": chain_hash}).fetchall()
        
        tickets = []
        for row in result:
            ticket = {
                "ticketlinkchainid": row.ticketlinkchainid,
                "chain_dateline": row.chain_dateline,
                "ticketid": row.ticketid,
                "locationid": row.locationid,
                "tickettypetitle": row.tickettypetitle,
                "subject": row.subject,
                "ticketstatustitle": row.ticketstatustitle,
                "departmenttitle": row.departmenttitle,
                "fullname": row.fullname,
                "ticket_created": row.ticket_created,
                "site_number": row.site_number,
                "customer": row.customer,
                "state": row.state,
                "city": row.city,
                "service_date": row.service_date,
                "project_id": row.project_id,
                "closed": row.closed,
                "total_replies": row.total_replies,
                "ticket_category": row.TicketCategory,
                "first_post_date": row.first_post_date,
                "first_posted_by": row.first_posted_by,
                "first_post_content": row.first_post_content,
                "last_post_date": row.last_post_date,
                "last_posted_by": row.last_posted_by,
                "last_post_content": row.last_post_content,
                "chainhash": row.chainhash
            }
            
            if ticket["chain_dateline"]:
                ticket["chain_dateline_datetime"] = datetime.datetime.fromtimestamp(ticket["chain_dateline"])
            if ticket["ticket_created"]:
                ticket["ticket_created_datetime"] = datetime.datetime.fromtimestamp(ticket["ticket_created"])
                
            tickets.append(ticket)
        
        return tickets

    @staticmethod
    def get_ticket_details(session: Session, ticket_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information for a specific ticket, including posts and notes
        
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

            # Query posts with GROUP_CONCAT
            posts_query = text("""
                SELECT CONCAT(
                    '[',
                    GROUP_CONCAT(
                        CONCAT(
                            '{"ticketpostid":"', stp.ticketpostid,
                            '", "post_dateline":"', FROM_UNIXTIME(stp.dateline),
                            '", "fullname":"', REPLACE(stp.fullname, '"', '\\"'),
                            '", "contents":"', REPLACE(stp.contents, '"', '\\"'),
                            '", "isprivate":"', stp.isprivate,
                            '"}'
                        )
                    ),
                    ']'
                ) AS posts
                FROM sw_ticketposts stp
                WHERE stp.ticketid = :ticket_id
            """)
            posts_result = session.execute(posts_query, {"ticket_id": ticket_id}).scalar()
            posts = json.loads(posts_result) if posts_result and posts_result != '[]' else []

            # Query notes with GROUP_CONCAT
            notes_query = text("""
                SELECT CONCAT(
                    '[',
                    GROUP_CONCAT(
                        CONCAT(
                            '{"ticketnoteid":"', n.ticketnoteid,
                            '", "note_staffid":"', n.staffid,
                            '", "note_dateline":"', FROM_UNIXTIME(n.dateline),
                            '", "staffname":"', REPLACE(n.staffname, '"', '\\"'),
                            '", "note":"', REPLACE(n.note, '"', '\\"'),
                            '"}'
                        )
                    ),
                    ']'
                ) AS notes
                FROM sw_ticketnotes n
                WHERE n.linktypeid = :ticket_id
            """)
            notes_result = session.execute(notes_query, {"ticket_id": ticket_id}).scalar()
            notes = json.loads(notes_result) if notes_result and notes_result != '[]' else []

            # Return ticket details (core fields populated in get_linked_tickets_by_hash)
            return {
                "ticket_id": str(ticket.ticketid),
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
                    "ticketid": ticket["ticketid"],
                    "locationid": ticket["locationid"],
                    "tickettypetitle": ticket["tickettypetitle"],
                    "subject": ticket["subject"],
                    "ticketstatustitle": ticket["ticketstatustitle"],
                    "departmenttitle": ticket["departmenttitle"],
                    "fullname": ticket["fullname"],
                    "ticket_created": ticket["ticket_created"],
                    "site_number": ticket["site_number"],
                    "customer": ticket["customer"],
                    "state": ticket["state"],
                    "city": ticket["city"],
                    "service_date": ticket["service_date"],
                    "project_id": ticket["project_id"],
                    "closed": ticket["closed"],
                    "total_replies": ticket["total_replies"],
                    "ticket_category": ticket["ticket_category"],
                    "first_post_date": ticket["first_post_date"],
                    "first_posted_by": ticket["first_posted_by"],
                    "first_post_content": ticket["first_post_content"],
                    "last_post_date": ticket["last_post_date"],
                    "last_posted_by": ticket["last_posted_by"],
                    "last_post_content": ticket["last_post_content"],
                    "chain_dateline_datetime": ticket.get("chain_dateline_datetime"),
                    "ticket_created_datetime": ticket.get("ticket_created_datetime")
                })
                tickets_details.append(ticket_details)
        
        return {
            "chain_hash": chain_hash,
            "ticket_count": len(tickets_details),
            "tickets": tickets_details
        }