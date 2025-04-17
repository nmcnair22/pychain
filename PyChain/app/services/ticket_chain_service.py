from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional
from sqlalchemy import text, create_engine
from app.models.ticket_chain import TicketChain
from app.models.dispatch_ticket import DispatchTicket
from app.models.turnup_ticket import TurnupTicket
from .ai_service import AIService
import datetime
from config import CISSDM_DB_CONFIG

class TicketChainService:
    """Service to handle ticket chain operations and analysis"""
    
    @staticmethod
    def get_tickets_by_id(db: Session, ticket_id: str) -> List[Dict[str, Any]]:
        """
        Get all tickets related to the given ticket ID using a single comprehensive query
        
        Args:
            db: Database session
            ticket_id: The ticket ID to find related tickets for
            
        Returns:
            List of dictionaries with complete ticket information
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

                    -- Custom fields
                    CustomFieldText(117, st4.ticketid) AS site_number,
                    CustomFieldDropdown(104, st4.ticketid) AS customer,
                    CustomFieldText(122, st4.ticketid) AS state,
                    CustomFieldText(121, st4.ticketid) AS city,
                    FROM_UNIXTIME(st4.duedate) AS service_date,
                    CustomFieldDropdown(248, st4.ticketid) AS project_id,

                    -- Improved handling of resolution dateline
                    CASE
                        WHEN st4.resolutiondateline > 0 THEN FROM_UNIXTIME(st4.resolutiondateline)
                        ELSE NULL
                    END AS closed,

                    st4.totalreplies AS total_replies,

                    -- Categorize the ticket based on department
                    CASE 
                        WHEN st4.departmenttitle IN ('FST Accounting', 'Dispatch', 'Pro Services') THEN 'Dispatch Tickets'
                        WHEN st4.departmenttitle = 'Turnups' THEN 'Turnup Tickets'
                        WHEN st4.departmenttitle IN ('Shipping', 'Outbound', 'Inbound') THEN 'Shipping Tickets'
                        WHEN st4.departmenttitle = 'Turn up Projects' THEN 'Project Management'
                        ELSE 'Other'
                    END AS TicketCategory,

                    -- First Post Information
                    FROM_UNIXTIME(first_post.dateline) AS first_post_date,
                    first_post.fullname AS first_posted_by,
                    first_post.contents AS first_post_content,

                    -- Last Post Information
                    FROM_UNIXTIME(last_post.dateline) AS last_post_date,
                    last_post.fullname AS last_posted_by,
                    last_post.contents AS last_post_content,
                    
                    -- Extract chain hash for reference
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

                -- First Post join
                LEFT JOIN (
                    SELECT tp.ticketid, tp.dateline, tp.fullname, tp.contents
                    FROM sw_ticketposts tp
                    INNER JOIN (
                        SELECT ticketid, MIN(dateline) AS first_dateline
                        FROM sw_ticketposts
                        GROUP BY ticketid
                    ) fp ON tp.ticketid = fp.ticketid AND tp.dateline = fp.first_dateline
                ) first_post ON first_post.ticketid = st4.ticketid

                -- Last Post join
                LEFT JOIN (
                    SELECT tp.ticketid, tp.dateline, tp.fullname, tp.contents
                    FROM sw_ticketposts tp
                    INNER JOIN (
                        SELECT ticketid, MAX(dateline) AS last_dateline
                        FROM sw_ticketposts
                        GROUP BY ticketid
                    ) lp ON tp.ticketid = lp.ticketid AND tp.dateline = lp.last_dateline
                ) last_post ON last_post.ticketid = st4.ticketid

                WHERE st.ticketid = :ticket_id
                  AND st4.departmenttitle NOT IN ('Add to NPM', 'Helpdesk Tier 1', 'Helpdesk Tier 2', 'Helpdesk Tier 3', 'Engineering')
            )
            SELECT * FROM cte WHERE row_num = 1
            ORDER BY TicketCategory, chain_dateline;
        """)
        
        result = db.execute(query, {"ticket_id": ticket_id}).fetchall()
        
        # Convert the SQLAlchemy result to a list of dictionaries
        tickets = []
        chain_hash = None  # Store the chain hash
        
        # Lists to store ticket IDs for the secondary query
        dispatch_ticket_ids = []
        turnup_ticket_ids = []
        
        for row in result:
            # Extract the chain hash from the first result
            if chain_hash is None and hasattr(row, 'chainhash'):
                chain_hash = row.chainhash
                
            # Create a dictionary from the row
            ticket = {}
            for column in row._mapping:
                ticket[column] = row._mapping[column]
            
            # Store ticket IDs for the secondary query
            if ticket.get('TicketCategory') == 'Dispatch Tickets':
                dispatch_ticket_ids.append(ticket.get('ticketid'))
            elif ticket.get('TicketCategory') == 'Turnup Tickets':
                turnup_ticket_ids.append(ticket.get('ticketid'))
                
            tickets.append(ticket)
        
        # Query the CISSDM database for additional turnup tickets if we have dispatch tickets
        if dispatch_ticket_ids:
            try:
                # Format the dispatch ticket IDs for the IN clause
                dispatch_ids_str = ', '.join(str(id) for id in dispatch_ticket_ids)
                turnup_ids_str = ', '.join(str(id) for id in turnup_ticket_ids) if turnup_ticket_ids else "0"
                
                # Create a connection string for the CISSDM database
                cissdm_conn_str = f"mysql+mysqlconnector://{CISSDM_DB_CONFIG['user']}:{CISSDM_DB_CONFIG['password']}@{CISSDM_DB_CONFIG['host']}:{CISSDM_DB_CONFIG['port']}/{CISSDM_DB_CONFIG['database']}"
                
                # Create a new engine and connection for the CISSDM database
                cissdm_engine = create_engine(cissdm_conn_str)
                
                # Build the query
                additional_turnups_query = text(f"""
                    SELECT
                        t.ticketid AS ticketid,
                        t.DispatchId AS Related_Dispatch_ID,
                        t.subject,
                        t.ticketstatustitle,
                        t.ServiceDate AS service_date,
                        t.ExpectedTimeIn AS expected_time_in,
                        t.InTime AS in_time,
                        t.OutTime AS out_time,
                        t.CustomerName AS customer,
                        t.SiteNumber AS site_number,
                        t.CISTechnicianName AS fullname,
                        t.technicianComment AS technician_comment,
                        t.created_at AS ticket_created,
                        t.closed_at AS closed
                    FROM turnups t
                    WHERE t.DispatchId IN ({dispatch_ids_str})
                    AND t.ticketid NOT IN ({turnup_ids_str})
                """)
                
                # Use the cissdm connection to execute the query
                with cissdm_engine.connect() as conn:
                    additional_turnups = conn.execute(additional_turnups_query).fetchall()
                    
                    # Add any additional turnup tickets found to our list
                    for row in additional_turnups:
                        turnup_ticket = {}
                        for column in row._mapping:
                            turnup_ticket[column] = row._mapping[column]
                        
                        # Add additional fields for consistency with the main query
                        turnup_ticket['TicketCategory'] = 'Turnup Tickets'
                        turnup_ticket['departmenttitle'] = 'Turnups'
                        turnup_ticket['tickettypetitle'] = 'Turnup'
                        turnup_ticket['chain_dateline'] = turnup_ticket.get('ticket_created')
                        
                        # Add the ticket to our list
                        tickets.append(turnup_ticket)
            except Exception as e:
                print(f"Error querying CISSDM database: {e}")
        
        return tickets, chain_hash
    
    @staticmethod
    def get_chain_details_by_ticket_id(db: Session, ticket_id: str) -> Dict[str, Any]:
        """
        Get details for all tickets in the chain that contains the given ticket
        
        Args:
            db: Database session
            ticket_id: Any ticket ID in the chain
            
        Returns:
            Dictionary with chain details and all ticket information
        """
        # Get all tickets with a single query
        tickets, chain_hash = TicketChainService.get_tickets_by_id(db, ticket_id)
        
        if not chain_hash:
            return {"error": f"No chain hash found for ticket ID {ticket_id}"}
        
        if not tickets:
            return {"error": f"No linked tickets found for ticket ID {ticket_id}"}
        
        return {
            "chain_hash": chain_hash,
            "ticket_count": len(tickets),
            "tickets": tickets
        }
    
    @staticmethod
    def analyze_chain_relationships(db: Session, ticket_id: str) -> str:
        """
        Use AI to analyze the relationships between tickets in a chain
        
        Args:
            db: Database session
            ticket_id: Any ticket ID in the chain
            
        Returns:
            Analysis of the ticket chain relationships
        """
        chain_details = TicketChainService.get_chain_details_by_ticket_id(db, ticket_id)
        
        if "error" in chain_details:
            return chain_details["error"]
        
        # Prepare the data for AI analysis
        ai_service = AIService()
        
        # Create a detailed prompt with chain information
        prompt = TicketChainService._create_chain_analysis_prompt(chain_details)
        
        # Send to AI for analysis
        analysis_result = ai_service.analyze_chain(prompt)
        
        return analysis_result
    
    @staticmethod
    def _create_chain_analysis_prompt(chain_details: Dict[str, Any]) -> str:
        """
        Create a prompt for the AI to analyze ticket relationships
        
        Args:
            chain_details: Dictionary with chain details
            
        Returns:
            Prompt string for AI analysis
        """
        # Group tickets by category
        dispatch_tickets = [t for t in chain_details['tickets'] if t['TicketCategory'] == 'Dispatch Tickets']
        turnup_tickets = [t for t in chain_details['tickets'] if t['TicketCategory'] == 'Turnup Tickets']
        shipping_tickets = [t for t in chain_details['tickets'] if t['TicketCategory'] == 'Shipping Tickets']
        project_tickets = [t for t in chain_details['tickets'] if t['TicketCategory'] == 'Project Management']
        other_tickets = [t for t in chain_details['tickets'] if t['TicketCategory'] == 'Other']
        
        # Calculate how many tickets we're actually analyzing
        dispatch_count = len(dispatch_tickets)
        turnup_count = len(turnup_tickets)
        shipping_count = len(shipping_tickets)
        
        prompt = f"""
        I need you to analyze a set of field service tickets ({dispatch_count} dispatch tickets, {turnup_count} turnup tickets, and {shipping_count} shipping tickets) that are linked together in a chain with hash {chain_details['chain_hash']}.
        
        BACKGROUND:
        In our field service system, we have these main types of tickets:
        1. DISPATCH tickets - Initial records created when a service is requested (departments: FST Accounting, Dispatch, Pro Services)
        2. TURNUP tickets - Created when a technician is booked, containing the work details (department: Turnups)
        3. SHIPPING tickets - Records for shipments related to the service work (departments: Shipping, Outbound, Inbound)
        
        Normally, there should be a 1:1 relationship between dispatch and turnup tickets, but for complex
        projects, there can be multiple relationships that aren't properly tracked in the system.
        
        GOAL:
        Based on the information in these tickets, please:
        1. Identify the actual relationships between these tickets
        2. Determine the chronological order of events
        3. Explain which dispatch tickets spawned which turnup tickets
        4. Note any anomalies or issues with the ticket relationships
        5. Provide a clear summary of the entire service history represented by these tickets
        
        NOTE: This chain has {len(project_tickets)} project management tickets and {len(other_tickets)} other related tickets that are excluded from this analysis.
        
        TICKET DETAILS:
        """
        
        # Add Dispatch Tickets
        if dispatch_tickets:
            prompt += "\n\n=== DISPATCH TICKETS ===\n"
            for i, ticket in enumerate(dispatch_tickets, 1):
                prompt += f"""
                --- DISPATCH TICKET {i}: ID {ticket['ticketid']} ---
                Subject: {ticket.get('subject', 'N/A')}
                Type: {ticket.get('tickettypetitle', 'N/A')}
                Status: {ticket.get('ticketstatustitle', 'N/A')}
                Department: {ticket.get('departmenttitle', 'N/A')}
                Customer: {ticket.get('fullname', 'N/A')}
                Created: {ticket.get('ticket_created', 'N/A')}
                Site Number: {ticket.get('site_number', 'N/A')}
                Customer: {ticket.get('customer', 'N/A')}
                Location: {ticket.get('city', 'N/A')}, {ticket.get('state', 'N/A')}
                Service Date: {ticket.get('service_date', 'N/A')}
                Project ID: {ticket.get('project_id', 'N/A')}
                Closed: {ticket.get('closed', 'N/A')}
                First Post: {ticket.get('first_post_date', 'N/A')} by {ticket.get('first_posted_by', 'N/A')}
                Last Post: {ticket.get('last_post_date', 'N/A')} by {ticket.get('last_posted_by', 'N/A')}
                """
                
                # Add first post content if available
                first_content = ticket.get('first_post_content', '')
                if first_content:
                    # Limit post content length
                    if len(first_content) > 150:
                        first_content = first_content[:147] + "..."
                    prompt += f"\nFirst Post Content:\n{first_content}\n"
        
        # Add Turnup Tickets
        if turnup_tickets:
            prompt += "\n\n=== TURNUP TICKETS ===\n"
            for i, ticket in enumerate(turnup_tickets, 1):
                prompt += f"""
                --- TURNUP TICKET {i}: ID {ticket['ticketid']} ---
                Subject: {ticket.get('subject', 'N/A')}
                Type: {ticket.get('tickettypetitle', 'N/A')}
                Status: {ticket.get('ticketstatustitle', 'N/A')}
                Department: {ticket.get('departmenttitle', 'N/A')}
                Technician: {ticket.get('fullname', 'N/A')}
                Created: {ticket.get('ticket_created', 'N/A')}
                Site Number: {ticket.get('site_number', 'N/A')}
                Customer: {ticket.get('customer', 'N/A')}
                Location: {ticket.get('city', 'N/A')}, {ticket.get('state', 'N/A')}
                Service Date: {ticket.get('service_date', 'N/A')}
                Project ID: {ticket.get('project_id', 'N/A')}
                Closed: {ticket.get('closed', 'N/A')}
                First Post: {ticket.get('first_post_date', 'N/A')} by {ticket.get('first_posted_by', 'N/A')}
                Last Post: {ticket.get('last_post_date', 'N/A')} by {ticket.get('last_posted_by', 'N/A')}
                """
                
                # Add last post content if available (for turnups, the last post is often more relevant)
                last_content = ticket.get('last_post_content', '')
                if last_content:
                    # Limit post content length
                    if len(last_content) > 150:
                        last_content = last_content[:147] + "..."
                    prompt += f"\nLast Post Content:\n{last_content}\n"
                
        # Add Shipping Tickets (if any)
        if shipping_tickets:
            prompt += "\n\n=== SHIPPING TICKETS ===\n"
            for i, ticket in enumerate(shipping_tickets, 1):
                prompt += f"""
                --- SHIPPING TICKET {i}: ID {ticket['ticketid']} ---
                Subject: {ticket.get('subject', 'N/A')}
                Type: {ticket.get('tickettypetitle', 'N/A')}
                Status: {ticket.get('ticketstatustitle', 'N/A')}
                Department: {ticket.get('departmenttitle', 'N/A')}
                Coordinator: {ticket.get('fullname', 'N/A')}
                Created: {ticket.get('ticket_created', 'N/A')}
                Site Number: {ticket.get('site_number', 'N/A')}
                Customer: {ticket.get('customer', 'N/A')}
                Location: {ticket.get('city', 'N/A')}, {ticket.get('state', 'N/A')}
                Service Date: {ticket.get('service_date', 'N/A')}
                Project ID: {ticket.get('project_id', 'N/A')}
                Closed: {ticket.get('closed', 'N/A')}
                """
                
        # Add final instructions
        prompt += """
        
        RESPONSE FORMAT:
        1. Timeline of Events: (chronological list of what happened)
        2. Relationship Map: (which dispatch tickets spawned which turnup tickets)
        3. Anomalies/Issues: (any problems or inconsistencies in the ticket relationships)
        4. Summary: (overall description of the service history)
        
        Please analyze all the data in these tickets and explain the relationships between them.
        """
        
        return prompt 