from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional
from sqlalchemy import text, create_engine
from app.models.ticket_chain import TicketChain
from app.models.dispatch_ticket import DispatchTicket
from app.models.turnup_ticket import TurnupTicket
from app.models.ticket import Ticket
from .ai_service import AIService
import datetime
from config import CISSDM_DB_CONFIG
import os
import json
from openai import OpenAI

# Initialize OpenAI client with API key from environment
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

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
                  AND st4.tickettypetitle <> '3rd Party Turnup'
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
        Get all tickets in the same chain as the specified ticket ID
        using a single SQL query.
        
        Args:
            db: Database session
            ticket_id: The ticket ID to find the chain for
            
        Returns:
            Dictionary with chain details and list of tickets
        """
        try:
            # Use a direct SQL query since we're dealing with a complex structure
            tickets, chain_hash = TicketChainService.get_tickets_by_id(db, ticket_id)
            
            if not tickets:
                return {"error": f"Ticket {ticket_id} not found."}
            
            if not chain_hash:
                return {"error": f"Ticket {ticket_id} doesn't have a chain hash."}
                
            return {
                "chain_hash": chain_hash,
                "ticket_count": len(tickets),
                "tickets": tickets
            }
        except Exception as e:
            # Return an error if something goes wrong
            return {"error": str(e)}
    
    @staticmethod
    def analyze_chain_relationships(db: Session, ticket_id: str, report_type: str = "relationship_summary") -> str:
        """
        Analyze relationships between tickets in a chain using OpenAI
        
        Args:
            db: Database session
            ticket_id: The ticket ID to analyze
            report_type: Type of report to generate (relationship_summary or timelines_outcomes)
            
        Returns:
            Analysis text describing the relationships
        """
        # Get the ticket chain
        chain_details = TicketChainService.get_chain_details_by_ticket_id(db, ticket_id)
        
        if "error" in chain_details:
            return f"Error: {chain_details['error']}"
        
        # Create a detailed prompt
        prompt = TicketChainService._create_chain_analysis_prompt(chain_details, report_type)
        
        # Use AIService to analyze the chain
        return AIService.analyze_chain(prompt, report_type)
    
    @staticmethod
    def _create_chain_analysis_prompt(chain_details: Dict[str, Any], report_type: str = "relationship_summary") -> str:
        """
        Create a detailed prompt for OpenAI based on the ticket chain
        
        Args:
            chain_details: Dictionary with chain details and list of tickets
            report_type: Type of report to generate (relationship_summary or timelines_outcomes)
            
        Returns:
            Prompt text for OpenAI
        """
        # Start building the prompt
        if report_type == "relationship_summary":
            prompt = "Please analyze the following chain of service tickets and provide insights about their relationships.\n\n"
            prompt += "For your analysis:\n"
            prompt += "1. Identify the main scope/project and how the tickets relate to each other\n"
            prompt += "2. Identify any dependencies between tickets\n"
            prompt += "3. Summarize what appears to be happening across this chain of tickets\n"
            prompt += "4. Note any issues, delays, or recurrences that appear in the chain\n"
        elif report_type == "timelines_outcomes":
            prompt = "Please analyze the following chain of service tickets and provide a detailed timeline analysis.\n\n"
            prompt += "For your analysis:\n"
            prompt += "1. Create a timeline of events showing each site visit\n"
            prompt += "2. For each visit, describe the specific scope and what was completed\n"
            prompt += "3. Highlight any issues or incomplete work requiring revisits\n"
            prompt += "4. Specifically track cable drops: what was requested vs. what was completed\n"
            prompt += "5. Note any material shortages and their impact\n"
            prompt += "6. Determine if revisits were billable to the client or due to incomplete prior work\n"
            prompt += "7. Conclude with an overall assessment of project efficiency\n"
        else:
            # Default to relationship summary
            prompt = "Please analyze the following chain of service tickets and provide insights about their relationships.\n\n"
        
        # Add the ticket details
        prompt += f"\nTicket Chain ID: {chain_details['chain_hash']}\n"
        prompt += f"Number of Tickets: {chain_details['ticket_count']}\n\n"
        
        # Group tickets by category
        tickets_by_category = {}
        for ticket in chain_details['tickets']:
            category = ticket.get('TicketCategory', 'Unknown')
            if category not in tickets_by_category:
                tickets_by_category[category] = []
            tickets_by_category[category].append(ticket)
        
        # Add tickets grouped by category
        for category, tickets in tickets_by_category.items():
            prompt += f"# {category} Tickets ({len(tickets)}):\n\n"
            
            for ticket in tickets:
                prompt += f"## Ticket ID: {ticket.get('ticketid', 'Unknown')}\n"
                prompt += f"Subject: {ticket.get('subject', 'Unknown')}\n"
                prompt += f"Status: {ticket.get('ticketstatus', 'Unknown')}\n"
                prompt += f"Created: {ticket.get('datecreated', 'Unknown')}\n"
                prompt += f"Modified: {ticket.get('datemodified', 'Unknown')}\n"
                prompt += f"Description: {ticket.get('ticketdescription', 'None')}\n"
                
                # Include any work orders associated with the ticket
                work_orders = ticket.get('work_orders', [])
                if work_orders:
                    prompt += f"Work Orders: {len(work_orders)}\n"
                    for wo in work_orders:
                        prompt += f"- {wo.get('title', 'Unknown')}: {wo.get('description', 'None')}\n"
                
                prompt += "\n"
        
        # End with specific questions based on report type
        if report_type == "relationship_summary":
            prompt += "\nBased on this information, please provide a comprehensive analysis of the relationships between these tickets. Include insights about dependencies, timeline, and any patterns you notice. Make sure to identify the overall project or service these tickets represent."
        elif report_type == "timelines_outcomes":
            prompt += "\nBased on this information, please provide a comprehensive timeline analysis with special focus on scope completion, cable drops, material issues, and the necessity of revisits. Create a clear progression of events that shows what happened at each visit and why additional visits were needed."
            
        return prompt 