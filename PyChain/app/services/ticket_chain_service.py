from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional
from sqlalchemy import text
from app.models.ticket_chain import TicketChain
from app.models.dispatch_ticket import DispatchTicket
from app.models.turnup_ticket import TurnupTicket
from .ai_service import AIService
import datetime

class TicketChainService:
    """Service to handle ticket chain operations and analysis"""
    
    @staticmethod
    def get_chain_hash_by_ticket_id(db: Session, ticket_id: str) -> Optional[str]:
        """
        Get the chain hash for a specific ticket ID
        
        Args:
            db: Database session
            ticket_id: The ticket ID to find the chain hash for
            
        Returns:
            The chain hash string or None if not found
        """
        query = text("""
            SELECT chainhash 
            FROM sw_ticketlinkchains 
            WHERE ticketid = :ticket_id
        """)
        
        result = db.execute(query, {"ticket_id": ticket_id}).first()
        
        if result:
            return result[0]  # Return the chainhash value
        return None
    
    @staticmethod
    def get_linked_tickets_by_hash(db: Session, chain_hash: str) -> List[Dict[str, Any]]:
        """
        Get all tickets linked by the given chain hash
        
        Args:
            db: Database session
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
                -- Categorize the ticket based on its department
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
                -- Ignore specific departments that are not of interest
                AND t.departmenttitle NOT IN ('Add to NPM', 'Helpdesk Tier 1', 'Helpdesk Tier 2', 'Helpdesk Tier 3', 'Engineering')
                -- Exclude 3rd Party Turnup tickets
                AND t.tickettypetitle <> '3rd Party Turnup'
            GROUP BY t.ticketid
            ORDER BY TicketCategory, tlc.dateline
        """)
        
        result = db.execute(query, {"chain_hash": chain_hash}).fetchall()
        
        # Convert the SQLAlchemy result to a list of dictionaries
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
            
            # Convert Unix timestamps to datetime objects
            if ticket["chain_dateline"]:
                ticket["chain_dateline_datetime"] = datetime.datetime.fromtimestamp(ticket["chain_dateline"])
            if ticket["ticket_created"]:
                ticket["ticket_created_datetime"] = datetime.datetime.fromtimestamp(ticket["ticket_created"])
            if ticket["lastactivity"]:
                ticket["lastactivity_datetime"] = datetime.datetime.fromtimestamp(ticket["lastactivity"])
                
            tickets.append(ticket)
        
        return tickets
    
    @staticmethod
    def get_ticket_posts(db: Session, ticket_id: str, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Get the posts (conversation/notes) from a ticket
        
        Args:
            db: Database session
            ticket_id: The ticket ID to get posts for
            limit: Maximum number of posts to retrieve (default 5)
            
        Returns:
            List of dictionaries with post information
        """
        query = text("""
            SELECT 
                p.ticketpostid,
                p.ticketid,
                p.contents,
                p.fullname,
                p.dateline,
                p.isprivate
            FROM sw_ticketposts p
            WHERE p.ticketid = :ticket_id
            ORDER BY p.dateline
            LIMIT :limit
        """)
        
        result = db.execute(query, {"ticket_id": ticket_id, "limit": limit}).fetchall()
        
        posts = []
        for row in result:
            post = {
                "ticketpostid": row.ticketpostid,
                "ticketid": row.ticketid,
                "contents": row.contents,
                "fullname": row.fullname,
                "dateline": row.dateline,
                "isprivate": row.isprivate
            }
            
            # Convert Unix timestamp to datetime
            if post["dateline"]:
                post["dateline_datetime"] = datetime.datetime.fromtimestamp(post["dateline"])
                
            posts.append(post)
        
        return posts
    
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
        # First, get the chain hash for this ticket
        chain_hash = TicketChainService.get_chain_hash_by_ticket_id(db, ticket_id)
        
        if not chain_hash:
            return {"error": f"No chain hash found for ticket ID {ticket_id}"}
        
        # Next, get all tickets linked by this chain hash
        linked_tickets = TicketChainService.get_linked_tickets_by_hash(db, chain_hash)
        
        if not linked_tickets:
            return {"error": f"No linked tickets found for chain hash {chain_hash}"}
        
        # Get additional information for each ticket
        for ticket in linked_tickets:
            # Get the first few posts for this ticket
            ticket["posts"] = TicketChainService.get_ticket_posts(db, ticket["ticketid"], 2)
        
        return {
            "chain_hash": chain_hash,
            "ticket_count": len(linked_tickets),
            "tickets": linked_tickets
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
        dispatch_tickets = [t for t in chain_details['tickets'] if t['ticket_category'] == 'Dispatch Tickets']
        turnup_tickets = [t for t in chain_details['tickets'] if t['ticket_category'] == 'Turnup Tickets']
        project_tickets = [t for t in chain_details['tickets'] if t['ticket_category'] == 'Project Management']
        other_tickets = [t for t in chain_details['tickets'] if t['ticket_category'] == 'Other']
        
        # Calculate how many tickets we're actually analyzing
        dispatch_count = len(dispatch_tickets)
        turnup_count = len(turnup_tickets)
        
        prompt = f"""
        I need you to analyze a set of field service tickets ({dispatch_count} dispatch tickets and {turnup_count} turnup tickets) that are linked together in a chain with hash {chain_details['chain_hash']}.
        
        BACKGROUND:
        In our field service system, we have two main types of tickets:
        1. DISPATCH tickets - Initial records created when a service is requested (departments: FST Accounting, Dispatch, Pro Services)
        2. TURNUP tickets - Created when a technician is booked, containing the work details (department: Turnups)
        
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
                Created: {ticket.get('ticket_created_datetime', 'N/A')}
                Last Activity: {ticket.get('lastactivity_datetime', 'N/A')}
                """
                
                # Add only a limited number of posts if available
                if ticket.get('posts'):
                    prompt += "\nPosts/Notes:\n"
                    # Only include the first post to save tokens
                    first_post = ticket['posts'][0] if ticket['posts'] else None
                    if first_post:
                        prompt += f"- {first_post.get('dateline_datetime', 'N/A')} by {first_post.get('fullname', 'N/A')}:\n"
                        # Limit post content length
                        content = first_post.get('contents', 'N/A')
                        if len(content) > 150:
                            content = content[:147] + "..."
                        prompt += f"  {content}\n"
        
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
                Created: {ticket.get('ticket_created_datetime', 'N/A')}
                Last Activity: {ticket.get('lastactivity_datetime', 'N/A')}
                """
                
                # Add only a limited number of posts if available
                if ticket.get('posts'):
                    prompt += "\nPosts/Notes:\n"
                    # Only include the first post to save tokens
                    first_post = ticket['posts'][0] if ticket['posts'] else None
                    if first_post:
                        prompt += f"- {first_post.get('dateline_datetime', 'N/A')} by {first_post.get('fullname', 'N/A')}:\n"
                        # Limit post content length
                        content = first_post.get('contents', 'N/A')
                        if len(content) > 150:
                            content = content[:147] + "..."
                        prompt += f"  {content}\n"
        
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