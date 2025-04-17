import datetime
import random
import uuid
import time
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict, Any

from app.models.dispatch_ticket import DispatchTicket
from app.models.turnup_ticket import TurnupTicket
from app.models.ticket_chain import TicketChain

def create_mock_ticket_chain(db: Session, num_dispatch: int = 2, num_turnup: int = 3) -> Dict[str, Any]:
    """
    Create a mock ticket chain for testing with the specified number of dispatch and turnup tickets
    
    Args:
        db: Database session
        num_dispatch: Number of dispatch tickets to create
        num_turnup: Number of turnup tickets to create
        
    Returns:
        Dictionary with information about the created chain
    """
    # Generate a unique chain hash in the format used by the real system
    chain_hash = str(uuid.uuid4()).upper()
    
    # Current Unix timestamp
    current_timestamp = int(time.time())
    
    # Lists to store created ticket IDs
    dispatch_tickets = []
    turnup_tickets = []
    
    # Create mock data directly in the database tables using SQL
    
    # 1. Create dispatch tickets
    for i in range(num_dispatch):
        # Generate a unique ticket ID
        ticket_id = random.randint(2000000, 2999999)
        
        # Insert into sw_tickets table
        ticket_query = text("""
            INSERT INTO sw_tickets 
            (ticketid, subject, tickettypetitle, ticketstatustitle, departmenttitle, fullname, dateline, lastactivity) 
            VALUES 
            (:ticketid, :subject, :tickettypetitle, :ticketstatustitle, :departmenttitle, :fullname, :dateline, :lastactivity)
        """)
        
        # Select a department for this dispatch ticket
        dept = random.choice(['FST Accounting', 'Dispatch', 'Pro Services'])
        
        # Create ticket with varying timestamps for chronological testing
        dateline = current_timestamp - random.randint(5, 30) * 86400  # 5-30 days ago
        lastactivity = dateline + random.randint(1, 10) * 86400  # 1-10 days after creation
        
        db.execute(ticket_query, {
            "ticketid": ticket_id,
            "subject": f"Test Dispatch {i+1}: Installation at Customer Site",
            "tickettypetitle": "Service Request",
            "ticketstatustitle": random.choice(["Open", "In Progress", "Complete", "Pending"]),
            "departmenttitle": dept,
            "fullname": f"Customer {i+1}",
            "dateline": dateline,
            "lastactivity": lastactivity
        })
        
        # Insert into sw_ticketlinkchains table
        chain_query = text("""
            INSERT INTO sw_ticketlinkchains 
            (ticketid, chainhash, dateline, ticketlinktypeid) 
            VALUES 
            (:ticketid, :chainhash, :dateline, :ticketlinktypeid)
        """)
        
        db.execute(chain_query, {
            "ticketid": ticket_id,
            "chainhash": chain_hash,
            "dateline": dateline,
            "ticketlinktypeid": 2  # Assuming 2 is a standard link type ID
        })
        
        # Add to our list
        dispatch_tickets.append(str(ticket_id))
        
        # Create some mock posts for this ticket
        posts_query = text("""
            INSERT INTO sw_ticketposts 
            (ticketid, contents, fullname, dateline, isprivate) 
            VALUES 
            (:ticketid, :contents, :fullname, :dateline, :isprivate)
        """)
        
        # Initial post
        db.execute(posts_query, {
            "ticketid": ticket_id,
            "contents": f"Initial dispatch request for service. Customer needs installation at site. This is a test dispatch ticket {i+1}.",
            "fullname": "Dispatcher Name",
            "dateline": dateline,
            "isprivate": 0
        })
        
        # Follow-up post
        db.execute(posts_query, {
            "ticketid": ticket_id,
            "contents": f"Scheduled for next available technician. Will coordinate with customer for access.",
            "fullname": "Coordinator Name",
            "dateline": dateline + 86400,  # 1 day later
            "isprivate": 0
        })
    
    # 2. Create turnup tickets
    for i in range(num_turnup):
        # Generate a unique ticket ID
        ticket_id = random.randint(3000000, 3999999)
        
        # Assign to a random dispatch ticket
        related_dispatch_id = int(random.choice(dispatch_tickets))
        
        # Get the dateline of the dispatch ticket
        related_query = text("SELECT dateline FROM sw_tickets WHERE ticketid = :ticketid")
        dispatch_date_result = db.execute(related_query, {"ticketid": related_dispatch_id}).first()
        dispatch_date = dispatch_date_result[0] if dispatch_date_result else current_timestamp - 15 * 86400
        
        # Turnup tickets are created after dispatch tickets
        dateline = dispatch_date + random.randint(1, 5) * 86400  # 1-5 days after dispatch
        lastactivity = dateline + random.randint(1, 10) * 86400  # 1-10 days after creation
        
        # Insert into sw_tickets table
        ticket_query = text("""
            INSERT INTO sw_tickets 
            (ticketid, subject, tickettypetitle, ticketstatustitle, departmenttitle, fullname, dateline, lastactivity) 
            VALUES 
            (:ticketid, :subject, :tickettypetitle, :ticketstatustitle, :departmenttitle, :fullname, :dateline, :lastactivity)
        """)
        
        db.execute(ticket_query, {
            "ticketid": ticket_id,
            "subject": f"Turnup for Dispatch #{related_dispatch_id}",
            "tickettypetitle": "Turnup",
            "ticketstatustitle": random.choice(["Scheduled", "In Progress", "Complete", "Canceled"]),
            "departmenttitle": "Turnups",
            "fullname": f"Tech {random.choice(['Alice', 'Bob', 'Charlie', 'Diana'])}",
            "dateline": dateline,
            "lastactivity": lastactivity
        })
        
        # Insert into sw_ticketlinkchains table
        chain_query = text("""
            INSERT INTO sw_ticketlinkchains 
            (ticketid, chainhash, dateline, ticketlinktypeid) 
            VALUES 
            (:ticketid, :chainhash, :dateline, :ticketlinktypeid)
        """)
        
        db.execute(chain_query, {
            "ticketid": ticket_id,
            "chainhash": chain_hash,
            "dateline": dateline,
            "ticketlinktypeid": 2  # Assuming 2 is a standard link type ID
        })
        
        # Add to our list
        turnup_tickets.append(str(ticket_id))
        
        # Create some mock posts for this ticket
        posts_query = text("""
            INSERT INTO sw_ticketposts 
            (ticketid, contents, fullname, dateline, isprivate) 
            VALUES 
            (:ticketid, :contents, :fullname, :dateline, :isprivate)
        """)
        
        # Initial post
        db.execute(posts_query, {
            "ticketid": ticket_id,
            "contents": f"Technician scheduled for service call. Will arrive between 9am-12pm. This is turnup ticket {i+1} for dispatch {related_dispatch_id}.",
            "fullname": "Scheduler Name",
            "dateline": dateline,
            "isprivate": 0
        })
        
        # Work performed post
        work_results = random.choice([
            "Installed new equipment according to specifications. Customer signed off on work.",
            "Diagnosed and repaired fault in existing system. System is now operational.",
            "Could not complete all tasks due to missing parts. Follow-up visit needed.",
            "Site access issues delayed work completion. Rescheduling needed."
        ])
        
        db.execute(posts_query, {
            "ticketid": ticket_id,
            "contents": f"Work performed: {work_results}",
            "fullname": f"Tech {random.choice(['Alice', 'Bob', 'Charlie', 'Diana'])}",
            "dateline": dateline + 28800,  # 8 hours later
            "isprivate": 0
        })
    
    # 3. Create a project management ticket if we have complex enough scenario
    if num_dispatch > 1 and num_turnup > 2:
        # Generate a unique ticket ID
        project_id = random.randint(4000000, 4999999)
        
        # Project tickets are often created early in the process
        dateline = current_timestamp - random.randint(20, 40) * 86400  # 20-40 days ago
        lastactivity = current_timestamp - random.randint(1, 5) * 86400  # 1-5 days ago
        
        # Insert into sw_tickets table
        ticket_query = text("""
            INSERT INTO sw_tickets 
            (ticketid, subject, tickettypetitle, ticketstatustitle, departmenttitle, fullname, dateline, lastactivity) 
            VALUES 
            (:ticketid, :subject, :tickettypetitle, :ticketstatustitle, :departmenttitle, :fullname, :dateline, :lastactivity)
        """)
        
        db.execute(ticket_query, {
            "ticketid": project_id,
            "subject": f"Project Management for Multi-Phase Installation",
            "tickettypetitle": "Project",
            "ticketstatustitle": "In Progress",
            "departmenttitle": "Turn up Projects",
            "fullname": "Project Manager",
            "dateline": dateline,
            "lastactivity": lastactivity
        })
        
        # Insert into sw_ticketlinkchains table
        chain_query = text("""
            INSERT INTO sw_ticketlinkchains 
            (ticketid, chainhash, dateline, ticketlinktypeid) 
            VALUES 
            (:ticketid, :chainhash, :dateline, :ticketlinktypeid)
        """)
        
        db.execute(chain_query, {
            "ticketid": project_id,
            "chainhash": chain_hash,
            "dateline": dateline,
            "ticketlinktypeid": 2  # Assuming 2 is a standard link type ID
        })
        
        # Create some mock posts for this ticket
        posts_query = text("""
            INSERT INTO sw_ticketposts 
            (ticketid, contents, fullname, dateline, isprivate) 
            VALUES 
            (:ticketid, :contents, :fullname, :dateline, :isprivate)
        """)
        
        # Initial post
        db.execute(posts_query, {
            "ticketid": project_id,
            "contents": f"Project initialized for multi-phase installation. Will coordinate all dispatch and turnup tickets under this project.",
            "fullname": "Project Manager",
            "dateline": dateline,
            "isprivate": 0
        })
        
        # Status update post
        all_tickets = dispatch_tickets + turnup_tickets
        tickets_str = ", ".join(all_tickets)
        
        db.execute(posts_query, {
            "ticketid": project_id,
            "contents": f"Phase 1 of project in progress. Related tickets: {tickets_str}",
            "fullname": "Project Manager",
            "dateline": dateline + 604800,  # 1 week later
            "isprivate": 0
        })
    
    # Commit all changes
    db.commit()
    
    # Return information about the created chain
    return {
        "chain_hash": chain_hash,
        "dispatch_tickets": dispatch_tickets,
        "turnup_tickets": turnup_tickets,
        "example_ticket": dispatch_tickets[0]  # Return one ticket to use for queries
    } 