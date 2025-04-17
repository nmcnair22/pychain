import os
import sys
from sqlalchemy.exc import OperationalError

# Add the parent directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from app.models.base import Base, get_cissdm_db, get_ticketing_db, cissdm_engine, ticketing_engine
from app.services.ticket_service import TicketService
from app.services.user_service import UserService
from app.services.ai_service import AIService
from app.models.ticket import Ticket
from app.models.user import User
from config import USE_IN_MEMORY_DB

def create_tables():
    """Create database tables from models"""
    try:
        # Create tables for CISSDM database
        Base.metadata.create_all(bind=cissdm_engine)
        # Create tables for Ticketing database
        Base.metadata.create_all(bind=ticketing_engine)
        
        if USE_IN_MEMORY_DB:
            print("Using in-memory SQLite databases for development.")
        else:
            print("Connected to CISSDM and Ticketing databases.")
            
        print("Database tables created successfully.")
    except OperationalError as e:
        print(f"Error: Cannot connect to database - {e}")
        if not USE_IN_MEMORY_DB:
            print("Make sure you have:")
            print("  1. Network access to the database servers")
            print("  2. Correct database configuration in .env file")
            print("  3. Proper permissions to access the databases")
            print("\nFalling back to in-memory database...")
            global USE_IN_MEMORY_DB
            USE_IN_MEMORY_DB = True
            create_tables()  # Try again with in-memory database
        else:
            sys.exit(1)

def create_sample_data(cissdm_db, ticketing_db):
    """Create sample data for testing with in-memory database"""
    if not USE_IN_MEMORY_DB:
        print("Skip creating sample data in production mode.")
        return None, None
    
    # Create sample user in CISSDM database
    sample_user = {
        "username": "jdoe",
        "email": "jdoe@example.com",
        "department": "IT Support",
        "role": "Technician"
    }
    user = UserService.create_user(cissdm_db, sample_user)
    print(f"Created sample user: {user.username}")
    
    # Create sample ticket in Ticketing database
    sample_ticket = {
        "title": "Application crashes on startup",
        "description": "The application crashes immediately when opened on MacOS. Error log shows a segmentation fault in the main thread.",
        "priority": "high",
        "status": "new"
    }
    ticket = TicketService.create_ticket(ticketing_db, sample_ticket)
    print(f"Created sample ticket with ID: {ticket.id}")
    
    return user, ticket

def analyze_ticket_example(db, ticket_id):
    """Example of analyzing a ticket with OpenAI"""
    print(f"Analyzing ticket ID: {ticket_id}")
    
    analyzed_ticket = TicketService.analyze_ticket(db, ticket_id)
    
    if analyzed_ticket:
        print("\n=== Analysis Result ===")
        print(analyzed_ticket.analysis_result)
        print("======================\n")
    else:
        print(f"Ticket ID {ticket_id} not found")

def main():
    """Main application function"""
    print("Starting PyChain...")
    
    # Create database tables
    create_tables()
    
    # Get database sessions
    cissdm_db_generator = get_cissdm_db()
    ticketing_db_generator = get_ticketing_db()
    cissdm_db = next(cissdm_db_generator)
    ticketing_db = next(ticketing_db_generator)
    
    try:
        if USE_IN_MEMORY_DB:
            # Create sample data for testing
            user, ticket = create_sample_data(cissdm_db, ticketing_db)
            
            # Demo: Show user from CISSDM database
            if user:
                print(f"\nUser from CISSDM: {user.username}, {user.department}, {user.role}")
            
            # Demo: Analyze a ticket using OpenAI
            if ticket and not ticket.is_analyzed:
                print("\nDemonstrating ticket analysis with OpenAI:")
                analyze_ticket_example(ticketing_db, ticket.id)
        else:
            # In production mode, query existing data
            try:
                # Query example from CISSDM
                users = UserService.get_all_users(cissdm_db, limit=5)
                if users:
                    print(f"\nFound {len(users)} users in CISSDM database")
                    for user in users:
                        print(f"- {user.username}: {user.department}")
                else:
                    print("No users found in CISSDM database")
                
                # Query example from Ticketing
                tickets = TicketService.get_all_tickets(ticketing_db, limit=5)
                if tickets:
                    print(f"\nFound {len(tickets)} tickets in Ticketing database")
                    for ticket in tickets:
                        print(f"- {ticket.id}: {ticket.title} ({ticket.status})")
                    
                    # Analyze the first ticket as an example
                    first_ticket = tickets[0]
                    if not first_ticket.is_analyzed:
                        print("\nAnalyzing first ticket:")
                        analyze_ticket_example(ticketing_db, first_ticket.id)
                else:
                    print("No tickets found in Ticketing database")
            except Exception as e:
                print(f"Error querying databases: {e}")
                print("This might be due to missing tables or insufficient permissions.")
        
        print("\nPyChain is ready to use!")
        print("You can modify main.py to implement your specific requirements.")
        
    except Exception as e:
        print(f"Error in main: {e}")
    finally:
        cissdm_db.close()
        ticketing_db.close()

if __name__ == "__main__":
    main() 