#!/usr/bin/env python3
"""
Ticket Chain Analysis Tool

This script is used to test different approaches to analyzing relationships
between tickets in a field service system using OpenAI.
"""

import os
import sys
import argparse
from sqlalchemy.exc import OperationalError

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from app.models.base import Base, get_ticketing_db, ticketing_engine
from app.services.ticket_chain_service import TicketChainService
from app.utils.db_helpers import create_mock_ticket_chain
from config import USE_IN_MEMORY_DB

def create_tables():
    """Create database tables for testing"""
    print("Setting up database tables...")
    Base.metadata.create_all(bind=ticketing_engine)
    print("Database tables created.")

def test_with_mock_data(complexity=1):
    """
    Test ticket chain analysis using mock data
    
    Args:
        complexity: 1=simple, 2=moderate, 3=complex relationship patterns
    """
    print(f"Creating mock ticket chain (complexity level: {complexity})...")
    
    # Different complexity levels will create more challenging relationships
    if complexity == 1:
        num_dispatch = 1
        num_turnup = 1
    elif complexity == 2:
        num_dispatch = 2
        num_turnup = 3
    else:  # complexity >= 3
        num_dispatch = 3
        num_turnup = 5
    
    # Get database session
    db_generator = get_ticketing_db()
    db = next(db_generator)
    
    try:
        # Create mock data
        chain_info = create_mock_ticket_chain(db, num_dispatch, num_turnup)
        print(f"Created ticket chain with hash: {chain_info['chain_hash']}")
        print(f"Dispatch tickets: {', '.join(chain_info['dispatch_tickets'])}")
        print(f"Turnup tickets: {', '.join(chain_info['turnup_tickets'])}")
        
        # Get a ticket to use for testing
        test_ticket = chain_info['example_ticket']
        print(f"\nUsing ticket {test_ticket} for analysis...\n")
        
        # First, show the raw chain details
        chain_details = TicketChainService.get_chain_details_by_ticket_id(db, test_ticket)
        print(f"Found {chain_details['ticket_count']} tickets in chain.\n")
        
        # Analyze the relationships
        print("Analyzing ticket relationships with OpenAI...\n")
        analysis = TicketChainService.analyze_chain_relationships(db, test_ticket)
        
        print("=" * 80)
        print("TICKET CHAIN ANALYSIS RESULT")
        print("=" * 80)
        print(analysis)
        print("=" * 80)
        
    except Exception as e:
        print(f"Error during testing: {e}")
    finally:
        db.close()

def analyze_real_ticket(ticket_id):
    """
    Analyze a real ticket chain from the database
    
    Args:
        ticket_id: The ticket ID to analyze
    """
    if USE_IN_MEMORY_DB:
        print("Cannot analyze real tickets in in-memory mode.")
        print("Please set USE_IN_MEMORY_DB=false in .env file to connect to real databases.")
        return
    
    print(f"Analyzing ticket chain for ticket ID: {ticket_id}")
    
    # Get database session
    db_generator = get_ticketing_db()
    db = next(db_generator)
    
    try:
        # Get chain details directly with the single query approach
        chain_details = TicketChainService.get_chain_details_by_ticket_id(db, ticket_id)
        
        if "error" in chain_details:
            print(f"Error: {chain_details['error']}")
            return
        
        # Show chain hash
        print(f"Found chain hash: {chain_details['chain_hash']}")
        
        # Show tickets found
        print(f"Found {chain_details['ticket_count']} tickets in chain:")
        
        # Group tickets by category
        tickets_by_category = {}
        for ticket in chain_details['tickets']:
            category = ticket['TicketCategory']
            if category not in tickets_by_category:
                tickets_by_category[category] = []
            tickets_by_category[category].append(ticket)
        
        # Print ticket summary by category
        for category, tickets in tickets_by_category.items():
            print(f"\n{category} ({len(tickets)}):")
            for ticket in tickets:
                print(f"  - ID: {ticket['ticketid']}, Subject: {ticket['subject']}")
        
        print("\nAnalyzing ticket relationships with OpenAI...\n")
        analysis = TicketChainService.analyze_chain_relationships(db, ticket_id)
        
        print("=" * 80)
        print("TICKET CHAIN ANALYSIS RESULT")
        print("=" * 80)
        print(analysis)
        print("=" * 80)
        
    except Exception as e:
        print(f"Error analyzing ticket: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Ticket Chain Analysis Tool")
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Test with mock data
    mock_parser = subparsers.add_parser("mock", help="Test with mock data")
    mock_parser.add_argument(
        "--complexity", type=int, choices=[1, 2, 3], default=2,
        help="Complexity level of mock relationships (1=simple, 2=moderate, 3=complex)"
    )
    
    # Analyze a real ticket
    real_parser = subparsers.add_parser("analyze", help="Analyze a real ticket chain")
    real_parser.add_argument(
        "ticket_id", type=str,
        help="Ticket ID to analyze"
    )
    
    return parser.parse_args()

def main():
    """Main function"""
    print("Ticket Chain Analysis Tool")
    print("-------------------------\n")
    
    args = parse_arguments()
    
    try:
        # Create tables (needed for in-memory mode)
        if USE_IN_MEMORY_DB:
            create_tables()
        
        # Handle different commands
        if args.command == "mock":
            test_with_mock_data(args.complexity)
        elif args.command == "analyze":
            analyze_real_ticket(args.ticket_id)
        else:
            # Default to mock test if no command specified
            test_with_mock_data(2)
    
    except OperationalError as e:
        print(f"Database error: {e}")
        print("Make sure your database connections are configured correctly.")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 