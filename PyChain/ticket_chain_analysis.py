#!/usr/bin/env python3
"""
Analyze the relationships between tickets in a ticket chain.
"""

import os
import json
import argparse
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sys

# Add parent directory to path for proper imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables
load_dotenv()

# Define available report types
REPORT_TYPES = {
    "1": {
        "id": "relationship",
        "name": "Ticket Relationship and Summary",
        "description": "Analyzes the relationships between tickets and provides a summary of the whole chain",
        "prompt_focus": "Analyze the relationships between these tickets and provide an overall summary."
    },
    "2": {
        "id": "timeline",
        "name": "Timelines and Outcomes",
        "description": "Analyzes the timeline, scope, completions, and issues for each visit",
        "prompt_focus": "Analyze the timeline, scope, completion status, issues, revisits, cable drops, and material shortages. Determine if revisits were billable to client."
    }
}

# Database connection
def get_db_session():
    """Create a new database session"""
    # Load database configuration 
    db_config = {
        "host": os.environ.get("TICKETING_DB_HOST", "localhost"),
        "user": os.environ.get("TICKETING_DB_USER", "root"),
        "password": os.environ.get("TICKETING_DB_PASSWORD", ""),
        "database": os.environ.get("TICKETING_DB_NAME", "")
    }
    
    # Create database engine
    connection_string = f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
    engine = create_engine(connection_string)
    
    # Create session
    Session = sessionmaker(bind=engine)
    session = Session()
    
    return session

def test_with_mock_data():
    """Test the analysis with mock data"""
    from PyChain.app.services.ticket_chain_service import TicketChainService
    
    # Mock chain details
    with open("PyChain/data/mock_ticket_chain.json", "r") as f:
        mock_chain = json.load(f)
    
    # Print ticket details
    print("\nTicket Chain Sample")
    print("-" * 30)
    print(f"Chain Hash: {mock_chain['chain_hash']}")
    print(f"Number of Tickets: {mock_chain['ticket_count']}")
    
    # Group tickets by category
    tickets_by_category = {}
    for ticket in mock_chain['tickets']:
        category = ticket.get('TicketCategory', 'Unknown')
        if category not in tickets_by_category:
            tickets_by_category[category] = []
        tickets_by_category[category].append(ticket)
    
    # Display tickets by category
    for category, tickets in tickets_by_category.items():
        print(f"\n{category} Tickets: {len(tickets)}")
        for ticket in tickets:
            print(f"  - {ticket.get('ticketid')}: {ticket.get('subject', 'No Subject')}")
    
    # Automatically run the relationship summary report (option 1)
    report_type = "relationship"
    run_selected_report(mock_chain, report_type)

def get_report_selection():
    """Display available report options and get user selection"""
    print("\nAvailable Reports:")
    for key, report in REPORT_TYPES.items():
        print(f"{key}: {report['name']} - {report['description']}")
    print("q: Quit")
    
    while True:
        selection = input("\nSelect a report to run: ").strip().lower()
        if selection == "q":
            return "quit"
        if selection in REPORT_TYPES:
            return REPORT_TYPES[selection]["id"]
        print("Invalid selection. Please try again.")

def display_ticket_details(chain_details):
    """Display details about the ticket chain"""
    if "error" in chain_details:
        print(f"Error: {chain_details['error']}")
        return None
    
    print("\nTicket Chain Details")
    print("-" * 30)
    print(f"Chain Hash: {chain_details['chain_hash']}")
    print(f"Number of Tickets: {chain_details['ticket_count']}")
    
    # Group tickets by category for easier viewing
    tickets_by_category = {}
    for ticket in chain_details['tickets']:
        category = ticket.get('TicketCategory', 'Unknown')
        if category not in tickets_by_category:
            tickets_by_category[category] = []
        tickets_by_category[category].append(ticket)
    
    # Display tickets by category
    for category, tickets in tickets_by_category.items():
        print(f"\n{category} Tickets: {len(tickets)}")
        for ticket in tickets:
            print(f"  - {ticket.get('ticketid')}: {ticket.get('subject', 'No Subject')}")
    
    return chain_details

def run_selected_report(chain_details, report_type):
    """Run the selected report type"""
    from PyChain.app.services.ticket_chain_service import TicketChainService
    from PyChain.app.services.ai_service import AIService
    
    # Find the report name for saving
    report_name = next((r["name"] for r in REPORT_TYPES.values() if r["id"] == report_type), "Unknown Report")
    
    print(f"\nRunning {report_name}...")
    
    # Create a detailed prompt with chain information
    prompt = TicketChainService._create_chain_analysis_prompt(chain_details, report_type)
    
    # Use AIService to analyze the chain
    analysis = AIService.analyze_chain(prompt, report_type)
    
    # Print the analysis
    print("\n" + "=" * 50)
    print(f"ANALYSIS: {report_name}")
    print("=" * 50)
    print(analysis)
    print("=" * 50)
    
    # Ask if user wants to save the analysis
    save_response = input("\nDo you want to save this analysis? (y/n): ").strip().lower()
    if save_response == 'y':
        # Create the data directory if it doesn't exist
        os.makedirs("PyChain/data/analyses", exist_ok=True)
        
        # Create a timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create a filename that includes the chain hash and timestamp
        filename = f"PyChain/data/analyses/{chain_details['chain_hash']}_{timestamp}_{report_type}.txt"
        
        # Save the analysis with metadata
        with open(filename, "w") as f:
            f.write(f"Ticket Chain Analysis\n")
            f.write(f"Report Type: {report_name}\n")
            f.write(f"Chain Hash: {chain_details['chain_hash']}\n")
            f.write(f"Number of Tickets: {chain_details['ticket_count']}\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("=" * 50 + "\n")
            f.write(analysis)
        
        print(f"Analysis saved to {filename}")

def analyze_real_ticket(ticket_id):
    """Analyze a real ticket from the database"""
    from PyChain.app.services.ticket_chain_service import TicketChainService
    
    # Create a database session
    session = get_db_session()
    
    try:
        # Get the ticket chain details
        chain_details = TicketChainService.get_chain_details_by_ticket_id(session, ticket_id)
        
        # Display ticket information
        chain_details = display_ticket_details(chain_details)
        if not chain_details:
            return
        
        # Automatically run the relationship summary report (option 1)
        report_type = "relationship"
        run_selected_report(chain_details, report_type)
            
    finally:
        # Close the session
        session.close()

def analyze_multiple_tickets(ticket_ids):
    """Analyze multiple tickets in sequence"""
    from PyChain.app.services.ticket_chain_service import TicketChainService
    
    # Create a database session
    session = get_db_session()
    
    try:
        # Set relationship report as default
        report_type = "relationship"
        report_name = next((r["name"] for r in REPORT_TYPES.values() if r["id"] == report_type), "Unknown Report")
        print(f"\nRunning {report_name} for all tickets...")
        
        # Analyze each ticket
        for i, ticket_id in enumerate(ticket_ids, 1):
            print(f"\n[{i}/{len(ticket_ids)}] Analyzing ticket {ticket_id}...")
            
            # Get the ticket chain details
            chain_details = TicketChainService.get_chain_details_by_ticket_id(session, ticket_id)
            
            # Display ticket information
            chain_details = display_ticket_details(chain_details)
            if not chain_details:
                print(f"Skipping ticket {ticket_id} due to errors.")
                continue
            
            # Run the relationship summary report for this ticket
            run_selected_report(chain_details, report_type)
        
        print(f"\nCompleted analysis of {len(ticket_ids)} tickets using {report_name}.")
    
    finally:
        # Close the session
        session.close()

def list_saved_analyses():
    """List all saved analyses"""
    analyses_dir = "PyChain/data/analyses"
    
    # Check if the directory exists
    if not os.path.exists(analyses_dir):
        print("No saved analyses found.")
        return
    
    # Get all analysis files
    analysis_files = [f for f in os.listdir(analyses_dir) if f.endswith(".txt")]
    
    if not analysis_files:
        print("No saved analyses found.")
        return
    
    print("\nSaved Analyses:")
    print("-" * 30)
    
    # Sort by timestamp (newest first)
    analysis_files.sort(reverse=True)
    
    for i, filename in enumerate(analysis_files, 1):
        # Extract information from filename
        parts = filename.split("_")
        chain_hash = parts[0]
        timestamp = parts[1]
        
        # Extract report type
        report_type_id = parts[2].split(".")[0]
        report_name = next((r["name"] for r in REPORT_TYPES.values() if r["id"] == report_type_id), "Unknown Report")
        
        # Format timestamp
        timestamp_date = datetime.strptime(timestamp, "%Y%m%d_%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"{i}. Chain: {chain_hash} - {timestamp_date} - {report_name}")
    
    return analysis_files

def show_saved_analysis():
    """Show a saved analysis"""
    analysis_files = list_saved_analyses()
    
    if not analysis_files:
        return
    
    while True:
        selection = input("\nEnter the number of the analysis to view (or 'q' to quit): ").strip().lower()
        
        if selection == 'q':
            return
        
        try:
            index = int(selection) - 1
            if 0 <= index < len(analysis_files):
                filename = analysis_files[index]
                
                # Extract report type from filename
                parts = filename.split("_")
                report_type_id = parts[2].split(".")[0]
                report_name = next((r["name"] for r in REPORT_TYPES.values() if r["id"] == report_type_id), "Unknown Report")
                
                with open(f"PyChain/data/analyses/{filename}", "r") as f:
                    content = f.read()
                
                print("\n" + "=" * 50)
                print(f"SAVED ANALYSIS - {report_name}")
                print("=" * 50)
                print(content)
                print("=" * 50)
                break
            else:
                print(f"Invalid selection. Please enter a number between 1 and {len(analysis_files)}.")
        except ValueError:
            print("Invalid input. Please enter a number or 'q'.")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Analyze ticket chains")
    parser.add_argument("--ticket", type=str, help="Ticket ID to analyze")
    parser.add_argument("--tickets", type=str, help="Comma-separated list of ticket IDs to analyze")
    parser.add_argument("--test", action="store_true", help="Run with test data")
    parser.add_argument("--list", action="store_true", help="List saved analyses")
    parser.add_argument("--show", action="store_true", help="Show a saved analysis")
    
    args = parser.parse_args()
    
    if args.list:
        list_saved_analyses()
    elif args.show:
        show_saved_analysis()
    elif args.test:
        test_with_mock_data()
    elif args.ticket:
        analyze_real_ticket(args.ticket)
    elif args.tickets:
        ticket_ids = [tid.strip() for tid in args.tickets.split(",")]
        analyze_multiple_tickets(ticket_ids)
    else:
        parser.print_help()

if __name__ == "__main__":
    main() 