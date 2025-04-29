import argparse
import json
import logging
import sys
import os
import re
from datetime import datetime
from openai import OpenAI
from app.services.ticket_chain_service import TicketChainService
from app.services.ai_service import AIService

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Add parent directory to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Custom JSON encoder to handle datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def setup_vector_store_and_assistant(client: OpenAI, ticket_files: list[str]):
    try:
        vector_store_id = os.getenv("VECTOR_STORE_ID")
        if vector_store_id:
            logging.info(f"Using existing vector store ID: {vector_store_id}")
            return vector_store_id, os.getenv("ASSISTANT_ID")
        
        logging.info("No VECTOR_STORE_ID found, creating new vector store...")
        vector_store = client.beta.vector_stores.create(
            name=f"Ticket Analysis Store - {datetime.now():%Y%m%d-%H%M%S}"
        )
        vector_store_id = vector_store.id
        
        logging.info(f"Created vector store with ID: {vector_store_id}")
        
        assistant = client.beta.assistants.create(
            name="Ticket Analysis Assistant",
            instructions="You are an expert in analyzing ticket chains for field service operations. Provide detailed insights based on uploaded ticket data.",
            model="gpt-4o",
            tools=[{"type": "file_search"}],
            tool_resources={"file_search": {"vector_store_ids": [vector_store_id]}}
        )
        
        logging.info(f"Created assistant with ID: {assistant.id}")
        return vector_store_id, assistant.id
    except Exception as e:
        logging.error(f"Failed to set up vector store or assistant: {str(e)}")
        if "invalid_api_key" in str(e).lower():
            logging.error("Invalid OpenAI API key. Please verify OPENAI_API_KEY in your .env file and ensure it has permissions for vector store operations.")
        raise

def analyze_real_ticket(ticket_id: str, phase: str = "all"):
    logging.info(f"Retrieving ticket chain for ticket ID: {ticket_id}")
    session = TicketChainService.get_db_session("primary")
    if not session:
        logging.error("Failed to create database session")
        return
    
    try:
        chain_details = TicketChainService.get_chain_details_by_ticket_id(session, ticket_id)
        if not chain_details or "error" in chain_details:
            logging.error(chain_details.get("error", f"No ticket chain found for ticket ID: {ticket_id}"))
            return

        # Remove duplicates by ticket_id
        unique_tickets = {t['ticket_id']: t for t in chain_details.get('tickets', [])}.values()
        chain_details['tickets'] = list(unique_tickets)

        print("\nTicket Chain Details")
        print("------------------------------")
        print(f"Chain Hash: {chain_details.get('chain_hash')}")
        print(f"Number of Tickets: {len(chain_details.get('tickets', []))}\n")
        
        # Group tickets by ticket_category
        category_groups = {
            "Dispatch Tickets": [],
            "Turnup Tickets": [],
            "Shipping Tickets": [],
            "Project Management": [],
            "Other": []
        }
        
        for ticket in chain_details.get('tickets', []):
            category = ticket.get('ticket_category', 'Other')
            if category not in category_groups:
                category_groups[category] = []
            category_groups[category].append(ticket)
            logging.debug(f"Ticket {ticket['ticket_id']} categorized as {category}")
        
        for category, tickets in category_groups.items():
            if tickets:
                print(f"{category} Tickets: {len(tickets)}")
                for ticket in tickets:
                    print(f"  - {ticket.get('ticket_id')}: {ticket.get('subject')}")
        print()

        # Initialize AIService for analysis
        ai_service = AIService()

        # Proceed with AI analysis
        if phase in ["all", "phase1"]:
            logging.info(f"\nRunning Ticket Relationship and Summary (Phase 1) for chain {chain_details.get('chain_hash')}...")
            prompt = f"""
            Analyze the ticket chain for ticket ID {ticket_id}. The chain includes:
            {json.dumps(chain_details, indent=2, cls=DateTimeEncoder)}
            Provide a summary of relationships between tickets, including:
            1. Parent-child relationships (use linked_tickets field)
            2. Dispatch-turnup-billing linkages
            3. Any orphaned or unlinked records
            Extract specific details from posts and notes, including:
            - Cable drop counts (e.g., 'Qty. X')
            - Completion status (e.g., 'Closing', 'Completed')
            - Revisit requirements (e.g., 'reschedule', 'pending')
            - Delays (e.g., 'did not ship', 'on hold')
            """
            result = ai_service.analyze_chain(prompt, report_type="relationship_summary", provider="openai")
            
            print("\n==================================================")
            print("ANALYSIS: Ticket Relationship and Summary (Phase 1)")
            print("==================================================")
            logging.info("Phase 1 analysis complete. Summary follows:")
            print(result)
            
            os.makedirs("PyChain/data/ticket_files", exist_ok=True)
            os.makedirs("PyChain/data/analyses", exist_ok=True)
            
            summary_file = f"PyChain/data/ticket_files/summary_{chain_details.get('chain_hash')}.json"
            analysis_file = f"PyChain/data/analyses/{chain_details.get('chain_hash')}_{datetime.now():%Y%m%d_%H%M%S}_relationship.txt"
            
            with open(summary_file, 'w') as f:
                json.dump({
                    "ticket_id": ticket_id,
                    "chain_hash": chain_details.get('chain_hash'),
                    "summary": result,
                    "tickets": chain_details.get('tickets')
                }, f, indent=2, cls=DateTimeEncoder)
            with open(analysis_file, 'w') as f:
                f.write(result)
            
            logging.info(f"Phase 1 Summary saved to {summary_file}")
            logging.info(f"Phase 1 Analysis text saved to {analysis_file}")
        
        if phase in ["all", "phase2"]:
            proceed = input("\nDo you want to run Phase 2 analysis (detailed multi-stage)? (y/n): ").lower()
            if proceed != 'y':
                logging.info("Skipping Phase 2 analysis.")
                return
            
            if not ai_service.openai_client:
                logging.error("OpenAI client not initialized. Please check your OPENAI_API_KEY in the .env file.")
                return
                
            client = ai_service.openai_client
            vector_store_id, assistant_id = setup_vector_store_and_assistant(client, [])
            
            ticket_files = [f"PyChain/data/ticket_files/summary_{chain_details.get('chain_hash')}.json"]
            files = [client.files.create(file=open(f, 'rb'), purpose="assistants") for f in ticket_files]
            
            client.beta.vector_stores.files.batch_create(
                vector_store_id=vector_store_id,
                file_ids=[f.id for f in files]
            )
            
            thread = client.beta.threads.create()
            client.beta.threads.messages.create(
                thread_id=thread.id,
                role="user",
                content=f"""
                Analyze the ticket chain data for ticket ID {ticket_id}. Provide a detailed multi-stage analysis:
                1. Detailed timeline of visits and outcomes
                2. Issues and incomplete work
                3. Revisit requirements and classifications
                Extract from posts and notes:
                - Cable drop counts
                - Completion status
                - Delays and reasons
                - Technician time on site
                """
            )
            
            run = client.beta.threads.runs.create(
                thread_id=thread.id,
                assistant_id=assistant_id
            )
            
            while run.status != "completed":
                run = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
            
            messages = client.beta.threads.messages.list(thread_id=thread.id)
            phase2_result = messages.data[0].content[0].text.value
            
            print("\n==================================================")
            print("ANALYSIS: Detailed Multi-Stage (Phase 2)")
            print("==================================================")
            logging.info("Phase 2 analysis complete. Summary follows:")
            print(phase2_result)
            
            analysis_file = f"PyChain/data/analyses/{chain_details.get('chain_hash')}_{datetime.now():%Y%m%d_%H%M%S}_detailed.txt"
            with open(analysis_file, 'w') as f:
                f.write(phase2_result)
            
            logging.info(f"Phase 2 Analysis text saved to {analysis_file}")
            
            for file in files:
                client.files.delete(file.id)
    
    finally:
        logging.info("Primary database session closed by analyze_real_ticket.")
        session.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze a ticket chain")
    parser.add_argument("--ticket", required=True, help="Ticket ID to analyze")
    parser.add_argument("--phase", choices=["all", "phase1", "phase2"], default="all", help="Analysis phase to run")
    args = parser.parse_args()
    
    analyze_real_ticket(args.ticket, args.phase)