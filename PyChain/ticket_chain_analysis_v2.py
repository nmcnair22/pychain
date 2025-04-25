#!/usr/bin/env python3
"""
Enhanced ticket chain analysis using both Chat Completions API (Phase 1) and Assistant API (Phase 2).
"""

import os
import json
import argparse
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import sys
import time

# Add parent directory to path for proper imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables
load_dotenv()

# Initialize OpenAI client
from openai import OpenAI
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
ASSISTANT_ID = os.getenv("ASSISTANT_ID")
# No early error if ASSISTANT_ID is not found - we'll create it as needed

# Define report type (fixed to relationship summary)
REPORT_TYPE = {
    "id": "relationship",
    "name": "Ticket Relationship and Summary",
    "prompt_focus": "Analyze the relationships between these tickets and provide an overall summary."
}

# Assistant configuration
ASSISTANT_NAME = "Ticket Chain Analyzer"
ASSISTANT_DESCRIPTION = "Analyzes ticket chains to extract relationships, material shortages, timeline events, and more."
ASSISTANT_INSTRUCTIONS = """
You are an expert field service analyst specializing in ticket chain analysis. Your primary task is to analyze JSON files containing ticket data for field service projects and extract useful insights.

For each ticket chain, you'll examine:
1. The relationships between tickets
2. Timeline of events across tickets
3. Material shortages and their impact
4. Revisits and their reasons
5. Cable drops requested vs. completed
6. Phase completion status

When analyzing tickets:
- Pay close attention to post contents for mentions of material shortages, issues, or incomplete work
- Look for relationships between tickets (P1 → P2 → P3 → Revisit)
- Track billing milestones (50% billing, completed billing)
- Identify causes of revisits and whether they were billable
- Be factual and specific in your analyses
- Always respond in JSON format when asked to do so

Provide structured data that can be easily parsed programmatically when requested.
"""

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

# --- New Functions for Vector Store Management ---

def setup_vector_store_and_assistant():
    """
    Create a vector store and assistant if they don't exist.
    Updates .env file with IDs.
    
    Returns:
        tuple: (assistant_id, vector_store_id)
    """
    # Check for existing IDs
    assistant_id = os.getenv("ASSISTANT_ID")
    vector_store_id = os.getenv("VECTOR_STORE_ID")
    
    if assistant_id and vector_store_id:
        print(f"Using existing assistant (ID: {assistant_id}) and vector store (ID: {vector_store_id})")
        return assistant_id, vector_store_id
    
    # Create vector store
    print("Creating new vector store...")
    try:
        vector_store = client.vector_stores.create(name=f"ticket_analysis_store_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        vector_store_id = vector_store.id
        print(f"Vector store created with ID: {vector_store_id}")
    except Exception as e:
        print(f"Error creating vector store: {e}")
        return None, None
    
    # Create assistant
    print(f"Creating new assistant: {ASSISTANT_NAME}...")
    try:
        assistant = client.beta.assistants.create(
            name=ASSISTANT_NAME,
            description=ASSISTANT_DESCRIPTION,
            instructions=ASSISTANT_INSTRUCTIONS,
            tools=[{"type": "file_search"}],
            tool_resources={"file_search": {"vector_store_ids": [vector_store_id]}},
            model="gpt-4o"
        )
        assistant_id = assistant.id
        print(f"Assistant created with ID: {assistant_id}")
    except Exception as e:
        print(f"Error creating assistant: {e}")
        return None, vector_store_id
    
    # Update .env file
    update_env_file(assistant_id, vector_store_id)
    
    return assistant_id, vector_store_id

def update_env_file(assistant_id, vector_store_id):
    """Update the .env file with the assistant ID and vector store ID"""
    try:
        env_file = ".env"
        
        # Check if .env file exists
        if not os.path.exists(env_file):
            # Create new .env file
            with open(env_file, "w") as f:
                f.write(f"ASSISTANT_ID={assistant_id}\n")
                f.write(f"VECTOR_STORE_ID={vector_store_id}\n")
            print(f"Created new .env file with ASSISTANT_ID and VECTOR_STORE_ID.")
            return
        
        # Read existing .env file
        with open(env_file, "r") as f:
            lines = f.readlines()
        
        # Check if ASSISTANT_ID already exists
        assistant_id_exists = False
        vector_store_id_exists = False
        
        for i, line in enumerate(lines):
            if line.startswith("ASSISTANT_ID="):
                lines[i] = f"ASSISTANT_ID={assistant_id}\n"
                assistant_id_exists = True
            elif line.startswith("VECTOR_STORE_ID="):
                lines[i] = f"VECTOR_STORE_ID={vector_store_id}\n"
                vector_store_id_exists = True
        
        # Add IDs if they don't exist
        if not assistant_id_exists:
            lines.append(f"ASSISTANT_ID={assistant_id}\n")
        if not vector_store_id_exists:
            lines.append(f"VECTOR_STORE_ID={vector_store_id}\n")
        
        # Write updated .env file
        with open(env_file, "w") as f:
            f.writelines(lines)
        
        print(f"Updated .env file with ASSISTANT_ID and VECTOR_STORE_ID.")
    
    except Exception as e:
        print(f"Error updating .env file: {e}")

def cleanup_openai_resources(file_ids, vector_store_id, delete_vector_store=False):
    """
    Clean up OpenAI resources after analysis.
    
    Args:
        file_ids: List of file IDs to delete
        vector_store_id: Vector store ID to clean up
        delete_vector_store: Whether to delete the vector store itself
    """
    # Delete files from OpenAI
    print("\nCleaning up OpenAI resources...")
    
    for file_id in file_ids:
        try:
            # First remove file from vector store
            print(f"Removing file {file_id} from vector store...")
            client.vector_stores.files.delete(
                vector_store_id=vector_store_id,
                file_id=file_id
            )
            
            # Then delete the file from OpenAI
            print(f"Deleting file {file_id} from OpenAI...")
            client.files.delete(file_id=file_id)
        except Exception as e:
            print(f"Error deleting file {file_id}: {e}")
    
    # Optionally delete the vector store
    if delete_vector_store:
        try:
            print(f"Deleting vector store {vector_store_id}...")
            client.vector_stores.delete(vector_store_id=vector_store_id)
            print("Vector store deleted.")
            
            # Remove from .env file
            env_file = ".env"
            if os.path.exists(env_file):
                with open(env_file, "r") as f:
                    lines = f.readlines()
                
                with open(env_file, "w") as f:
                    for line in lines:
                        if not line.startswith("VECTOR_STORE_ID="):
                            f.write(line)
            
        except Exception as e:
            print(f"Error deleting vector store: {e}")

# --- End of New Functions ---

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

def run_phase_1_report(chain_details):
    """Run Phase 1 analysis and save results as JSON"""
    from PyChain.app.services.ticket_chain_service import TicketChainService
    from PyChain.app.services.ai_service import AIService
    
    print(f"\nRunning {REPORT_TYPE['name']} (Phase 1)...")
    
    # Create a detailed prompt with chain information
    prompt = TicketChainService._create_chain_analysis_prompt(chain_details, REPORT_TYPE['id'])
    
    # Use AIService to analyze the chain
    analysis = AIService.analyze_chain(prompt, REPORT_TYPE['id'])
    
    # Print the analysis
    print("\n" + "=" * 50)
    print(f"ANALYSIS: {REPORT_TYPE['name']}")
    print("=" * 50)
    print(analysis)
    print("=" * 50)
    
    # Save analysis as JSON
    os.makedirs("PyChain/data/ticket_files", exist_ok=True)
    summary_file = f"PyChain/data/ticket_files/summary_{chain_details['chain_hash']}.json"
    summary_data = {
        "chain_hash": chain_details['chain_hash'],
        "ticket_count": chain_details['ticket_count'],
        "analysis": analysis,
        "timestamp": datetime.now().isoformat()
    }
    with open(summary_file, "w") as f:
        json.dump(summary_data, f, indent=4)
    print(f"Summary saved to {summary_file}")
    
    # Ask if user wants to save the analysis
    save_response = input("\nDo you want to save this analysis? (y/n): ").strip().lower()
    if save_response == 'y':
        # Create the data directory if it doesn't exist
        os.makedirs("PyChain/data/analyses", exist_ok=True)
        
        # Create a timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create a filename that includes the chain hash and timestamp
        filename = f"PyChain/data/analyses/{chain_details['chain_hash']}_{timestamp}_{REPORT_TYPE['id']}.txt"
        
        # Save the analysis with metadata
        with open(filename, "w") as f:
            f.write(f"Ticket Chain Analysis\n")
            f.write(f"Report Type: {REPORT_TYPE['name']}\n")
            f.write(f"Chain Hash: {chain_details['chain_hash']}\n")
            f.write(f"Number of Tickets: {chain_details['ticket_count']}\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("=" * 50 + "\n")
            f.write(analysis)
        
        print(f"Analysis saved to {filename}")
    
    return summary_file, analysis

def fetch_full_ticket_data(session, ticket_ids):
    """Fetch all posts and details for each ticket"""
    if not ticket_ids:
        return {}
    
    # Check if ticket_ids is already a list, if not convert it
    if not isinstance(ticket_ids, list):
        ticket_ids = list(ticket_ids)
    
    # Handle case with only one ticket ID
    if len(ticket_ids) == 1:
        query = text("""
            SELECT 
                tp.ticketid, 
                tp.contents, 
                tp.fullname, 
                FROM_UNIXTIME(tp.dateline) AS timestamp,
                t.subject, 
                t.ticketstatustitle AS status, 
                t.departmenttitle
            FROM sw_ticketposts tp
            JOIN sw_tickets t ON tp.ticketid = t.ticketid
            WHERE tp.ticketid = :ticket_id
            ORDER BY tp.ticketid, tp.dateline
        """)
        
        result = session.execute(query, {"ticket_id": ticket_ids[0]}).fetchall()
    else:
        # Convert ticket_ids to strings to ensure proper SQL formatting
        ticket_ids_str = [str(id) for id in ticket_ids]
        
        # Create placeholders for the IN clause
        placeholders = ', '.join(['%s' % id for id in ticket_ids_str])
        
        query = text(f"""
            SELECT 
                tp.ticketid, 
                tp.contents, 
                tp.fullname, 
                FROM_UNIXTIME(tp.dateline) AS timestamp,
                t.subject, 
                t.ticketstatustitle AS status, 
                t.departmenttitle
            FROM sw_ticketposts tp
            JOIN sw_tickets t ON tp.ticketid = t.ticketid
            WHERE tp.ticketid IN ({placeholders})
            ORDER BY tp.ticketid, tp.dateline
        """)
        
        result = session.execute(query).fetchall()
    
    ticket_data = {}
    for row in result:
        ticket_id = row.ticketid
        if ticket_id not in ticket_data:
            ticket_data[ticket_id] = {
                "ticket_id": ticket_id,
                "subject": row.subject,
                "status": row.status,
                "department": row.departmenttitle,
                "phase": get_phase(row.subject),
                "posts": []
            }
        ticket_data[ticket_id]["posts"].append({
            "content": row.contents,
            "author": row.fullname,
            "timestamp": str(row.timestamp)
        })
    
    return ticket_data

def get_phase(subject):
    """Extract the phase from the ticket subject"""
    if not subject:
        return "Unknown"
        
    if "P1" in subject:
        return "P1"
    elif "P2" in subject:
        return "P2"
    elif "P3" in subject:
        return "P3"
    elif "Site Survey" in subject.lower():
        return "Site Survey"
    elif "Billing" in subject:
        return "Billing"
    elif "Revisit" in subject:
        return "Revisit"
    else:
        return "Other"

def create_ticket_files(ticket_data, chain_hash):
    """Create JSON files for each ticket"""
    os.makedirs("PyChain/data/ticket_files", exist_ok=True)
    file_paths = []
    
    # Create a chain metadata file
    chain_file = f"PyChain/data/ticket_files/chain_{chain_hash}.json"
    with open(chain_file, "w") as f:
        chain_data = {
            "chain_hash": chain_hash,
            "ticket_count": len(ticket_data),
            "ticket_ids": list(ticket_data.keys())
        }
        json.dump(chain_data, f, indent=4)
    file_paths.append(chain_file)
    
    # Create individual ticket files
    for ticket_id, data in ticket_data.items():
        file_path = f"PyChain/data/ticket_files/ticket_{ticket_id}.json"
        with open(file_path, "w") as f:
            json.dump(data, f, indent=4)
        file_paths.append(file_path)
    
    return file_paths

def upload_files(file_paths):
    """Upload files to the OpenAI API"""
    file_ids = []
    for file_path in file_paths:
        try:
            print(f"Uploading {os.path.basename(file_path)}...")
            with open(file_path, "rb") as file:
                upload = client.files.create(
                    file=file,
                    purpose="assistants"
                )
                file_ids.append(upload.id)
        except Exception as e:
            print(f"Error uploading {file_path}: {e}")
    return file_ids

def run_phase_2_analysis(chain_details, summary_file, analysis):
    """Run Phase 2 analysis using the OpenAI Assistant API with vector store for file search"""
    # Set up vector store and assistant if needed
    assistant_id, vector_store_id = setup_vector_store_and_assistant()
    
    if not assistant_id or not vector_store_id:
        print("ERROR: Failed to create or find assistant and vector store. Cannot run Phase 2 analysis.")
        return
        
    print("\nRunning Phase 2 Analysis with OpenAI Assistant and vector store...")
    print("This will provide more detailed insights and extract structured data.")
    
    # Get ticket IDs from chain details
    ticket_ids = []
    for ticket in chain_details['tickets']:
        ticket_id = ticket.get('ticketid')
        if ticket_id:
            # Make sure ticket_id is a string
            ticket_ids.append(str(ticket_id))
    
    if not ticket_ids:
        print("No ticket IDs found in chain details. Aborting Phase 2 analysis.")
        return
    
    chain_hash = chain_details['chain_hash']
    file_ids = []
    
    # Fetch full ticket data
    session = get_db_session()
    try:
        print("Fetching complete ticket data including all posts...")
        ticket_data = fetch_full_ticket_data(session, ticket_ids)
        
        if not ticket_data:
            print("No ticket data found. Aborting Phase 2 analysis.")
            return
            
        print(f"Creating JSON files for {len(ticket_data)} tickets...")
        file_paths = create_ticket_files(ticket_data, chain_hash)
        
        # Upload files
        print("Uploading files to OpenAI...")
        file_ids = upload_files(file_paths)
        if not file_ids:
            print("Failed to upload files. Aborting Phase 2 analysis.")
            return
            
        print(f"Successfully uploaded {len(file_ids)} files")
        
        # Add files to vector store
        print(f"Adding files to vector store...")
        for file_id in file_ids:
            try:
                file_association = client.vector_stores.files.create(
                    vector_store_id=vector_store_id,
                    file_id=file_id
                )
                print(f"Added file {file_id} to vector store")
            except Exception as e:
                print(f"Error adding file {file_id} to vector store: {e}")
        
        # Wait for file processing
        print("Waiting for file processing in vector store...")
        time.sleep(5)
        
        # Update assistant with the vector store for file search
        print(f"Updating assistant with vector store...")
        assistant = client.beta.assistants.update(
            assistant_id=assistant_id,
            tool_resources={"file_search": {"vector_store_ids": [vector_store_id]}}
        )
        
        # Create thread
        print("Creating analysis thread...")
        thread = client.beta.threads.create()
        
        # Define initial queries
        print("Running analysis queries...")
        queries = [
            "Extract all ticket IDs and their phases in JSON format.",
            "Identify any material shortages mentioned in the tickets in JSON format.",
            "Create a timeline of events for this ticket chain in JSON format.",
            "List all revisits and their reasons in JSON format."
        ]
        
        responses = []
        for i, query in enumerate(queries, 1):
            print(f"\nQuery {i}/{len(queries)}: {query}")
            
            # Create message
            message = client.beta.threads.messages.create(
                thread_id=thread.id,
                role="user",
                content=query
            )
            
            # Run the thread
            run = client.beta.threads.runs.create(
                thread_id=thread.id,
                assistant_id=assistant_id
            )
            
            # Poll for completion
            while True:
                run_status = client.beta.threads.runs.retrieve(
                    thread_id=thread.id,
                    run_id=run.id
                )
                
                if run_status.status == "completed":
                    break
                elif run_status.status in ["failed", "cancelled", "expired"]:
                    print(f"Run failed with status: {run_status.status}")
                    break
                
                print("Waiting for assistant response...")
                time.sleep(5)
            
            # Retrieve the response
            if run_status.status == "completed":
                messages = client.beta.threads.messages.list(
                    thread_id=thread.id
                )
                
                # Find the latest assistant message
                for msg in messages.data:
                    if msg.role == "assistant" and msg.run_id == run.id:
                        response_text = msg.content[0].text.value
                        responses.append((query, response_text))
                        
                        print("\n" + "-" * 40)
                        print("RESPONSE:")
                        print("-" * 40)
                        print(response_text)
                        print("-" * 40)
                        
                        # Check if follow-up is needed
                        if ("shortage" in query.lower() and 
                            ("shortage" in response_text.lower() or "shortages" in response_text.lower()) and
                            not "not found" in response_text.lower()):
                            
                            follow_up = "List tickets affected by material shortages and provide detailed information about each one in JSON format."
                            print(f"\nFollow-up Query: {follow_up}")
                            
                            follow_up_message = client.beta.threads.messages.create(
                                thread_id=thread.id,
                                role="user",
                                content=follow_up
                            )
                            
                            follow_up_run = client.beta.threads.runs.create(
                                thread_id=thread.id,
                                assistant_id=assistant_id
                            )
                            
                            # Poll for completion
                            while True:
                                follow_up_status = client.beta.threads.runs.retrieve(
                                    thread_id=thread.id,
                                    run_id=follow_up_run.id
                                )
                                
                                if follow_up_status.status == "completed":
                                    break
                                elif follow_up_status.status in ["failed", "cancelled", "expired"]:
                                    print(f"Follow-up run failed with status: {follow_up_status.status}")
                                    break
                                
                                print("Waiting for assistant response...")
                                time.sleep(5)
                            
                            # Get follow-up response
                            if follow_up_status.status == "completed":
                                follow_up_messages = client.beta.threads.messages.list(
                                    thread_id=thread.id
                                )
                                
                                for fm in follow_up_messages.data:
                                    if fm.role == "assistant" and fm.run_id == follow_up_run.id:
                                        follow_up_response = fm.content[0].text.value
                                        responses.append((follow_up, follow_up_response))
                                        
                                        print("\n" + "-" * 40)
                                        print("FOLLOW-UP RESPONSE:")
                                        print("-" * 40)
                                        print(follow_up_response)
                                        print("-" * 40)
                                        
                                        break
                        break
        
        # Save Phase 2 results
        print("\nSaving Phase 2 analysis results...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        phase2_file = f"PyChain/data/analyses/{chain_hash}_{timestamp}_phase2.json"
        
        with open(phase2_file, "w") as f:
            result_data = {
                "chain_hash": chain_hash,
                "ticket_count": len(ticket_ids),
                "phase1_summary": analysis,
                "phase2_responses": [
                    {"query": q, "response": r} for q, r in responses
                ],
                "timestamp": datetime.now().isoformat()
            }
            json.dump(result_data, f, indent=4)
        
        print(f"Phase 2 analysis saved to {phase2_file}")
        
        # Clean up local files
        print("\nCleaning up temporary files...")
        for file_path in file_paths:
            try:
                os.remove(file_path)
            except Exception as e:
                print(f"Error removing {file_path}: {e}")
        
        # Ask if user wants to clean up OpenAI resources
        cleanup_response = input("\nDo you want to clean up OpenAI resources (files and vector store)? (y/n): ").strip().lower()
        if cleanup_response == 'y':
            delete_store = input("Also delete the vector store? This will prevent reusing it for future analyses. (y/n): ").strip().lower() == 'y'
            cleanup_openai_resources(file_ids, vector_store_id, delete_store)
        
        print("Phase 2 analysis complete!")
        
    except Exception as e:
        print(f"Error during Phase 2 analysis: {e}")
        # Attempt cleanup even if analysis fails
        if file_ids:
            print("\nError occurred. Attempting to clean up OpenAI resources...")
            cleanup_response = input("Do you want to clean up OpenAI resources despite the error? (y/n): ").strip().lower()
            if cleanup_response == 'y':
                delete_store = input("Also delete the vector store? (y/n): ").strip().lower() == 'y'
                cleanup_openai_resources(file_ids, vector_store_id, delete_store)
    finally:
        session.close()

def analyze_real_ticket(ticket_id):
    """Analyze a real ticket from the database"""
    from PyChain.app.services.ticket_chain_service import TicketChainService
    
    # Create a database session
    session = get_db_session()
    
    try:
        # Get the ticket chain details
        print(f"Retrieving ticket chain for ticket ID: {ticket_id}")
        chain_details = TicketChainService.get_chain_details_by_ticket_id(session, ticket_id)
        
        # Display ticket information
        chain_details = display_ticket_details(chain_details)
        if not chain_details:
            return
        
        # Run Phase 1 (standard analysis)
        summary_file, analysis = run_phase_1_report(chain_details)
        
        # Ask if user wants to run Phase 2
        run_phase2 = input("\nDo you want to run Phase 2 analysis with the OpenAI Assistant API? (y/n): ").strip().lower()
        if run_phase2 == 'y':
            run_phase_2_analysis(chain_details, summary_file, analysis)
            
    finally:
        # Close the session
        session.close()

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
    
    # Run Phase 1
    summary_file, analysis = run_phase_1_report(mock_chain)
    
    # Ask if user wants to run Phase 2
    run_phase2 = input("\nDo you want to run Phase 2 analysis with the OpenAI Assistant API? (y/n): ").strip().lower()
    if run_phase2 == 'y':
        run_phase_2_analysis(mock_chain, summary_file, analysis)

def test_with_real_tickets():
    """Test the analysis with real ticket IDs for more realistic testing"""
    from PyChain.app.services.ticket_chain_service import TicketChainService
    
    # List of real ticket IDs from a known chain
    real_ticket_ids = [
        2401912, 2408499, 2410860, 2410943, 2411028, 2422191,  # Dispatch tickets
        2402096, 2427282,  # Other tickets
        2410661, 2411131, 2411167, 2416822, 2422743  # Turnup tickets
    ]
    
    # Create a simulated chain details structure
    chain_hash = "test_real_data_chain"
    mock_chain = {
        "chain_hash": chain_hash,
        "ticket_count": len(real_ticket_ids),
        "tickets": []
    }
    
    # Create a database session to get actual ticket data
    session = get_db_session()
    
    try:
        print("\nFetching data for real tickets...")
        
        # Convert to strings for SQL query
        ticket_ids_str = [str(tid) for tid in real_ticket_ids]
        ids_string = ", ".join(ticket_ids_str)
        
        # Query basic ticket info - fix column names based on actual schema
        query = text(f"""
            SELECT 
                t.ticketid, 
                t.subject,
                t.departmenttitle,
                t.ticketstatustitle AS status,
                FROM_UNIXTIME(t.dateline) AS datecreated,
                FROM_UNIXTIME(t.lastactivity) AS datemodified
            FROM sw_tickets t
            WHERE t.ticketid IN ({ids_string})
        """)
        
        result = session.execute(query).fetchall()
        
        # Add each ticket to the mock chain
        for row in result:
            # Determine ticket category
            if row.departmenttitle == 'Turnups':
                category = "Turnup Tickets"
            elif row.departmenttitle in ['Dispatch', 'Pro Services', 'FST Accounting']:
                category = "Dispatch Tickets"
            else:
                category = "Other"
            
            # Create ticket entry
            ticket = {
                "ticketid": row.ticketid,
                "subject": row.subject,
                "TicketCategory": category,
                "ticketstatus": row.status,
                "datecreated": str(row.datecreated) if row.datecreated else None,
                "datemodified": str(row.datemodified) if row.datemodified else None,
                "ticketdescription": "Description fetched from actual ticket database."
            }
            
            # Add to mock chain
            mock_chain["tickets"].append(ticket)
        
        print(f"Fetched data for {len(mock_chain['tickets'])} tickets")
        
        # Group tickets by category for display
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
        
        # Run Phase 1
        summary_file, analysis = run_phase_1_report(mock_chain)
        
        # Ask if user wants to run Phase 2
        run_phase2 = input("\nDo you want to run Phase 2 analysis with the OpenAI Assistant API? (y/n): ").strip().lower()
        if run_phase2 == 'y':
            # Pass the original ticket IDs for Phase 2 to fetch full data
            run_phase_2_analysis(mock_chain, summary_file, analysis)
            
    except Exception as e:
        print(f"Error during test with real tickets: {e}")
    finally:
        session.close()

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Advanced ticket chain analysis (Phase 1 & 2)")
    parser.add_argument("--ticket", type=str, help="Ticket ID to analyze")
    parser.add_argument("--test", action="store_true", help="Run with test data")
    parser.add_argument("--test-real", action="store_true", help="Run test with real ticket IDs")
    parser.add_argument("--assistant-id", type=str, help="Override the Assistant ID from .env")
    
    args = parser.parse_args()
    
    # Override Assistant ID if provided
    global ASSISTANT_ID
    if args.assistant_id:
        ASSISTANT_ID = args.assistant_id
        print(f"Using provided Assistant ID: {ASSISTANT_ID}")
    
    if args.test_real:
        test_with_real_tickets()
    elif args.test:
        test_with_mock_data()
    elif args.ticket:
        analyze_real_ticket(args.ticket)
    else:
        parser.print_help()

if __name__ == "__main__":
    main() 