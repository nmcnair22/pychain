#!/usr/bin/env python3
"""
Test script for Phase 2 analysis using OpenAI Assistant API.
"""

import os
import json
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from openai import OpenAI

# Load environment variables
load_dotenv()

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
ASSISTANT_ID = os.getenv("ASSISTANT_ID")
if not ASSISTANT_ID:
    print("ERROR: ASSISTANT_ID not found in .env file. Cannot run Phase 2 analysis.")
    exit(1)

# Assistant configuration
ASSISTANT_NAME = "Ticket Chain Analyzer (Test)"
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

def fetch_ticket_data(session, ticket_ids):
    """Fetch data for specific tickets including all posts"""
    
    # Convert all ticket_ids to strings for consistent handling
    ticket_ids_str = [str(tid) for tid in ticket_ids]
    
    # Build comma-separated string for the IN clause
    ids_string = ", ".join(ticket_ids_str)
    
    # Fetch basic ticket info
    query = text(f"""
        SELECT 
            t.ticketid, 
            t.subject,
            t.departmenttitle,
            t.ticketstatustitle AS status,
            FROM_UNIXTIME(t.dateline) AS created_at
        FROM sw_tickets t
        WHERE t.ticketid IN ({ids_string})
    """)
    
    tickets_result = session.execute(query).fetchall()
    
    # Fetch posts for these tickets
    posts_query = text(f"""
        SELECT 
            tp.ticketid, 
            tp.contents, 
            tp.fullname, 
            FROM_UNIXTIME(tp.dateline) AS timestamp
        FROM sw_ticketposts tp
        WHERE tp.ticketid IN ({ids_string})
        ORDER BY tp.ticketid, tp.dateline
    """)
    
    posts_result = session.execute(posts_query).fetchall()
    
    # Process the results
    ticket_data = {}
    for row in tickets_result:
        ticket_id = str(row.ticketid)
        # Determine ticket phase from the subject
        phase = "Unknown"
        if row.subject:
            if "_P1" in row.subject:
                phase = "P1"
            elif "_P2" in row.subject:
                phase = "P2"
            elif "_P3" in row.subject:
                phase = "P3"
            elif "Site Survey" in row.subject:
                phase = "Site Survey"
            elif "Revisit" in row.subject:
                phase = "Revisit"
            elif "Billing" in row.subject:
                phase = "Billing"
                
        ticket_data[ticket_id] = {
            "ticket_id": ticket_id,
            "subject": row.subject,
            "status": row.status,
            "department": row.departmenttitle,
            "phase": phase,
            "created_at": str(row.created_at) if row.created_at else None,
            "posts": []
        }
    
    # Add posts to each ticket
    for post in posts_result:
        ticket_id = str(post.ticketid)
        if ticket_id in ticket_data:
            ticket_data[ticket_id]["posts"].append({
                "content": post.contents,
                "author": post.fullname,
                "timestamp": str(post.timestamp) if post.timestamp else None
            })
    
    return ticket_data

def create_ticket_files(ticket_data, chain_id):
    """Create JSON files for each ticket"""
    os.makedirs("PyChain/data/ticket_files", exist_ok=True)
    file_paths = []
    
    # Create a chain metadata file
    chain_file = f"PyChain/data/ticket_files/chain_{chain_id}.json"
    with open(chain_file, "w") as f:
        chain_data = {
            "chain_id": chain_id,
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

def create_assistant_with_files(file_ids):
    """Create a new assistant with files attached"""
    try:
        print(f"Creating new test assistant with {len(file_ids)} files...")
        
        # Create assistant using the current API structure
        assistant = client.beta.assistants.create(
            name=ASSISTANT_NAME,
            description=ASSISTANT_DESCRIPTION,
            instructions=ASSISTANT_INSTRUCTIONS,
            tools=[{"type": "file_search"}],
            model="gpt-4o",
            file_ids=file_ids
        )
        
        print(f"Successfully created assistant with ID: {assistant.id}")
        return assistant.id
    
    except Exception as e:
        print(f"Error creating assistant: {e}")
        return None

def run_phase2_test():
    """Test Phase 2 analysis with real ticket data"""
    # List of real ticket IDs
    real_ticket_ids = [
        2401912, 2408499, 2410860, 2410943, 2411028, 2422191,  # Dispatch tickets
        2402096, 2427282,  # Other tickets
        2410661, 2411131, 2411167, 2416822, 2422743  # Turnup tickets
    ]
    
    # Create a unique chain ID for this test
    chain_id = f"test_chain_{int(time.time())}"
    
    # Create a database session
    session = get_db_session()
    
    try:
        print(f"Fetching data for {len(real_ticket_ids)} tickets...")
        
        # Fetch ticket data with posts
        ticket_data = fetch_ticket_data(session, real_ticket_ids)
        
        if not ticket_data:
            print("No ticket data found. Aborting test.")
            return
        
        print(f"Successfully fetched data for {len(ticket_data)} tickets")
        
        # Create ticket files
        print("Creating JSON files for tickets...")
        file_paths = create_ticket_files(ticket_data, chain_id)
        
        # Upload files to OpenAI
        print("Uploading files to OpenAI...")
        file_ids = upload_files(file_paths)
        
        if not file_ids:
            print("Failed to upload files. Aborting test.")
            return
            
        print(f"Successfully uploaded {len(file_ids)} files with IDs: {file_ids}")
        
        # Create a new test assistant with files
        print("Creating a new test assistant with files...")
        test_assistant_id = create_assistant_with_files(file_ids)

        if not test_assistant_id:
            print("Failed to create test assistant. Aborting test.")
            return
        
        # Create thread
        print("Creating analysis thread...")
        thread = client.beta.threads.create()
        
        # Define queries
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
            
            # Run the thread with the test assistant
            run = client.beta.threads.runs.create(
                thread_id=thread.id,
                assistant_id=test_assistant_id
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
                        
                        # Check if follow-up is needed for shortages
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
                                assistant_id=test_assistant_id
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
        
        # Save results
        print("\nSaving Phase 2 results...")
        timestamp = int(time.time())
        results_file = f"PyChain/data/analyses/phase2_test_{timestamp}.json"
        
        # Make sure directory exists
        os.makedirs("PyChain/data/analyses", exist_ok=True)
        
        with open(results_file, "w") as f:
            result_data = {
                "chain_id": chain_id,
                "ticket_count": len(ticket_data),
                "responses": [
                    {"query": q, "response": r} for q, r in responses
                ],
                "timestamp": timestamp
            }
            json.dump(result_data, f, indent=4)
        
        print(f"Results saved to {results_file}")
        
        # Clean up files
        print("\nCleaning up temporary files...")
        for file_path in file_paths:
            try:
                os.remove(file_path)
            except Exception as e:
                print(f"Error removing {file_path}: {e}")
        
        print("Phase 2 test completed!")
        
    except Exception as e:
        print(f"Error during Phase 2 test: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    run_phase2_test() 