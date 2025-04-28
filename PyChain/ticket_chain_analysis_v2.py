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
import logging
from config import CISSDM_DB_CONFIG, OPENAI_API_KEY # Make sure OPENAI_API_KEY is in config or env
from openai import OpenAI # <-- Add this missing import
import tempfile

# Debug Python path
print("Current Python path:", sys.path)

# Add parent directory to path for proper imports
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)
    print(f"Added {parent_dir} to Python path")
else:
    print(f"Parent directory {parent_dir} already in Python path")

# Load environment variables
load_dotenv()

# --- Attempt to load configs ---
try:
    from config import OPENAI_API_KEY, CISSDM_DB_CONFIG
except ImportError:
    OPENAI_API_KEY = None
    CISSDM_DB_CONFIG = None
    print("Warning: config.py not found. Using environment variables only.")

# --- Attempt to import services ---
try:
    print("Attempting to import services...")
    from PyChain.app.services.ticket_chain_service import TicketChainService
    print("Successfully imported TicketChainService")
    from PyChain.app.services.ai_service import AIService
    print("Successfully imported AIService")
except ImportError as e:
    print(f"ImportError: {e}")
    print("Warning: app services not found. Running in standalone mode.")
    TicketChainService = None
    AIService = None
except Exception as e:
    print(f"Unexpected error during import: {e}")
    print("Warning: app services not found. Running in standalone mode.")
    TicketChainService = None
    AIService = None

# --- Configure logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize OpenAI client
try:
    # Prefer API key from config.py if available, otherwise use environment variable
    api_key_to_use = OPENAI_API_KEY or os.getenv("OPENAI_API_KEY")
    if not api_key_to_use:
        raise ValueError("OpenAI API Key not found in config.py (OPENAI_API_KEY) or environment variables.")
    # Log the first few characters of the API key for debugging (masked for security)
    logging.info(f"Using OpenAI API Key starting with: {api_key_to_use[:10]}...")
    client = OpenAI(api_key=api_key_to_use)
except Exception as e:
    logging.error(f"Failed to initialize OpenAI client: {e}")
    sys.exit(1)

# --- Assistant Configuration ---
# Get IDs from environment, will be created if not found
ASSISTANT_ID = os.getenv("ASSISTANT_ID")
VECTOR_STORE_ID = os.getenv("VECTOR_STORE_ID")

ASSISTANT_NAME = "Ticket Chain Analyzer V2" # Renamed for clarity
ASSISTANT_DESCRIPTION = "Analyzes ticket chains using full context and rules to extract structured data."
ASSISTANT_INSTRUCTIONS = """
You are an expert field service analyst. Analyze ticket chains using the provided files (consolidated tickets JSON, chain metadata JSON, Phase 1 summary JSON, and context/rules TXT/JSON), extracting structured JSON data for specific lifecycle phases. Follow these rules STRICTLY:
1.  **Read ALL Context First:** Before answering any query, fully read and understand ALL provided files: `tickets_*.json`, `chain_*.json`, `phase1_analysis_*.json`, `relationships_*.json`, and especially `Ticket_Records_Information_and_Rules.txt` or `rules.json`. Refer to these rules and context in your analysis.
2.  **Include ALL Tickets:** Ensure every ticket ID mentioned in the `chain_*.json` metadata file is considered and included where relevant in your analysis outputs, particularly in task lists or summaries. Do not omit tickets. If a ticket seems irrelevant to a specific phase query, state that rather than omitting it.
3.  **Map Relationships Accurately:** Use the `parent_dispatch_id` field from the ticket data and the predefined relationships in `relationships_*.json` to map dispatch-to-turnup links. Validate these against the context rules. **Suggest ALL other potential relationships** based on phase, subject, or scope overlaps, providing evidence and a confidence score (Low, Medium, High). Explicitly note any orphaned tickets or non-1:1 relationships found, referencing the rules.
4.  **Extract Detailed Scope:** Pull specific technical details (e.g., 'Cat 6 cabling', 'rack installation', equipment models, configuration details like 'pre-ARP tables') directly from ticket subjects, posts, or notes, especially from tickets like 2376701, 2380336. Summarize these details accurately where requested.
5.  **Prioritize Ticket Data & Validate:** Use the Phase 1 summary only for high-level context. Your primary source MUST be the detailed `tickets_*.json` data. **Validate** information like issue reasons (e.g., cancellations, failures - ensure 'snowstorm' for 2382726, 'no check-in' for 2385184, 'customer unaware' for 2389439/2389461/2382746) and statuses against specific ticket post/note content. **Cite the post timestamp/author or quote** the relevant text as evidence. Resolve conflicts in favor of the detailed ticket data.
6.  **Report ONLY Explicit & Accurate Data:** Only include data fields and values explicitly present in the ticket files or derived directly according to rules. Do NOT invent, assume, or generate unsupported fields (e.g., feedback scores, completion percentages, skill match, speculative dates). Use the **earliest/latest post timestamps** for start/end dates if explicit dates aren't available. State 'Not Available' or null for genuinely missing data.
7.  **Cite Sources:** Where feasible, reference the ticket ID and post timestamp/author that supports a specific finding, especially for issue reasons, scope details, statuses, and dates.
8.  **Strict JSON Output for Specific Queries:** When asked for JSON output, provide ONLY valid JSON as the response. Do not include explanations or conversational text outside the JSON structure unless the query specifically asks for a narrative component.
9.  **Flag Missing Data Clearly:** In narrative summaries or specific JSON fields (e.g., a `MissingDataNotes` field), explicitly list requested information that was NOT found in the files (e.g., "Cable drop counts not specified.", "Billable status for revisit X unclear."). Explain the potential impact.
10. **Handle Duplicates:** Ensure ticket lists in JSON responses do not contain duplicate IDs (e.g., 2380336 should appear only once if representing the same entity).
"""

# Define report type (fixed for Phase 1, Phase 2 uses narrative + targeted queries)
PHASE1_REPORT_TYPE = {
    "id": "relationship", # Keep using the ID for consistency if needed
    "name": "Ticket Relationship and Summary",
    "prompt_focus": "Analyze the relationships between these tickets and provide an overall summary."
}

# --- Database Connection ---
def get_db_session(db_type="primary"):
    """Create a new database session for primary or CISSDM."""
    if db_type == "primary":
        config = {
            "host": os.environ.get("TICKETING_DB_HOST", "localhost"),
            "user": os.environ.get("TICKETING_DB_USER", "root"),
            "password": os.environ.get("TICKETING_DB_PASSWORD", ""),
            "database": os.environ.get("TICKETING_DB_NAME", ""),
            "port": os.environ.get("TICKETING_DB_PORT", "3306") # Default MySQL port
        }
        db_name = "Primary Ticketing DB"
    elif db_type == "cissdm":
        try:
             # Ensure CISSDM_DB_CONFIG keys exist
            if not all(k in CISSDM_DB_CONFIG for k in ['user', 'password', 'host', 'port', 'database']):
                 raise ValueError("CISSDM_DB_CONFIG missing required keys in config.py")
            config = CISSDM_DB_CONFIG
            db_name = "CISSDM DB"
        except NameError:
             logging.error("CISSDM_DB_CONFIG not found in config.py.")
             return None
        except ValueError as e:
             logging.error(e)
             return None
    else:
        logging.error(f"Invalid database type specified: {db_type}")
        return None

    connection_string = f"mysql+mysqlconnector://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
    try:
        engine = create_engine(connection_string, connect_args={'connect_timeout': 10}) # Add timeout
        Session = sessionmaker(bind=engine)
        session = Session()
        logging.info(f"{db_name} database session created successfully.")
        return session
    except Exception as e:
        logging.error(f"Error creating {db_name} database session for {config['host']}/{config['database']}: {e}")
        # Optionally, re-raise specific critical errors if needed
        return None # Return None on connection failure


# --- Assistant/Vector Store Management ---
def setup_vector_store_and_assistant():
    """Creates/updates Assistant & Vector Store, returns IDs."""
    global ASSISTANT_ID, VECTOR_STORE_ID # Allow modification of global vars if needed

    assistant_id = ASSISTANT_ID
    vector_store_id = VECTOR_STORE_ID
    vector_store_created_now = False
    assistant_created_now = False

    # --- Vector Store Handling ---
    if not vector_store_id:
        logging.info("No VECTOR_STORE_ID found, creating new vector store...")
        try:
            vector_store = client.vector_stores.create(
                name=f"Ticket Analysis Store - {datetime.now():%Y%m%d-%H%M%S}"
            )
            vector_store_id = vector_store.id
            logging.info(f"Vector store created with ID: {vector_store_id}")
            vector_store_created_now = True
            VECTOR_STORE_ID = vector_store_id # Update global var
        except Exception as e:
            logging.error(f"Error creating vector store: {e}", exc_info=True)
            return None, None
    else:
        logging.info(f"Using existing vector store (ID: {vector_store_id})")
        # Optional: Verify vector store exists?
        try:
             client.vector_stores.retrieve(vector_store_id)
             logging.info(f"Vector store {vector_store_id} verified.")
        except Exception as e:
             logging.error(f"Failed to retrieve existing vector store {vector_store_id}: {e}. Please check ID or create new one.")
             return None, None


    # --- Assistant Handling ---
    if not assistant_id:
        if not vector_store_id: # Should not happen if VS creation succeeded
             logging.error("Cannot create assistant without a vector store ID.")
             return None, None
        logging.info(f"No ASSISTANT_ID found, creating new assistant: {ASSISTANT_NAME}...")
        try:
            assistant = client.beta.assistants.create(
                name=ASSISTANT_NAME,
                description=ASSISTANT_DESCRIPTION,
                instructions=ASSISTANT_INSTRUCTIONS,
                tools=[{"type": "file_search"}],
                tool_resources={"file_search": {"vector_store_ids": [vector_store_id]}},
                model="gpt-4o" # Or your preferred model
            )
            assistant_id = assistant.id
            logging.info(f"Assistant created with ID: {assistant_id}")
            assistant_created_now = True
            ASSISTANT_ID = assistant_id # Update global var
        except Exception as e:
            logging.error(f"Error creating assistant: {e}", exc_info=True)
            # Consider cleanup if vector store was just created
            return None, vector_store_id
    else:
        logging.info(f"Using existing assistant (ID: {assistant_id})")
        # Ensure existing assistant is linked to the *current* vector store ID
        try:
            logging.info(f"Updating existing assistant {assistant_id} to use vector store {vector_store_id}")
            client.beta.assistants.update(
                 assistant_id=assistant_id,
                 instructions=ASSISTANT_INSTRUCTIONS, # Keep instructions updated
                 tool_resources={"file_search": {"vector_store_ids": [vector_store_id]}}
            )
            logging.info(f"Assistant {assistant_id} updated successfully.")
        except Exception as e:
            logging.error(f"Error updating existing assistant {assistant_id} with vector store {vector_store_id}: {e}")
            # Decide if fatal. Maybe proceed but warn?
            # For now, log error and continue, assuming link might already be okay.

    # Update .env file only if new IDs were generated
    if assistant_created_now or vector_store_created_now:
        update_env_file(assistant_id, vector_store_id)

    return assistant_id, vector_store_id

def update_env_file(assistant_id, vector_store_id):
    """Update the .env file with the assistant ID and vector store ID"""
    try:
        env_file = ".env"
        lines = []
        # Read existing .env file if it exists
        if os.path.exists(env_file):
            with open(env_file, "r") as f:
                lines = f.readlines()

        # Prepare new/updated lines
        new_lines = {}
        if assistant_id: new_lines["ASSISTANT_ID"] = f"ASSISTANT_ID={assistant_id}\n"
        if vector_store_id: new_lines["VECTOR_STORE_ID"] = f"VECTOR_STORE_ID={vector_store_id}\n"

        updated_lines = []
        keys_updated = set()

        # Update existing lines
        for line in lines:
            found = False
            for key, new_line in new_lines.items():
                if line.startswith(key + "="):
                    updated_lines.append(new_line)
                    keys_updated.add(key)
                    found = True
                    break
            if not found:
                updated_lines.append(line)

        # Add new lines if they weren't updates
        for key, new_line in new_lines.items():
             if key not in keys_updated:
                  updated_lines.append(new_line)

        # Write updated .env file
        with open(env_file, "w") as f:
            f.writelines(updated_lines)

        logging.info(f"Updated .env file with ASSISTANT_ID and/or VECTOR_STORE_ID.")

    except Exception as e:
        logging.error(f"Error updating .env file: {e}")


def cleanup_openai_resources(file_ids, vector_store_id, delete_vector_store=False):
    """Cleanup OpenAI resources created during analysis."""
    logging.info("\nCleaning up OpenAI resources...")

    if not file_ids and not vector_store_id:
         logging.info("No file IDs or vector store ID provided for cleanup.")
         return

    # 1. Remove files from vector store first (if VS ID provided)
    if vector_store_id:
        for file_id in file_ids:
            try:
                logging.info(f"Removing file {file_id} from vector store {vector_store_id}...")
                client.vector_stores.files.delete(
                    vector_store_id=vector_store_id,
                    file_id=file_id
                )
            except Exception as e:
                logging.error(f"Error removing file {file_id} from vector store {vector_store_id}: {e}. Will still attempt to delete file.")
    else:
        logging.warning("No vector store ID provided, cannot remove files from vector store.")

    # 2. Delete the files from OpenAI storage
    for file_id in file_ids:
         try:
              logging.info(f"Deleting file {file_id} from OpenAI...")
              client.files.delete(file_id=file_id)
         except Exception as e:
              logging.error(f"Error deleting file {file_id} from OpenAI: {e}")

    # 3. Optionally delete the vector store
    if delete_vector_store and vector_store_id:
        try:
            logging.info(f"Deleting vector store {vector_store_id}...")
            client.vector_stores.delete(vector_store_id=vector_store_id)
            logging.info("Vector store deleted.")
            # Remove from .env file
            env_file = ".env"
            if os.path.exists(env_file):
                updated_lines = []
                try:
                    with open(env_file, "r") as f: lines = f.readlines()
                    with open(env_file, "w") as f:
                        for line in lines:
                            if not line.startswith("VECTOR_STORE_ID="): f.write(line)
                except Exception as e:
                     logging.error(f"Error removing VECTOR_STORE_ID from .env file: {e}")
        except Exception as e:
            logging.error(f"Error deleting vector store {vector_store_id}: {e}")
    elif delete_vector_store and not vector_store_id:
         logging.warning("Vector store deletion requested, but no vector store ID was provided.")


# --- Phase 1 Display ---
def display_ticket_details(chain_details):
    """Display details about the ticket chain"""
    if "error" in chain_details:
        logging.error(f"Cannot display ticket details: {chain_details['error']}")
        return None
    
    print("\nTicket Chain Details")
    print("-" * 30)
    print(f"Chain Hash: {chain_details.get('chain_hash', 'N/A')}")
    print(f"Number of Tickets: {chain_details.get('ticket_count', 0)}")
    
    # Group tickets by category for easier viewing
    tickets_by_category = {}
    if 'tickets' not in chain_details or not isinstance(chain_details['tickets'], list):
         logging.warning("No 'tickets' list found in chain_details for display.")
         return chain_details # Return original if no tickets to display

    for ticket in chain_details['tickets']:
         # Check if ticket is a dictionary
         if not isinstance(ticket, dict):
              logging.warning(f"Skipping non-dict item in tickets list: {ticket}")
              continue
         category = ticket.get('TicketCategory', 'Unknown') # Use the category determined by initial fetch
         if category not in tickets_by_category:
             tickets_by_category[category] = []
         tickets_by_category[category].append(ticket)

    # Display tickets by category
    for category, tickets in tickets_by_category.items():
        print(f"\n{category} Tickets: {len(tickets)}")
        for ticket in tickets:
            print(f"  - {ticket.get('ticketid', 'N/A')}: {ticket.get('subject', 'No Subject')}")
    
    return chain_details # Return details (potentially modified if error occurred)


# --- Phase 2 Data Fetching ---
def fetch_full_ticket_data(session, cissdm_session, ticket_ids):
    """Fetch detailed ticket data, including CISSDM headers and posts/notes."""
    if not ticket_ids:
        logging.warning("No ticket IDs provided to fetch_full_ticket_data.")
        return {}

    ticket_ids_str = [str(tid) for tid in set(ticket_ids)] # Use set to ensure unique IDs
    ticket_data = {}
    logging.info(f"Fetching details for {len(ticket_ids_str)} unique tickets: {ticket_ids_str}")

    # --- 1. Initial Fetch from Primary DB (Metadata & Category) ---
    try:
        placeholders = ','.join([':id_' + str(i) for i in range(len(ticket_ids_str))])
        # Using only columns that definitely exist in the database
        init_query = text(f"""
            SELECT
                t.ticketid, t.subject, t.ticketstatustitle, t.departmenttitle,
                FROM_UNIXTIME(t.dateline) AS created_date,
                FROM_UNIXTIME(t.lastactivity) AS last_activity_date
            FROM sw_tickets t
            WHERE t.ticketid IN ({placeholders})
        """)
        params = {f'id_{i}': tid for i, tid in enumerate(ticket_ids_str)}
        init_result = session.execute(init_query, params).mappings().all()

        if not init_result:
             logging.warning("Initial query returned no results for provided ticket IDs.")
             return {}

        for row_mapping in init_result:
            row = dict(row_mapping)
            ticket_id = str(row.get('ticketid'))
            dept = row.get('departmenttitle')
            subject = row.get('subject')
            status = row.get('ticketstatustitle')

            # Determine category (copied from previous logic)
            if dept == 'Turnups': category = "Turnup Tickets"
            elif dept in ['Dispatch', 'Pro Services', 'FST Accounting']: category = "Dispatch Tickets"
            elif dept in ['Shipping', 'Outbound', 'Inbound']: category = "Shipping Tickets"
            elif dept == 'Turn up Projects': category = "Project Management Tickets"
            else: category = "Other Tickets"

            # Initialize dict for this ticket
            ticket_data[ticket_id] = {
                'ticket_id': ticket_id,
                'subject': subject,
                'status': status,
                'department': dept,
                'category': category,
                # Use subject to determine phase since the phase column doesn't exist
                'phase': get_phase(row.get('subject')),
                # These fields don't exist in the database schema
                'parent_dispatch_id': None,
                'customer_feedback': None,
                'site_contact_name': None,
                'created_date': str(row.get('created_date')) if row.get('created_date') else None,
                'last_activity_date': str(row.get('last_activity_date')) if row.get('last_activity_date') else None,
                'posts': [],
                'notes': [],
                'all_posts_data': [],
                'all_notes_data': [],
                'earliest_post_timestamp': None, # Initialize for later update
                'latest_post_timestamp': None, # Initialize for later update
                'technical_details': '' # Initialize for later update
            }

    except Exception as e:
        logging.error(f"Error during initial ticket data fetch from primary DB: {e}", exc_info=True)
        raise

    # --- 2. Fetch Header Data from CISSDM (Conditional) ---
    dispatch_ids_to_fetch = [tid for tid, data in ticket_data.items() if data.get('category') == "Dispatch Tickets"]
    turnup_ids_to_fetch = [tid for tid, data in ticket_data.items() if data.get('category') == "Turnup Tickets"]

    if dispatch_ids_to_fetch and cissdm_session:
        try:
            logging.info(f"Fetching Dispatch headers from CISSDM for IDs: {dispatch_ids_to_fetch}")
            dispatch_placeholders = ','.join([f':did_{i}' for i in range(len(dispatch_ids_to_fetch))])
            dispatch_query = text(f"""
                SELECT id, id_turnup, id_wo, id_customer, customername, subject,
                       statusDispatch, statusTurnup, ticketType, serviceDate, serviceTime,
                       ticketPriority, postFirstDetails, postLastDetails, dateCreated,
                       department, projectId, billableRate, FSTHourlyCosts, FSTHourlyCostsToCustomer,
                       FSTFinalBilledToCIS, siteNumber, created_at, updated_at
                FROM dispatches WHERE id IN ({dispatch_placeholders})
            """)
            dispatch_params = {f'did_{i}': tid for i, tid in enumerate(dispatch_ids_to_fetch)}
            dispatch_results = cissdm_session.execute(dispatch_query, dispatch_params).mappings().all()
            for row_mapping in dispatch_results:
                row = dict(row_mapping)
                tid_str = str(row.get('id'))
                if tid_str in ticket_data:
                    ticket_data[tid_str]['header_data'] = row # Overwrite
                    ticket_data[tid_str]['status'] = row.get('statusDispatch', ticket_data[tid_str]['status'])
                    ticket_data[tid_str]['subject'] = row.get('subject', ticket_data[tid_str]['subject'])
        except Exception as e:
            logging.error(f"Error fetching dispatch headers from CISSDM: {e}", exc_info=True)
    elif dispatch_ids_to_fetch:
        logging.warning("CISSDM session not available for dispatch headers.")

    if turnup_ids_to_fetch and cissdm_session:
        try:
            logging.info(f"Fetching Turnup headers from CISSDM for IDs: {turnup_ids_to_fetch}")
            turnup_placeholders = ','.join([f':tid_{i}' for i in range(len(turnup_ids_to_fetch))])
            turnup_query = text(f"""
                SELECT ticketid, DispatchId, subject, ticketstatustitle, ServiceDate, CustomerName,
                       CISTechnicianName, InTime, OutTime, TurnupNotes, DispatchNotes, technicianGrade,
                       technicianComment, FailureCode, FailureCodeOther, pmreview, closeOutNotes,
                       brief_summary_for_invoice, isresolved, created_at, last_activity,
                       updated_at, closed_at, NextDueTime, SiteNumber, Postponed, ExpectedTimeIn
                FROM turnups WHERE ticketid IN ({turnup_placeholders})
            """)
            turnup_params = {f'tid_{i}': tid for i, tid in enumerate(turnup_ids_to_fetch)}
            turnup_results = cissdm_session.execute(turnup_query, turnup_params).mappings().all()
            for row_mapping in turnup_results:
                row = dict(row_mapping)
                tid_str = str(row.get('ticketid'))
                if tid_str in ticket_data:
                    ticket_data[tid_str]['header_data'] = row # Overwrite
                    ticket_data[tid_str]['parent_dispatch_id'] = row.get('DispatchId')
                    ticket_data[tid_str]['status'] = row.get('ticketstatustitle', ticket_data[tid_str]['status'])
                    ticket_data[tid_str]['subject'] = row.get('subject', ticket_data[tid_str]['subject'])
        except Exception as e:
            logging.error(f"Error fetching turnup headers from CISSDM: {e}", exc_info=True)
    elif turnup_ids_to_fetch:
         logging.warning("CISSDM session not available for turnup headers.")

    # --- 3. Fetch Posts and Notes from Primary DB ---
    try:
        for ticket_id_str in ticket_ids_str:
            if ticket_id_str not in ticket_data:
                logging.warning(f"Skipping posts/notes fetch for missing ticket {ticket_id_str}.")
                continue

            posts_result = []
            notes_result = []
            # Fetch Posts
            try:
                posts_query = text("""
                    SELECT ticketpostid, dateline AS post_dateline, userid, fullname, contents FROM sw_ticketposts WHERE ticketid = :ticket_id ORDER BY dateline
                """)
                posts_result = session.execute(posts_query, {'ticket_id': ticket_id_str}).mappings().all()
            except Exception as e:
                 logging.error(f"Error fetching posts for ticket {ticket_id_str}: {e}", exc_info=True)

            # Fetch Notes
            try:
                notes_query = text("""
                    SELECT ticketnoteid, linktypeid, dateline as note_dateline, staffname, note FROM sw_ticketnotes WHERE linktypeid = :ticket_id ORDER BY note_dateline
                """)
                notes_result = session.execute(notes_query, {'ticket_id': ticket_id_str}).mappings().all()
            except Exception as e:
                 logging.error(f"Error fetching notes for ticket {ticket_id_str}: {e}", exc_info=True)

            # Process Posts
            all_posts = []
            basic_posts = []
            tech_details_snippets = []
            post_timestamps = []
            for post_mapping in posts_result:
                 post_dict = dict(post_mapping)
                 formatted_timestamp = None
                 dateline_val = post_dict.get('post_dateline')
                 if dateline_val:
                     try:
                         dt_obj = datetime.fromtimestamp(int(dateline_val))
                         formatted_timestamp = dt_obj.strftime('%Y-%m-%d %H:%M:%S')
                         post_timestamps.append(dt_obj)
                         post_dict['post_dateline_formatted'] = formatted_timestamp
                     except (TypeError, ValueError): pass # Ignore formatting errors
                 all_posts.append(post_dict)
                 basic_posts.append({"content": post_dict.get('contents'), "author": post_dict.get('fullname'), "timestamp": formatted_timestamp})
                 # Extract technical details
                 content_lower = (post_dict.get('contents') or "").lower()
                 if any(kw in content_lower for kw in ['cable', 'cat6', 'rack', 'network', 'config', 'fortinet', 'fortigate', 'fortiswitch', 'fortiap', 'equipment']):
                      tech_details_snippets.append(f"Post ({formatted_timestamp or 'date unavailable'}): {post_dict.get('contents', '')[:150]}...")

            ticket_data[ticket_id_str]["all_posts_data"] = all_posts
            ticket_data[ticket_id_str]["posts"] = basic_posts
            ticket_data[ticket_id_str]["technical_details"] = "\n".join(tech_details_snippets)
            if post_timestamps:
                 ticket_data[ticket_id_str]['earliest_post_timestamp'] = min(post_timestamps).strftime('%Y-%m-%d %H:%M:%S')
                 ticket_data[ticket_id_str]['latest_post_timestamp'] = max(post_timestamps).strftime('%Y-%m-%d %H:%M:%S')

            # Process Notes
            all_notes = []
            basic_notes = []
            for note_mapping in notes_result:
                 note_dict = dict(note_mapping)
                 formatted_timestamp = None
                 dateline_val = note_dict.get('note_dateline')
                 if dateline_val:
                     try:
                         formatted_timestamp = datetime.fromtimestamp(int(dateline_val)).strftime('%Y-%m-%d %H:%M:%S')
                         note_dict['note_dateline_formatted'] = formatted_timestamp
                     except (TypeError, ValueError): pass
                 all_notes.append(note_dict)
                 basic_notes.append({"content": note_dict.get('note'), "author": note_dict.get('staffname'), "timestamp": formatted_timestamp})

            ticket_data[ticket_id_str]["all_notes_data"] = all_notes
            ticket_data[ticket_id_str]["notes"] = basic_notes

    except Exception as e:
        logging.error(f"Error during main post/note fetch loop: {e}", exc_info=True)
        # Continue even if some tickets fail post/note fetch

    # --- 4. Final Validation ---
    missing_data_keys = set(ticket_ids_str) - set(ticket_data.keys())
    if missing_data_keys:
        logging.warning(f"Data structure missing entirely for ticket IDs: {missing_data_keys}")

    logging.info(f"Finished fetching details for {len(ticket_data)} tickets.")
    return ticket_data

# --- Phase 2 File Preparation ---
def create_ticket_files(chain_details, ticket_data, phase1_analysis):
    """Creates consolidated JSON files, context, and relationship files."""
    output_dir = 'PyChain/data/ticket_files'
    os.makedirs(output_dir, exist_ok=True)
    chain_hash = chain_details['chain_hash']
    file_paths = []

    # --- 1. Chain metadata file ---
    chain_file = os.path.join(output_dir, f'chain_{chain_hash}.json')
    try:
        chain_meta = {
             "chain_hash": chain_hash,
             "ticket_count": chain_details['ticket_count'],
             "ticket_ids": list(ticket_data.keys())
        }
        with open(chain_file, 'w') as f: json.dump(chain_meta, f, indent=2)
        file_paths.append(chain_file)
        logging.info(f"Created chain metadata file: {chain_file}")
    except Exception as e: logging.error(f"Error creating chain metadata file {chain_file}: {e}")

    # --- 2. Consolidated ticket data file ---
    tickets_file = os.path.join(output_dir, f'tickets_{chain_hash}.json')
    try:
        with open(tickets_file, 'w') as f: json.dump(ticket_data, f, indent=2, default=str)
        file_paths.append(tickets_file)
        logging.info(f"Created consolidated tickets file: {tickets_file}")
    except Exception as e: logging.error(f"Error creating consolidated tickets file {tickets_file}: {e}")

    # --- 3. Phase 1 analysis file ---
    phase1_file = os.path.join(output_dir, f'phase1_analysis_{chain_hash}.json')
    try:
        phase1_data = {"chain_hash": chain_hash, "phase1_summary": phase1_analysis}
        with open(phase1_file, 'w') as f: json.dump(phase1_data, f, indent=2)
        file_paths.append(phase1_file)
        logging.info(f"Created phase 1 analysis file: {phase1_file}")
    except Exception as e: logging.error(f"Error creating phase 1 analysis file {phase1_file}: {e}")

    # --- 4. Relationships file (Example) ---
    relationships_file = os.path.join(output_dir, f'relationships_{chain_hash}.json')
    try:
        relationships_data = {
          "chain_hash": chain_hash,
          # Use example relationships provided by user
          "relationships": [
            {"dispatch_ticket_id": "2376830", "turnup_ticket_ids": ["2380336"], "phase": "Turnup Assist", "confidence": "High"},
            {"dispatch_ticket_id": "2382333", "turnup_ticket_ids": ["2382726", "2389439"], "phase": "P1", "confidence": "High"},
            {"dispatch_ticket_id": "2382333", "turnup_ticket_ids": ["2382746", "2389461"], "phase": "P2", "confidence": "Medium", "notes": "P2 turnups potentially linked to P1 dispatch via project scope."},
            {"dispatch_ticket_id": "2384350", "turnup_ticket_ids": ["2385184"], "phase": "Outlet Install", "confidence": "High"}
          ]
        }
        # Add dynamically found relationships
        dispatch_to_turnups = {}
        for tid, data in ticket_data.items():
             if data.get('category') == 'Turnup Tickets' and data.get('parent_dispatch_id'):
                  dispatch_id = str(data['parent_dispatch_id'])
                  if dispatch_id not in dispatch_to_turnups: dispatch_to_turnups[dispatch_id] = []
                  if tid not in dispatch_to_turnups[dispatch_id]: dispatch_to_turnups[dispatch_id].append(tid)
        for dispatch_id, turnup_ids in dispatch_to_turnups.items():
             existing = next((r for r in relationships_data["relationships"] if r["dispatch_ticket_id"] == dispatch_id), None)
             if not existing:
                  relationships_data["relationships"].append({"dispatch_ticket_id": dispatch_id, "turnup_ticket_ids": turnup_ids, "phase": ticket_data.get(dispatch_id, {}).get('phase', 'Unknown'), "confidence": "High (Direct Link)"})
             else:
                  # Optionally merge turnup IDs if link found multiple ways
                  pass

        with open(relationships_file, 'w') as f: json.dump(relationships_data, f, indent=2)
        file_paths.append(relationships_file)
        logging.info(f"Created relationships file: {relationships_file}")
    except Exception as e: logging.error(f"Error creating relationships file {relationships_file}: {e}")

    # --- 5. Context Rules File (Copy existing) ---
    rules_src_path = "PyChain/Ticket_Records_Information_and_Rules.txt"
    rules_dest_path = os.path.join(output_dir, 'Ticket_Records_Information_and_Rules.txt')
    try:
        if os.path.exists(rules_src_path):
            import shutil
            shutil.copyfile(rules_src_path, rules_dest_path)
            file_paths.append(rules_dest_path)
            logging.info(f"Copied context rules file to: {rules_dest_path}")
        else:
            logging.warning(f"Source context rules file not found at {rules_src_path}, skipping.")
    except Exception as e:
        logging.error(f"Error copying context rules file: {e}")

    return file_paths


# --- Phase 2 File Upload (Unchanged from previous version) ---
def upload_files(file_paths, max_retries=3):
    """Upload files to OpenAI API with retries."""
    file_ids = []
    for path in file_paths:
        if not os.path.exists(path):
             logging.error(f"File not found for upload: {path}. Skipping.")
             continue
        for attempt in range(max_retries):
            try:
                logging.info(f"Uploading {os.path.basename(path)} (Attempt {attempt + 1}/{max_retries})...")
                with open(path, 'rb') as file:
                    upload = client.files.create(file=file, purpose="assistants")
                file_ids.append(upload.id)
                logging.info(f"Successfully uploaded {os.path.basename(path)} -> File ID: {upload.id}")
                break
            except Exception as e:
                logging.warning(f"File upload failed for {path}, attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    logging.error(f"Max retries reached for {path}. Upload failed.")
                else:
                    time.sleep(2 ** attempt)
    return file_ids

# --- Phase 2 Vector Store Processing Wait (Unchanged from previous version) ---
def wait_for_vector_store_processing(vector_store_id, file_ids, timeout=300):
    """Polls vector store file status until all files are processed or timeout."""
    start_time = time.time()
    processed_files = set()
    all_files = set(file_ids)
    if not all_files: return True
    logging.info(f"Waiting for processing of {len(file_ids)} files in vector store {vector_store_id}...")
    while time.time() - start_time < timeout:
        remaining_files = list(all_files - processed_files)
        if not remaining_files:
            logging.info("All files processed in vector store.")
            return True
        all_processed_in_batch = True
        current_check_processed = set()
        try:
            for file_id in remaining_files:
                 time.sleep(0.5)
                 try:
                     file_status = client.vector_stores.files.retrieve(vector_store_id=vector_store_id, file_id=file_id)
                     if file_status.status == 'completed':
                         processed_files.add(file_id)
                         current_check_processed.add(file_id)
                     elif file_status.status in ['failed', 'cancelled']:
                         logging.error(f"File {file_id} failed processing in vector store {vector_store_id} with status: {file_status.status}")
                         all_files.remove(file_id)
                     else: all_processed_in_batch = False
                 except Exception as file_retrieve_error:
                      logging.error(f"Error retrieving status for file {file_id} in vector store {vector_store_id}: {file_retrieve_error}")
                      all_processed_in_batch = False
            if current_check_processed: logging.info(f"Processed {len(current_check_processed)} files. Total processed: {len(processed_files)}/{len(all_files)}")
            if all_processed_in_batch and not (all_files - processed_files):
                 logging.info("All files processed in vector store.")
                 return True
        except Exception as e:
            logging.error(f"Unexpected error during vector store file status check: {e}")
            time.sleep(5)
        if not (all_files - processed_files):
             logging.info("All files processed or failed processing.")
             return True
        logging.info(f"Still waiting for {len(all_files - processed_files)} files... ({(time.time() - start_time):.1f}s elapsed)")
        time.sleep(5)
    logging.error(f"Vector store processing timeout after {timeout} seconds. Files not processed: {all_files - processed_files}")
    return False

# --- Phase 2 Response Validation (with duplicate check) ---
def validate_response(response_text, expected_ticket_ids_str):
    """Validates the Assistant's JSON response, checks for missing/unsupported, removes duplicates."""
    expected_ids = set(expected_ticket_ids_str)
    if not response_text: # Handle empty response
        logging.error("Validation Error: Empty response received from Assistant.")
        return None
    try:
        # Clean potential markdown
        response_text = response_text.strip()
        if response_text.startswith("```json"): response_text = response_text[7:]
        if response_text.startswith("```"): response_text = response_text[3:]
        if response_text.endswith("```"): response_text = response_text[:-3]
        response_text = response_text.strip()

        response_json = json.loads(response_text)
        found_ids = set()
        processed_json = response_json # Start with original parsed JSON

        # --- Identify found ticket IDs and Remove Duplicates ---
        if isinstance(response_json, list):
            unique_items = []
            seen_ids = set()
            for item in response_json:
                if isinstance(item, dict):
                    tid = item.get("TicketID", item.get("ticket_id", item.get("TaskID")))
                    if tid:
                        tid_str = str(tid)
                        found_ids.add(tid_str)
                        if tid_str not in seen_ids:
                            unique_items.append(item) # Add item only if ID is new
                            seen_ids.add(tid_str)
                        else:
                             logging.warning(f"Duplicate ticket ID {tid_str} removed from list response.")
                    else:
                         unique_items.append(item) # Keep items without identifiable ID?
                else:
                     unique_items.append(item) # Keep non-dict items
            processed_json = unique_items # Update processed JSON to the unique list

        elif isinstance(response_json, dict):
            # Handle Job Scope Tasks duplicates
            if "JobScopeTasks" in response_json and isinstance(response_json["JobScopeTasks"], list):
                unique_tasks = []
                seen_task_ids = set()
                for task in response_json["JobScopeTasks"]:
                    if isinstance(task, dict):
                         tid = task.get("TicketID", task.get("TaskID"))
                         if tid:
                             tid_str = str(tid)
                             found_ids.add(tid_str)
                             if tid_str not in seen_task_ids:
                                 unique_tasks.append(task)
                                 seen_task_ids.add(tid_str)
                             else:
                                  logging.warning(f"Duplicate task/ticket ID {tid_str} removed from JobScopeTasks.")
                         else:
                              unique_tasks.append(task)
                    else:
                         unique_tasks.append(task)
                response_json["JobScopeTasks"] = unique_tasks # Modify dict in place
                processed_json = response_json
            # Add checks for other dictionary structures if needed

        # --- Validation Checks ---
        missing_tickets = expected_ids - found_ids
        if missing_tickets:
            logging.warning(f"Validation Warning: Assistant response missing expected ticket IDs: {missing_tickets}")
        else:
            # Check if MORE IDs were found than expected (might indicate unrelated tickets included)
            extra_tickets = found_ids - expected_ids
            if extra_tickets:
                 logging.warning(f"Validation Warning: Response included unexpected ticket IDs: {extra_tickets}")
            else:
                 logging.info(f"Validation Info: Response includes data for all {len(found_ids)} expected tickets.")

        # Check for unsupported fields (remains illustrative)
        unsupported_indicators = ["Customer Feedback Score", "Skill Match", "Completion Percentage"]
        response_str_lower = response_text.lower()
        for indicator in unsupported_indicators:
            if f'"{indicator.lower()}":' in response_str_lower:
                logging.warning(f"Validation Warning: Potentially unsupported field indicator '{indicator}' detected.")

        return processed_json # Return the processed (de-duplicated) JSON

    except json.JSONDecodeError as json_err:
        logging.error(f"Validation Error: Invalid JSON: {json_err}\nRaw Response:\n{response_text}")
        return None
    except Exception as e:
        logging.error(f"Validation Error: Unexpected: {e}\nRaw Response:\n{response_text}", exc_info=True)
        return None


# --- Helper to Run Assistant Query ---
def run_assistant_query(thread_id, assistant_id, prompt, timeout_seconds=600):
    """Adds a message, creates a run, polls, and returns the assistant's response text."""
    try:
        client.beta.threads.messages.create(thread_id=thread_id, role="user", content=prompt)
        run = client.beta.threads.runs.create(thread_id=thread_id, assistant_id=assistant_id)
        logging.info(f"Run created (ID: {run.id}) for prompt starting with: {prompt[:100]}...")
        start_poll_time = time.time()
        while time.time() - start_poll_time < timeout_seconds:
            run_status = client.beta.threads.runs.retrieve(thread_id=thread_id, run_id=run.id)
            if run_status.status == "completed":
                logging.info(f"Run {run.id} completed.")
                break
            elif run_status.status in ["failed", "cancelled", "expired"]:
                logging.error(f"Run {run.id} finished with non-completed status: {run_status.status}. Error: {run_status.last_error}")
                raise Exception(f"Run failed with status: {run_status.status}")
            elif run_status.status == 'requires_action':
                 logging.warning(f"Run {run.id} requires action: {run_status.required_action}")
                 time.sleep(15) # Give time for potential action to be processed if automatic
            else:
                logging.info(f"Waiting for run {run.id} (Status: {run_status.status})... ({(time.time() - start_poll_time):.0f}s elapsed)")
                time.sleep(10) # Increase polling interval
        else: raise TimeoutError(f"Run {run.id} timed out after {timeout_seconds} seconds.")
        messages = client.beta.threads.messages.list(thread_id=thread_id, order="desc", limit=5)
        for msg in messages.data:
            if msg.role == "assistant" and msg.run_id == run.id:
                if msg.content and isinstance(msg.content, list) and len(msg.content) > 0:
                    content_block = msg.content[0]
                    if hasattr(content_block, 'text') and content_block.text: return content_block.text.value
                    else: logging.warning(f"Assistant msg content block not text: {content_block}")
                else: logging.warning(f"Assistant msg content empty/invalid: {msg.content}")
        raise Exception("No valid assistant message found for the completed run.")

    except Exception as e:
        logging.error(f"Error in run_assistant_query: {e}", exc_info=True)
        raise


# --- Phase Extraction Helper ---
def get_phase(subject):
    """Extract the phase from the ticket subject (improved)."""
    if not subject: return "Unknown"
    subject_upper = subject.upper()
    if "CABLING AND NETWORK UPGRADE" in subject_upper: return "Project Planning"
    if "SITE SURVEY" in subject_upper: return "Site Survey"
    if "OUTLET INSTALL" in subject_upper: return "Outlet Install"
    if "TURNUP ASSIST" in subject_upper: return "Turnup Assist"
    if "50 PERCENT BILLING" in subject_upper: return "Billing Milestone"
    if "BILLING COMPLETE" in subject_upper: return "Billing Complete"
    if "P1" in subject_upper: return "P1"
    if "P2" in subject_upper: return "P2"
    if "P3" in subject_upper: return "P3"
    if "REVISIT" in subject_upper: return "Revisit"
    if "SHIPPING" in subject_upper: pass # Avoid generic classification for Shipping
    return "Other"


# --- Phase 1 Analysis Function ---
def run_phase_1_report(chain_details):
    """Run Phase 1 analysis, save results, handle errors."""
    if "error" in chain_details or not chain_details.get('chain_hash'):
        logging.error(f"Invalid chain_details for Phase 1: {chain_details.get('error', 'Missing chain_hash')}")
        return None, "Error: Invalid chain details provided."

    logging.info(f"\nRunning {PHASE1_REPORT_TYPE['name']} (Phase 1) for chain {chain_details['chain_hash']}...")
    try:
        prompt = TicketChainService._create_chain_analysis_prompt(chain_details, PHASE1_REPORT_TYPE['id'])
    except Exception as prompt_err:
         logging.error(f"Error creating Phase 1 prompt: {prompt_err}")
         return None, f"Error: {prompt_err}"
    try:
        analysis = AIService.analyze_chain(prompt, PHASE1_REPORT_TYPE['id'])
    except Exception as ai_err:
        logging.error(f"Error during Phase 1 AI analysis: {ai_err}")
        analysis = f"Error: {ai_err}"
    print("\n" + "=" * 50 + f"\nANALYSIS: {PHASE1_REPORT_TYPE['name']} (Phase 1)\n" + "=" * 50)
    logging.info("Phase 1 analysis complete. Summary follows:")
    print(analysis[:500] + "..." if len(analysis) > 500 else analysis)
    print("=" * 50)

    output_dir = "PyChain/data/ticket_files"
    analyses_dir = "PyChain/data/analyses"
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(analyses_dir, exist_ok=True)

    summary_file = os.path.join(output_dir, f"summary_{chain_details['chain_hash']}.json")
    summary_data = { "chain_hash": chain_details['chain_hash'], "ticket_count": chain_details.get('ticket_count', 0), "analysis": analysis, "timestamp": datetime.now().isoformat() }
    try:
        with open(summary_file, "w") as f:
            json.dump(summary_data, f, indent=4)
        logging.info(f"Phase 1 Summary saved to {summary_file}")
    except Exception as e:
         logging.error(f"Failed to save Phase 1 summary JSON: {e}")
         summary_file = None
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(analyses_dir, f"{chain_details['chain_hash']}_{timestamp}_{PHASE1_REPORT_TYPE['id']}.txt")
    try:
        with open(filename, "w") as f:
            f.write(f"Ticket Chain Analysis - Phase 1\nReport Type: {PHASE1_REPORT_TYPE['name']}\nChain Hash: {chain_details['chain_hash']}\nTickets: {chain_details.get('ticket_count', 0)}\nGenerated: {datetime.now():%Y-%m-%d %H:%M:%S}\n\n{'='*50}\n{analysis}")
        logging.info(f"Phase 1 Analysis text saved to {filename}")
    except Exception as e:
         logging.error(f"Failed to save Phase 1 analysis text file: {e}")

    return summary_file, analysis


# --- Phase 2 Core Logic (Redesigned Batch and Iterative Approach) ---
def run_phase_2_analysis(chain_details, phase1_analysis_text):
    """Run Phase 2 analysis using batch processing and iterative questioning for detailed metrics."""
    assistant_id, vector_store_id = setup_vector_store_and_assistant()
    if not assistant_id or not vector_store_id: return

    logging.info(f"\n--- Starting Phase 2 Analysis (Assistant: {assistant_id}, Store: {vector_store_id}) ---")
    expected_ticket_ids = [str(ticket.get('ticketid')) for ticket in chain_details.get('tickets', []) if ticket.get('ticketid')]
    if not expected_ticket_ids:
        logging.error("No ticket IDs found for Phase 2.")
        return

    chain_hash = chain_details['chain_hash']
    file_ids_uploaded = []
    file_paths_created = []
    session = None
    cissdm_session = None
    final_responses = {"chain_hash": chain_hash, "ticket_count": len(expected_ticket_ids), "stages": {}}

    try:
        # --- Setup: DB & Files ---
        session = get_db_session("primary")
        cissdm_session = get_db_session("cissdm")
        if not session: raise ConnectionError("Failed primary DB connection.")

        logging.info("Fetching full ticket data...")
        ticket_data = fetch_full_ticket_data(session, cissdm_session, expected_ticket_ids)
        if not ticket_data: raise ValueError("Failed to fetch ticket data.")

        logging.info("Creating analysis files...")
        file_paths_created = create_ticket_files(chain_details, ticket_data, phase1_analysis_text)

        # --- Upload & Process (Minimal for Reference Only) ---
        # Note: We will primarily use direct prompt transmission, not vector stores
        logging.info("Uploading files for reference...")
        file_ids_uploaded = upload_files(file_paths_created)
        if not file_ids_uploaded: raise ValueError("File upload failed.")

        logging.info(f"Adding {len(file_ids_uploaded)} files to vector store {vector_store_id} for reference...")
        successful_adds = []
        for file_id in file_ids_uploaded:
            try:
                add_wait_time = 0; timeout = 120
                while add_wait_time < timeout:
                    file_info = client.files.retrieve(file_id)
                    if file_info.status == 'processed': break
                    if file_info.status == 'failed': raise Exception(f"OpenAI file {file_id} processing failed.")
                    time.sleep(5)
                    add_wait_time += 5
                if client.files.retrieve(file_id).status != 'processed':
                    raise TimeoutError(f"Timeout waiting for file {file_id} to process.")
                client.vector_stores.files.create(vector_store_id=vector_store_id, file_id=file_id)
                successful_adds.append(file_id)
                logging.info(f"Added file {file_id} to vector store.")
            except Exception as e:
                logging.error(f"Error adding file {file_id} to vector store: {e}. Skipping.")
        if not successful_adds: raise ValueError("No files added to vector store.")

        logging.info("Waiting for vector store file processing...")
        if not wait_for_vector_store_processing(vector_store_id, successful_adds):
            raise TimeoutError("File processing in vector store timed out.")

        # --- Link Assistant & Create Thread ---
        try:
            # First, check if the assistant exists
            try:
                client.beta.assistants.retrieve(assistant_id=assistant_id)
                logging.info(f"Assistant {assistant_id} found, proceeding with update.")
                client.beta.assistants.update(assistant_id=assistant_id, tool_resources={"file_search": {"vector_store_ids": [vector_store_id]}})
                logging.info(f"Assistant {assistant_id} linked to store {vector_store_id}.")
            except openai.NotFoundError as not_found_err:
                logging.warning(f"Assistant {assistant_id} not found: {not_found_err}. Creating a new assistant.")
                # Clear the old ID and create a new assistant
                assistant = setup_vector_store_and_assistant()[1]
                if assistant:
                    assistant_id = assistant.id
                    update_env_file(assistant_id, vector_store_id)
                    logging.info(f"New assistant created with ID: {assistant_id} and linked to store {vector_store_id}.")
                else:
                    logging.error("Failed to create a new assistant.")
                    raise
            thread = client.beta.threads.create()
            if not thread or not thread.id: raise ConnectionError("Failed thread creation.")
            logging.info(f"Analysis thread created: {thread.id}")
        except Exception as e:
            logging.error(f"Error linking Assistant or creating Thread: {e}", exc_info=True)
            raise

        # --- Redesigned Phase 2: Batch Processing and Iterative Analysis ---
        ticket_ids_list_str = ", ".join(f'"{tid}"' for tid in expected_ticket_ids)

        # Step 1: Batch Analysis of Ticket Pairs/Groups
        logging.info("\n--- Running Phase 2 Step 1: Batch Analysis of Tickets ---")
        batches = create_ticket_batches(ticket_data, expected_ticket_ids)
        batch_results = []
        for i, batch in enumerate(batches):
            logging.info(f"Analyzing Batch {i+1}/{len(batches)} with {len(batch['ticket_ids'])} tickets...")
            batch_prompt = create_batch_analysis_prompt(chain_hash, batch, ticket_data)
            try:
                batch_response = run_assistant_query(thread.id, assistant_id, batch_prompt)
                validated_json = validate_response(batch_response, batch['ticket_ids'])
                batch_results.append({
                    "batch_id": i+1,
                    "ticket_ids": batch['ticket_ids'],
                    "result": validated_json if validated_json else {"error": "Invalid JSON", "raw": batch_response}
                })
                final_responses["stages"][f"Batch_{i+1}_Analysis"] = batch_results[-1]
                print(f"\n--- Batch {i+1} Analysis Result (JSON) ---")
                if validated_json: print(json.dumps(validated_json, indent=2))
                else: print(f"ERROR: Invalid JSON.\nRaw:\n{batch_response}")
                print("-----------------------------------")
            except Exception as e:
                logging.error(f"Batch {i+1} analysis failed: {e}")
                batch_results.append({"batch_id": i+1, "ticket_ids": batch['ticket_ids'], "error": str(e)})
                final_responses["stages"][f"Batch_{i+1}_Analysis"] = {"error": str(e)}

        # Step 2: Issue Indexing
        logging.info("\n--- Running Phase 2 Step 2: Issue Indexing ---")
        issues_index = compile_issues_index(batch_results)
        final_responses["stages"]["Issues_Index"] = issues_index
        print("\n--- Issues Index ---")
        print(json.dumps(issues_index, indent=2))
        print("--------------------")

        # Step 3: Follow-up Question Generation
        logging.info("\n--- Running Phase 2 Step 3: Follow-up Question Generation ---")
        questions_prompt = create_questions_prompt(chain_hash, issues_index, ticket_ids_list_str)
        try:
            questions_response = run_assistant_query(thread.id, assistant_id, questions_prompt)
            questions_json = validate_response(questions_response, expected_ticket_ids)
            final_responses["stages"]["Followup_Questions"] = questions_json if questions_json else {"error": "Invalid JSON", "raw": questions_response}
            print("\n--- Follow-up Questions (JSON) ---")
            if questions_json: print(json.dumps(questions_json, indent=2))
            else: print(f"ERROR: Invalid JSON.\nRaw:\n{questions_response}")
            print("--------------------------------")
        except Exception as e:
            logging.error(f"Follow-up question generation failed: {e}")
            final_responses["stages"]["Followup_Questions"] = {"error": str(e)}

        # Step 4: User Input for Custom Questions
        logging.info("\n--- Running Phase 2 Step 4: User Input for Custom Questions ---")
        try:
            user_question = input("\nDo you have a specific question about these dispatch events to ask for each relevant ticket? (type your question or 'no' to skip): ").strip()
            user_questions = {}
            if user_question.lower() != 'no' and user_question:
                user_questions = {"global_question": user_question}
                logging.info(f"User added custom question: {user_question}")
            else:
                logging.info("User skipped adding a custom question.")
            final_responses["stages"]["User_Questions"] = user_questions
        except Exception as e:
            logging.error(f"Error getting user input for custom questions: {e}")
            final_responses["stages"]["User_Questions"] = {"error": str(e)}

        # Step 5: Detailed Re-Analysis with Questions
        logging.info("\n--- Running Phase 2 Step 5: Detailed Re-Analysis with Questions ---")
        detailed_results = []
        tickets_with_questions = get_tickets_with_questions(questions_json if questions_json else {}, user_questions, ticket_data)
        for ticket_id, q_data in tickets_with_questions.items():
            logging.info(f"Re-analyzing ticket {ticket_id} with specific questions...")
            detail_prompt = create_detailed_analysis_prompt(chain_hash, ticket_id, ticket_data, q_data["questions"])
            try:
                detail_response = run_assistant_query(thread.id, assistant_id, detail_prompt)
                detail_json = validate_response(detail_response, [ticket_id])
                detailed_results.append({
                    "ticket_id": ticket_id,
                    "result": detail_json if detail_json else {"error": "Invalid JSON", "raw": detail_response}
                })
                final_responses["stages"][f"Detailed_Analysis_Ticket_{ticket_id}"] = detailed_results[-1]
                print(f"\n--- Detailed Analysis for Ticket {ticket_id} (JSON) ---")
                if detail_json: print(json.dumps(detail_json, indent=2))
                else: print(f"ERROR: Invalid JSON.\nRaw:\n{detail_response}")
                print("-----------------------------------")
            except Exception as e:
                logging.error(f"Detailed analysis for ticket {ticket_id} failed: {e}")
                detailed_results.append({"ticket_id": ticket_id, "error": str(e)})
                final_responses["stages"][f"Detailed_Analysis_Ticket_{ticket_id}"] = {"error": str(e)}

        # Step 6: Final Consolidation
        logging.info("\n--- Running Phase 2 Step 6: Final Consolidation ---")
        consolidated_report = consolidate_final_report(batch_results, issues_index, detailed_results, ticket_data, chain_hash)
        final_responses["consolidated_report"] = consolidated_report
        print("\n--- Consolidated Final Report (JSON) ---")
        print(json.dumps(consolidated_report, indent=2))
        print("---------------------------------------")

        # --- Save Final Output ---
        logging.info("\nPhase 2 analysis complete.")
        final_output_file = f"PyChain/data/analyses/Phase2_BatchIterative_{chain_hash}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        try:
            with open(final_output_file, 'w') as f:
                json.dump(final_responses, f, indent=2)
            logging.info(f"Saved combined Phase 2 results to {final_output_file}")
        except Exception as e:
            logging.error(f"Failed to save combined Phase 2 JSON output: {e}")

    except Exception as e:
        logging.error(f"CRITICAL ERROR during Phase 2 analysis: {e}", exc_info=True)
    finally:
        # Close DB sessions
        if session:
            try: session.close(); logging.info("Primary DB session closed.")
            except Exception as db_close_err: logging.error(f"Error closing primary DB session: {db_close_err}")
        if cissdm_session:
            try: cissdm_session.close(); logging.info("CISSDM DB session closed.")
            except Exception as db_close_err: logging.error(f"Error closing CISSDM DB session: {db_close_err}")
        # Clean up local files
        logging.info("Cleaning up temporary local files...")
        for file_path in file_paths_created:
            try:
                if os.path.exists(file_path): os.remove(file_path)
            except Exception as e: logging.warning(f"Error removing local file {file_path}: {e}")
        # Ask user about OpenAI cleanup
        if file_ids_uploaded:
            try:
                cleanup_response = input("\nDo you want to clean up OpenAI resources (files)? (y/n): ").strip().lower()
                if cleanup_response == 'y':
                    delete_store = input("Also delete the vector store? (y/n): ").strip().lower() == 'y'
                    cleanup_openai_resources(file_ids_uploaded, vector_store_id, delete_store)
            except EOFError: logging.warning("Skipping OpenAI cleanup prompt (non-interactive).")
            except Exception as input_err: logging.error(f"Error during cleanup prompt: {input_err}")
        else: logging.info("Skipping OpenAI cleanup as no files were uploaded.")


# --- Helper Functions for Redesigned Phase 2 ---
def create_ticket_batches(ticket_data, expected_ticket_ids):
    """Create batches of related tickets for analysis to manage token limits."""
    batches = []
    dispatch_to_turnups = {}
    orphan_tickets = []

    # Map dispatch to turnup tickets
    for tid in expected_ticket_ids:
        data = ticket_data.get(tid, {})
        if data.get('category') == 'Turnup Tickets' and data.get('parent_dispatch_id'):
            dispatch_id = str(data['parent_dispatch_id'])
            if dispatch_id not in dispatch_to_turnups:
                dispatch_to_turnups[dispatch_id] = []
            dispatch_to_turnups[dispatch_id].append(tid)
        elif data.get('category') not in ['Dispatch Tickets', 'Turnup Tickets'] or not data.get('parent_dispatch_id'):
            orphan_tickets.append(tid)

    # Create batches from dispatch-turnup mappings
    for dispatch_id, turnup_ids in dispatch_to_turnups.items():
        if dispatch_id in ticket_data:
            batch = [dispatch_id] + turnup_ids
            batches.append({
                "ticket_ids": batch,
                "focus": f"Dispatch {dispatch_id} and related Turnups"
            })

    # Handle orphans in small batches
    orphan_batch_size = 3
    for i in range(0, len(orphan_tickets), orphan_batch_size):
        batch = orphan_tickets[i:i + orphan_batch_size]
        batches.append({
            "ticket_ids": batch,
            "focus": "Orphan or Uncategorized Tickets"
        })

    if not batches:
        # Fallback: Batch all tickets in groups if no clear mapping
        batch_size = 5
        for i in range(0, len(expected_ticket_ids), batch_size):
            batch = expected_ticket_ids[i:i + batch_size]
            batches.append({
                "ticket_ids": batch,
                "focus": "General Ticket Batch"
            })

    return batches

def create_batch_analysis_prompt(chain_hash, batch, ticket_data):
    """Create a prompt for analyzing a batch of tickets."""
    ticket_ids_str = ", ".join(f'"{tid}"' for tid in batch['ticket_ids'])
    prompt = f"""
Analyze the following batch of tickets for chain {chain_hash}. Focus on {batch['focus']}.
Ticket IDs in this batch: [{ticket_ids_str}].

Extract detailed metrics and issues for each ticket. Include:
- Ticket ID, Category, Status, and Subject.
- Timeline: Creation date, service date, completion date (if available).
- Scope: Technical details or work description (e.g., cabling, equipment).
- Outcome: Was the scope completed? Any issues or cancellations?
- Revisit: Was a revisit required? If so, why?
- Metrics: Time on site (if determinable), delays (if any).

Provide the response as a valid JSON list of objects, one per ticket. Cite evidence from ticket data where possible.

Ticket Data for Reference:
"""
    for tid in batch['ticket_ids']:
        data = ticket_data.get(tid, {})
        technical_details = data.get('technical_details', 'N/A')
        if len(technical_details) > 200:
            technical_details = technical_details[:200] + "..."
        prompt += f"- Ticket {tid}:\n"
        prompt += f"  - Category: {data.get('category', 'N/A')}\n"
        prompt += f"  - Subject: {data.get('subject', 'N/A')}\n"
        prompt += f"  - Status: {data.get('status', 'N/A')}\n"
        prompt += f"  - Created: {data.get('created_date', 'N/A')}\n"
        prompt += f"  - Last Activity: {data.get('last_activity_date', 'N/A')}\n"
        prompt += f"  - Technical Details: {technical_details}\n"
        prompt += f"  - Posts/Notes Count: {len(data.get('posts', []))} posts, {len(data.get('notes', []))} notes\n"
    prompt += "\nFollow instructions strictly. Output ONLY valid JSON."
    return prompt

def compile_issues_index(batch_results):
    """Compile a list of issues/problems per ticket from batch analysis results."""
    issues_index = {"tickets_with_issues": []}
    for batch in batch_results:
        if "result" in batch and isinstance(batch["result"], list):
            for ticket_analysis in batch["result"]:
                ticket_id = str(ticket_analysis.get("TicketID", ticket_analysis.get("ticket_id", "")))
                issues = ticket_analysis.get("Issues", ticket_analysis.get("Outcome", {}).get("Issues", []))
                if isinstance(issues, str):
                    issues = [issues]
                if issues and ticket_id:
                    issues_index["tickets_with_issues"].append({
                        "ticket_id": ticket_id,
                        "issues": issues
                    })
    return issues_index

def create_questions_prompt(chain_hash, issues_index, ticket_ids_list_str):
    """Create a prompt to generate follow-up questions for tickets with issues."""
    prompt = f"""
Based on the analysis for chain {chain_hash}, generate specific follow-up questions to clarify issues identified in the tickets.

Issues Index:
"""
    for entry in issues_index.get("tickets_with_issues", []):
        prompt += f"- Ticket {entry['ticket_id']}:\n  - Issues: {', '.join(entry['issues']) if isinstance(entry['issues'], list) else entry['issues']}\n"
    prompt += f"""

Provide ONLY a valid JSON object with a key `questions_by_ticket` mapping ticket IDs to a list of specific questions (max 3 per ticket) to clarify the issues.
Include all tickets [{ticket_ids_list_str}], even if no issues were found (use empty list for no questions).
"""
    return prompt

def get_tickets_with_questions(questions_json, user_questions, ticket_data):
    """Compile tickets that need re-analysis with AI-generated and user questions."""
    tickets_with_questions = {}
    if questions_json and isinstance(questions_json, dict):
        questions_by_ticket = questions_json.get("questions_by_ticket", {})
        for ticket_id, questions in questions_by_ticket.items():
            if questions and isinstance(questions, list):
                tickets_with_questions[ticket_id] = {"questions": questions}

    # Add user question to relevant tickets (dispatch or turnup)
    user_q = user_questions.get("global_question", "")
    if user_q:
        for ticket_id, data in ticket_data.items():
            if data.get("category") in ["Dispatch Tickets", "Turnup Tickets"]:
                if ticket_id not in tickets_with_questions:
                    tickets_with_questions[ticket_id] = {"questions": []}
                tickets_with_questions[ticket_id]["questions"].append(user_q)

    return tickets_with_questions

def create_detailed_analysis_prompt(chain_hash, ticket_id, ticket_data, questions):
    """Create a prompt for detailed re-analysis of a ticket with specific questions."""
    data = ticket_data.get(ticket_id, {})
    questions_str = "\n".join([f"- {q}" for q in questions])
    prompt = f"""
Perform a detailed analysis of ticket {ticket_id} for chain {chain_hash}. Focus on answering specific questions to clarify issues.

Ticket Data:
- Category: {data.get('category', 'N/A')}
- Subject: {data.get('subject', 'N/A')}
- Status: {data.get('status', 'N/A')}
- Created: {data.get('created_date', 'N/A')}
- Last Activity: {data.get('last_activity_date', 'N/A')}
"""
    technical_details = data.get('technical_details', 'N/A')
    if len(technical_details) > 300:
        technical_details = technical_details[:300] + "..."
    prompt += f"- Technical Details: {technical_details}\n"
    prompt += f"- Posts: {len(data.get('posts', []))} entries (first few for reference):\n"
    for i, post in enumerate(data.get('posts', [])[:3]):
        content = post.get('content', 'N/A')
        if len(content) > 150:
            content = content[:150] + "..."
        prompt += f"  - Post {i+1} ({post.get('timestamp', 'N/A')} by {post.get('author', 'N/A')}): {content}\n"
    prompt += f"- Notes: {len(data.get('notes', []))} entries (first few for reference):\n"
    for i, note in enumerate(data.get('notes', [])[:3]):
        content = note.get('content', 'N/A')
        if len(content) > 150:
            content = content[:150] + "..."
        prompt += f"  - Note {i+1} ({note.get('timestamp', 'N/A')} by {note.get('author', 'N/A')}): {content}\n"
    prompt += f"""

Specific Questions to Answer:
{questions_str}

Provide the response as a valid JSON object with keys:
- `ticket_id`: The ticket ID.
- `category`: The ticket category.
- `detailed_metrics`: Object with timeline, scope, outcome, revisit info, and metrics.
- `answers`: Object mapping each question to its detailed answer with evidence cited.

Output ONLY valid JSON.
"""
    return prompt

def consolidate_final_report(batch_results, issues_index, detailed_results, ticket_data, chain_hash):
    """Consolidate all analysis results into a final structured report."""
    report = {
        "chain_hash": chain_hash,
        "timestamp": datetime.now().isoformat(),
        "tickets_analyzed": [],
        "metrics_summary": {},
        "issues_summary": issues_index,
        "detailed_analyses": []
    }

    # Compile ticket summaries from batch results
    ticket_summaries = {}
    for batch in batch_results:
        if "result" in batch and isinstance(batch["result"], list):
            for ticket_analysis in batch["result"]:
                ticket_id = str(ticket_analysis.get("TicketID", ticket_analysis.get("ticket_id", "")))
                if ticket_id:
                    ticket_summaries[ticket_id] = ticket_analysis
                    report["tickets_analyzed"].append(ticket_id)

    # Add detailed analyses
    for detail in detailed_results:
        ticket_id = detail["ticket_id"]
        if "result" in detail and detail["result"].get("ticket_id"):
            report["detailed_analyses"].append(detail["result"])

    # Summarize key metrics across tickets
    total_tickets = len(report["tickets_analyzed"])
    dispatch_count = sum(1 for tid in ticket_summaries if ticket_data.get(tid, {}).get("category") == "Dispatch Tickets")
    turnup_count = sum(1 for tid in ticket_summaries if ticket_data.get(tid, {}).get("category") == "Turnup Tickets")
    revisit_count = sum(1 for tid in ticket_summaries if ticket_summaries[tid].get("Revisit", {}).get("Required", False))
    issue_count = len(issues_index.get("tickets_with_issues", []))

    report["metrics_summary"] = {
        "total_tickets": total_tickets,
        "dispatch_tickets": dispatch_count,
        "turnup_tickets": turnup_count,
        "revisits_required": revisit_count,
        "tickets_with_issues": issue_count
    }

    return report


# --- Main Orchestration ---
def analyze_real_ticket(ticket_id):
    """Analyze a real ticket from the database, coordinating Phase 1 & 2."""
    session = None
    try:
        session = get_db_session("primary")
        if not session: return
        logging.info(f"Retrieving ticket chain for ticket ID: {ticket_id}")
        if TicketChainService is None:
            logging.error("TicketChainService is not available. Cannot retrieve ticket chain details.")
            print("Error: Ticket chain analysis cannot proceed because required services are not available.")
            return
        chain_details = TicketChainService.get_chain_details_by_ticket_id(session, ticket_id)

        chain_details_display = display_ticket_details(chain_details)
        if not chain_details_display or "error" in chain_details:
            logging.error("Failed to retrieve or display valid chain details. Aborting.")
            return

        summary_file, phase1_analysis_text = run_phase_1_report(chain_details)
        if "Error:" in (phase1_analysis_text or ""):
             logging.error("Phase 1 analysis failed. Cannot proceed to Phase 2.")
             return

        try:
             run_phase2 = input("\nDo you want to run Phase 2 analysis (detailed multi-stage)? (y/n): ").strip().lower()
             if run_phase2 == 'y': run_phase_2_analysis(chain_details, phase1_analysis_text)
             else: logging.info("Skipping Phase 2 analysis.")
        except (EOFError, RuntimeError) as e:
             logging.warning(f"Could not get user input for Phase 2 ({e}). Skipping Phase 2.")

    except Exception as e:
        logging.error(f"An error occurred in analyze_real_ticket: {e}", exc_info=True)
    finally:
        if session:
            try: session.close(); logging.info("Primary database session closed by analyze_real_ticket.")
            except Exception as db_close_err: logging.error(f"Error closing primary DB session: {db_close_err}")


# --- Test Functions (Marked as Needs Review) ---
def test_with_mock_data():
    """Test the analysis with mock data"""
    logging.warning("test_with_mock_data needs review for compatibility with multi-stage Phase 2.")
    # Placeholder logic...

def test_with_real_tickets():
    """Test the analysis with real ticket IDs for more realistic testing"""
    logging.warning("test_with_real_tickets needs review for compatibility with multi-stage Phase 2.")
    # Placeholder logic...

# --- Main Entry Point ---
def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Advanced ticket chain analysis (Phase 1 & 2)")
    parser.add_argument("--ticket", type=str, help="Ticket ID to analyze")
    parser.add_argument("--test", action="store_true", help="Run with test data (needs review)")
    parser.add_argument("--test-real", action="store_true", help="Run test with real ticket IDs (needs review)")
    parser.add_argument("--assistant-id", type=str, help="Override Assistant ID from .env/config")
    parser.add_argument("--vector-store-id", type=str, help="Override Vector Store ID from .env/config")

    args = parser.parse_args()

    if args.assistant_id:
        logging.warning(f"Overriding Assistant ID with provided: {args.assistant_id}")
        os.environ['ASSISTANT_ID'] = args.assistant_id
        global ASSISTANT_ID; ASSISTANT_ID = args.assistant_id
    if args.vector_store_id:
         logging.warning(f"Overriding Vector Store ID with provided: {args.vector_store_id}")
         os.environ['VECTOR_STORE_ID'] = args.vector_store_id
         global VECTOR_STORE_ID; VECTOR_STORE_ID = args.vector_store_id

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