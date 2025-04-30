import argparse
import json
import logging
import sys
import os
import re
import time
import shutil
from datetime import datetime, timedelta
from openai import OpenAI
from sqlalchemy import text
from app.services.ticket_chain_service import TicketChainService
from app.services.ai_service import AIService, openai_client

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Add parent directory to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Custom JSON encoder to handle datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, timedelta):
            # Convert timedelta to total seconds for json serialization
            return f"{obj.total_seconds()} seconds"
        elif hasattr(obj, 'isoformat'):
            # Handle any other date/time-like objects
            return obj.isoformat()
        # Let the base class handle anything else
        return super().default(obj)

def sanitize_json_string(json_str):
    """
    Sanitize JSON string for parsing.
    
    This function attempts to clean up malformed JSON by:
    1. Removing control characters
    2. Escaping quotes and slashes
    3. Handling truncated strings
    4. Extracting valid JSON within malformed strings
    """
    if not json_str:
        return "[]"
    
    try:
        # Save the original string for debugging
        debug_dir = 'PyChain/data/debug'
        os.makedirs(debug_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        debug_file = f"{debug_dir}/sanitize_json_input_{timestamp}.txt"
        try:
            with open(debug_file, 'w', encoding='utf-8', errors='ignore') as f:
                f.write(str(json_str))
            logging.info(f"Saved original JSON string to {debug_file}")
        except Exception as e:
            logging.error(f"Error saving JSON debug file: {str(e)}")
            
        # First replace null bytes which often cause problems
        cleaned = json_str.replace('\0', '').replace('\u0000', '')
        
        # Replace problematic control characters
        cleaned = re.sub(r'[\x00-\x1F\x7F]', '', cleaned)
        
        # Ensure the JSON string has proper opening/closing brackets
        if not cleaned.strip().startswith('['):
            cleaned = '[' + cleaned
        if not cleaned.strip().endswith(']'):
            cleaned = cleaned + ']'
        
        # Try to parse the cleaned string
        try:
            json.loads(cleaned)
            return cleaned
        except json.JSONDecodeError:
            # If that fails, try more aggressive cleaning
            pass
        
        # For more aggressive cleaning, look for JSON-like structures
        matches = re.findall(r'\{[^{}]*\}', cleaned)
        if matches:
            reconstructed = '[' + ','.join(matches) + ']'
            try:
                json.loads(reconstructed)  # Validate it's proper JSON
                return reconstructed
            except json.JSONDecodeError:
                logging.warning("Failed to reconstruct valid JSON with regex matches")
        
        # Last resort: Remove problematic characters and try again
        final_attempt = re.sub(r'[^\x20-\x7E]', '', cleaned)  # Keep only printable ASCII
        
        # Fix unescaped quotes and control characters in strings
        final_attempt = re.sub(r'(?<!\\)"(?=(.*?".*?"))', r'\"', final_attempt)
        
        # Fix truncated objects/arrays
        if final_attempt.count('{') > final_attempt.count('}'):
            final_attempt += '}' * (final_attempt.count('{') - final_attempt.count('}'))
        if final_attempt.count('[') > final_attempt.count(']'):
            final_attempt += ']' * (final_attempt.count('[') - final_attempt.count(']'))
            
        # Save the final sanitized string
        debug_file_out = f"{debug_dir}/sanitize_json_output_{timestamp}.txt"
        with open(debug_file_out, 'w', encoding='utf-8') as f:
            f.write(final_attempt)
        logging.info(f"Saved sanitized JSON string to {debug_file_out}")
        
        return final_attempt
    except Exception as e:
        logging.error(f"Error in sanitize_json_string: {str(e)}")
        import traceback
        logging.error(f"Sanitization error traceback: {traceback.format_exc()}")
        return "[]"  # Return empty array as a safe fallback

def get_db_session(db_type="primary"):
    """Create a database session with connection pooling and retry logic."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.exc import OperationalError
    import time

    max_retries = 3
    retry_delay = 2

    if db_type == "primary":
        return TicketChainService.get_db_session("primary")
    elif db_type == "cissdm":
        config = {
            "host": os.environ.get("CISSDM_DB_HOST", "localhost"),
            "user": os.environ.get("CISSDM_DB_USER", "root"),
            "password": os.environ.get("CISSDM_DB_PASSWORD", ""),
            "database": os.environ.get("CISSDM_DB_NAME", ""),
            "port": os.environ.get("CISSDM_DB_PORT", "3306")
        }
        connection_string = f"mysql+mysqlconnector://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
        for attempt in range(max_retries):
            try:
                engine = create_engine(connection_string, connect_args={'connect_timeout': 10}, pool_size=5, max_overflow=10)
                Session = sessionmaker(bind=engine)
                session = Session()
                logging.info("CISSDM database session created")
                return session
            except OperationalError as e:
                logging.warning(f"Failed to create CISSDM session, attempt {attempt + 1}/{max_retries}: {e}")
                if attempt == max_retries - 1:
                    logging.error("Max retries reached for CISSDM session")
                    return None
                time.sleep(retry_delay * (2 ** attempt))
        return None
    else:
        logging.error(f"Invalid database type: {db_type}")
        return None

def debug_query_results(results, query_name, identifier="debug"):
    """Save raw query results to file for debugging."""
    debug_dir = 'PyChain/data/debug'
    os.makedirs(debug_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{debug_dir}/{query_name}_{identifier}_{timestamp}.txt"
    
    try:
        with open(filename, 'w', encoding='utf-8', errors='ignore') as f:
            f.write(f"==== DEBUG: {query_name} Query Results ====\n\n")
            try:
                f.write(str(results))
            except Exception as e:
                f.write(f"<Error writing results: {str(e)}>")
        
        logging.info(f"Saved query debug data to {filename}")
        return filename
    except Exception as e:
        logging.error(f"Failed to save query debug data: {e}")
        return None

def fetch_full_ticket_data(session, cissdm_session, ticket_ids):
    """
    Fetch complete ticket data including posts, notes, and related data
    
    Args:
        session: Primary database session
        cissdm_session: CISSDM database session
        ticket_ids: List of ticket IDs to fetch data for
        
    Returns:
        Dictionary mapping ticket IDs to their detailed data
    """
    if not session:
        logging.error("No database session available")
        return {}
    
    if not ticket_ids:
        logging.error("No ticket IDs provided")
        return {}
    
    logging.info(f"Fetching detailed data for {len(ticket_ids)} tickets")
    
    # Convert all ticket IDs to strings for consistent handling
    ticket_ids_str = [str(tid) for tid in ticket_ids]
    
    # Create placeholders for SQL query
    id_placeholders = ", ".join([f":{idx}" for idx in range(len(ticket_ids_str))])
    id_params = {str(idx): tid for idx, tid in enumerate(ticket_ids_str)}
    
    # Query for all ticket data including posts and notes
    query_text = f"""
        SELECT 
            t.ticketid, 
            t.departmenttitle AS department,
            t.subject,
            t.ticketstatustitle AS status,
            UNIX_TIMESTAMP(t.dateline) AS created_time,
            FROM_UNIXTIME(t.dateline) AS created_date,
            t.fullname AS creator_name,
            t.email AS creator_email,
            CASE WHEN t.resolutiondateline > 0 THEN FROM_UNIXTIME(t.resolutiondateline) ELSE NULL END AS closed_date,
            CASE 
                WHEN t.departmenttitle IN ('FST Accounting', 'Dispatch', 'Pro Services') THEN 'Dispatch Tickets'
                WHEN t.departmenttitle = 'Turnups' THEN 'Turnup Tickets'
                WHEN t.departmenttitle IN ('Shipping', 'Outbound', 'Inbound') THEN 'Shipping Tickets'
                WHEN t.departmenttitle = 'Turn up Projects' THEN 'Project Management'
                ELSE 'Other'
            END AS category,
            (
                SELECT CAST(CONCAT('[', GROUP_CONCAT(
                    CONCAT(
                        '{{',
                        '"ticketpostid":"', tp.ticketpostid, '",',
                        '"post_dateline":"', FROM_UNIXTIME(tp.dateline), '",',
                        '"fullname":"', COALESCE(REPLACE(tp.fullname, '"', '\\"'), ''), '",',
                        '"contents":"', COALESCE(REPLACE(REPLACE(tp.contents, '\\\\', '\\\\\\\\'), '"', '\\"'), ''), '",',
                        '"isprivate":"', tp.isprivate, '",',
                        '"creator":"', tp.creator, '"',
                        '}}'
                    )
                    ORDER BY tp.dateline
                    SEPARATOR ','), ']') AS CHAR)
                FROM sw_ticketposts tp
                WHERE tp.ticketid = t.ticketid
            ) AS posts_json,
            (
                SELECT CAST(CONCAT('[', GROUP_CONCAT(
                    CONCAT(
                        '{{',
                        '"ticketnoteid":"', tn.ticketnoteid, '",',
                        '"note_staffid":"', tn.staffid, '",',
                        '"note_dateline":"', FROM_UNIXTIME(tn.dateline), '",',
                        '"staffname":"', COALESCE(REPLACE(tn.staffname, '"', '\\"'), ''), '",',
                        '"note":"', COALESCE(REPLACE(REPLACE(tn.note, '\\\\', '\\\\\\\\'), '"', '\\"'), ''), '"',
                        '}}'
                    )
                    ORDER BY tn.dateline
                    SEPARATOR ','), ']') AS CHAR)
                FROM sw_ticketnotes tn
                WHERE tn.linktypeid = t.ticketid
            ) AS notes_json,
            (SELECT fieldvalue FROM sw_customfieldvalues WHERE customfieldid = 117 AND typeid = t.ticketid) AS site_number,
            (SELECT fieldvalue FROM sw_customfieldvalues WHERE customfieldid = 104 AND typeid = t.ticketid) AS customer,
            (SELECT fieldvalue FROM sw_customfieldvalues WHERE customfieldid = 122 AND typeid = t.ticketid) AS state,
            (SELECT fieldvalue FROM sw_customfieldvalues WHERE customfieldid = 121 AND typeid = t.ticketid) AS city,
            (SELECT fieldvalue FROM sw_customfieldvalues WHERE customfieldid = 123 AND typeid = t.ticketid) AS street,
            FROM_UNIXTIME(t.duedate) AS service_date,
            (SELECT fieldvalue FROM sw_customfieldvalues WHERE customfieldid = 248 AND typeid = t.ticketid) AS project_id
        FROM sw_tickets t
        WHERE t.ticketid IN ({id_placeholders})
    """
    query = text(query_text)
    
    logging.info(f"Executing ticket data query for {len(ticket_ids_str)} tickets")
    results = session.execute(query, id_params).fetchall()
    logging.info(f"Retrieved detailed data for {len(results)} tickets")
    
    # Save raw query results for debugging
    debug_query_results(results, "full_ticket_data", f"{len(ticket_ids_str)}_tickets")
    
    # Process results into a structured format
    ticket_data = {}
    for row in results:
        tid_str = str(row.ticketid)
        
        # Save raw row data for this ticket for debugging
        debug_query_results(row, "ticket_row", tid_str)
        
        # Create a data structure for the ticket
        ticket_data[tid_str] = {
            'ticketid': tid_str,
            'department': row.department,
            'subject': row.subject,
            'status': row.status,
            'created_time': row.created_time,
            'created_date': row.created_date,
            'creator_name': row.creator_name,
            'creator_email': row.creator_email,
            'closed_date': row.closed_date,
            'category': row.category,
            'site_number': row.site_number,
            'customer': row.customer,
            'state': row.state,
            'city': row.city,
            'street': row.street,
            'service_date': row.service_date,
            'project_id': row.project_id,
            'posts': [],
            'notes': []
        }
        
        # Parse and add posts
        posts_json = row.posts_json
        if posts_json:
            try:
                # Save raw posts JSON for debugging
                debug_query_results(posts_json, "posts_json_raw", tid_str)
                
                # Sanitize and parse the JSON
                sanitized_posts_json = sanitize_json_string(posts_json)
                
                # Save sanitized posts JSON for debugging
                debug_query_results(sanitized_posts_json, "posts_json_sanitized", tid_str)
                
                posts = json.loads(sanitized_posts_json)
                
                if posts:
                    processed_posts = []
                    for post in posts:
                        processed_post = {
                            'id': post.get('ticketpostid', ''),
                            'time': post.get('post_dateline', ''),
                            'user': post.get('fullname', ''),
                            'contents': post.get('contents', ''),
                            'private': post.get('isprivate', '0'),
                            'creator': post.get('creator', '')
                        }
                        processed_posts.append(processed_post)
                    ticket_data[tid_str]['posts'] = processed_posts
                    logging.info(f"Processed {len(processed_posts)} posts for ticket {tid_str}")
                else:
                    logging.warning(f"No posts found in parsed JSON for ticket {tid_str}")
            except json.JSONDecodeError as e:
                logging.error(f"JSON parse error for posts in ticket {tid_str}: {e}")
                logging.error(f"Posts JSON: {posts_json[:200]}...")
            except Exception as e:
                logging.error(f"Error processing posts for ticket {tid_str}: {e}")
                import traceback
                logging.error(f"Posts processing traceback: {traceback.format_exc()}")
        
        # Parse and add notes
        notes_json = row.notes_json
        if notes_json:
            try:
                # Save raw notes JSON for debugging
                debug_query_results(notes_json, "notes_json_raw", tid_str)
                
                # Sanitize and parse the JSON
                sanitized_notes_json = sanitize_json_string(notes_json)
                
                # Save sanitized notes JSON for debugging
                debug_query_results(sanitized_notes_json, "notes_json_sanitized", tid_str)
                
                notes = json.loads(sanitized_notes_json)
                
                if notes:
                    processed_notes = []
                    for note in notes:
                        processed_note = {
                            'id': note.get('ticketnoteid', ''),
                            'time': note.get('note_dateline', ''),
                            'user': note.get('staffname', ''),
                            'contents': note.get('note', '')
                        }
                        processed_notes.append(processed_note)
                    ticket_data[tid_str]['notes'] = processed_notes
                    logging.info(f"Processed {len(processed_notes)} notes for ticket {tid_str}")
                else:
                    logging.warning(f"No notes found in parsed JSON for ticket {tid_str}")
            except json.JSONDecodeError as e:
                logging.error(f"JSON parse error for notes in ticket {tid_str}: {e}")
                logging.error(f"Notes JSON: {notes_json[:200]}...")
            except Exception as e:
                logging.error(f"Error processing notes for ticket {tid_str}: {e}")
                import traceback
                logging.error(f"Notes processing traceback: {traceback.format_exc()}")
    
    # Process additional CISSDM data if available
    if cissdm_session:
        try:
            # Add CISSDM dispatch - turnup relationships
            for tid_str in ticket_data:
                if ticket_data[tid_str]['category'] == 'Turnup Tickets':
                    # Check if this is a turnup ticket related to a dispatch
                    cissdm_query = text("""
                        SELECT 
                            d.TicketID AS dispatch_id,
                            d.LineItemID,
                            t.TemplateName
                        FROM TurnupTask t
                        JOIN Dispatch d ON t.DispatchID = d.ID
                        WHERE t.TicketID = :ticket_id
                        LIMIT 1
                    """)
                    try:
                        cissdm_result = cissdm_session.execute(cissdm_query, {"ticket_id": tid_str}).first()
                        if cissdm_result:
                            ticket_data[tid_str]['parent_dispatch_id'] = cissdm_result.dispatch_id
                            ticket_data[tid_str]['line_item_id'] = cissdm_result.LineItemID
                            ticket_data[tid_str]['turnup_template'] = cissdm_result.TemplateName
                            logging.info(f"Linked turnup {tid_str} to dispatch {cissdm_result.dispatch_id}")
                            
                            # Save debug information
                            debug_query_results(cissdm_result, "cissdm_turnup_relation", tid_str)
                    except Exception as e:
                        logging.error(f"Error querying CISSDM turnup relation for {tid_str}: {e}")
                
                # Add turnup data for all tickets
                turnup_query = text("""
                    SELECT 
                        tt.ID AS turnup_id,
                        tt.TicketID,
                        tt.TemplateName,
                        tt.DispatchID,
                        tt.Status,
                        tt.CreatedDate,
                        tt.DateComplete,
                        tt.CompletingUserName,
                        tt.Quantity
                    FROM TurnupTask tt
                    WHERE tt.TicketID = :ticket_id
                """)
                try:
                    turnup_results = cissdm_session.execute(turnup_query, {"ticket_id": tid_str}).fetchall()
                    if turnup_results:
                        turnup_list = []
                        for tr in turnup_results:
                            turnup = {
                                'id': tr.turnup_id,
                                'template': tr.TemplateName,
                                'dispatch_id': tr.DispatchID,
                                'status': tr.Status,
                                'created_date': tr.CreatedDate,
                                'completed_date': tr.DateComplete,
                                'completed_by': tr.CompletingUserName,
                                'quantity': tr.Quantity
                            }
                            turnup_list.append(turnup)
                        ticket_data[tid_str]['turnup_data'] = turnup_list
                        logging.info(f"Added {len(turnup_list)} turnup tasks for ticket {tid_str}")
                        
                        # Save debug information
                        debug_query_results(turnup_results, "cissdm_turnup_data", tid_str)
                except Exception as e:
                    logging.error(f"Error querying CISSDM turnup data for {tid_str}: {e}")
        except Exception as e:
            logging.error(f"Error processing CISSDM data: {e}")
    
    # Log summary of data retrieval
    categories = {}
    for tid_str, data in ticket_data.items():
        cat = data['category']
        categories[cat] = categories.get(cat, 0) + 1
        if data.get('parent_dispatch_id'):
            logging.info(f"  - {tid_str} ({cat}): Related to dispatch {data['parent_dispatch_id']}")
        else:
            logging.info(f"  - {tid_str} ({cat})")
        
        if 'posts' in data:
            logging.info(f"    - Posts: {len(data['posts'])}")
        if 'notes' in data:
            logging.info(f"    - Notes: {len(data['notes'])}")
        if 'turnup_data' in data:
            logging.info(f"  - Has turnup data: Yes")
        if data.get('parent_dispatch_id'):
            logging.info(f"  - Parent dispatch: {data.get('parent_dispatch_id')}")

    logging.info(f"Fetched details for {len(ticket_data)} tickets")
    return ticket_data

def setup_vector_store_and_assistant(client: OpenAI, ticket_files: list[str]):
    """Set up or reuse vector store and assistant, updating .env if needed."""
    try:
        # DEBUG: Check API key configuration
        api_key = os.getenv("OPENAI_API_KEY", "")
        if not api_key:
            logging.error("DEBUG: OPENAI_API_KEY environment variable is not set")
        else:
            masked_key = api_key[:4] + "..." + api_key[-4:] if len(api_key) > 8 else "***"
            logging.info(f"DEBUG: Using OpenAI API key starting with {masked_key}")
            
        # DEBUG: Log client configuration
        logging.info(f"DEBUG: OpenAI client configuration - API base: {client.base_url if hasattr(client, 'base_url') else 'default'}")
        
        vector_store_id = os.getenv("VECTOR_STORE_ID")
        assistant_id = os.getenv("ASSISTANT_ID")
        vector_store_created = False
        assistant_created = False

        # DEBUG: Log existing IDs
        logging.info(f"DEBUG: Existing vector_store_id: {vector_store_id or 'None'}")
        logging.info(f"DEBUG: Existing assistant_id: {assistant_id or 'None'}")

        # Vector Store Handling
        if not vector_store_id:
            logging.info("No VECTOR_STORE_ID found, creating new vector store...")
            try:
                # DEBUG: Log vector store creation attempt
                logging.info("DEBUG: Attempting to create new vector store")
                vector_store = client.vector_stores.create(
                    name=f"Ticket Analysis Store - {datetime.now():%Y%m%d-%H%M%S}"
                )
                logging.info(f"DEBUG: Vector store creation response: {vector_store}")
            except AttributeError as ae:
                logging.info(f"DEBUG: AttributeError in vector store creation: {ae}")
                try:
                    vector_store = client.beta.vector_stores.create(
                        name=f"Ticket Analysis Store - {datetime.now():%Y%m%d-%H%M%S}"
                    )
                    logging.info(f"DEBUG: Beta vector store creation response: {vector_store}")
                except Exception as e:
                    logging.error(f"DEBUG: Failed to create vector store using beta endpoint: {e}")
                    raise
            except Exception as e:
                logging.error(f"DEBUG: Failed to create vector store: {e}")
                raise
            
            vector_store_id = vector_store.id
            vector_store_created = True
            logging.info(f"Created vector store with ID: {vector_store_id}")
        else:
            logging.info(f"Using existing vector store ID: {vector_store_id}")
            try:
                # DEBUG: Log vector store verification attempt
                logging.info(f"DEBUG: Verifying existing vector store: {vector_store_id}")
                vs_info = client.vector_stores.retrieve(vector_store_id)
                logging.info(f"DEBUG: Vector store verification response: {vs_info}")
                logging.info(f"Vector store {vector_store_id} verified.")
            except Exception as e:
                logging.error(f"Failed to verify vector store {vector_store_id}: {e}")
                logging.info("Creating new vector store since verification failed...")
                try:
                    vector_store = client.vector_stores.create(
                        name=f"Ticket Analysis Store - {datetime.now():%Y%m%d-%H%M%S}"
                    )
                    vector_store_id = vector_store.id
                    vector_store_created = True
                    logging.info(f"Created new vector store with ID: {vector_store_id}")
                except Exception as new_e:
                    logging.error(f"Failed to create replacement vector store: {new_e}")
                    raise new_e

        # Assistant Handling
        if not assistant_id:
            logging.info("No ASSISTANT_ID found, creating new assistant...")
            try:
                # DEBUG: Log assistant creation attempt
                logging.info("DEBUG: Attempting to create new assistant")
                assistant = client.assistants.create(
                    name="Ticket Analysis Assistant",
                    instructions="You are an expert in analyzing ticket chains for field service operations. Analyze ticket data from uploaded files, extracting detailed metrics (e.g., timeline, scope, outcome, revisits, delays) and issues. Provide structured JSON responses, citing specific ticket data (e.g., posts, notes) as evidence. Do not assume data beyond what's provided.",
                    model="gpt-4o",
                    tools=[{"type": "file_search"}],
                    tool_resources={"file_search": {"vector_store_ids": [vector_store_id]}}
                )
                logging.info(f"DEBUG: Assistant creation response: {assistant}")
            except AttributeError as ae:
                logging.info(f"DEBUG: AttributeError in assistant creation: {ae}")
                try:
                    assistant = client.beta.assistants.create(
                        name="Ticket Analysis Assistant",
                        instructions="You are an expert in analyzing ticket chains for field service operations. Analyze ticket data from uploaded files, extracting detailed metrics (e.g., timeline, scope, outcome, revisits, delays) and issues. Provide structured JSON responses, citing specific ticket data (e.g., posts, notes) as evidence. Do not assume data beyond what's provided.",
                        model="gpt-4o",
                        tools=[{"type": "file_search"}],
                        tool_resources={"file_search": {"vector_store_ids": [vector_store_id]}}
                    )
                    logging.info(f"DEBUG: Beta assistant creation response: {assistant}")
                except Exception as e:
                    logging.error(f"DEBUG: Failed to create assistant using beta endpoint: {e}")
                    raise
            except Exception as e:
                logging.error(f"DEBUG: Failed to create assistant: {e}")
                raise
                
            assistant_id = assistant.id
            assistant_created = True
            logging.info(f"Created assistant with ID: {assistant_id}")
        else:
            logging.info(f"Using existing assistant ID: {assistant_id}")
            try:
                # DEBUG: Log assistant update attempt
                logging.info(f"DEBUG: Updating existing assistant: {assistant_id} with vector store: {vector_store_id}")
                try:
                    update_response = client.assistants.update(
                        assistant_id=assistant_id,
                        tool_resources={"file_search": {"vector_store_ids": [vector_store_id]}}
                    )
                    logging.info(f"DEBUG: Assistant update response: {update_response}")
                except AttributeError as ae:
                    logging.info(f"DEBUG: AttributeError in assistant update: {ae}")
                    update_response = client.beta.assistants.update(
                        assistant_id=assistant_id,
                        tool_resources={"file_search": {"vector_store_ids": [vector_store_id]}}
                    )
                    logging.info(f"DEBUG: Beta assistant update response: {update_response}")
                logging.info(f"Updated assistant {assistant_id} with vector store {vector_store_id}")
            except Exception as e:
                logging.error(f"DEBUG: Failed to update assistant: {e}")
                logging.info("Creating new assistant since update failed...")
                try:
                    assistant = client.assistants.create(
                        name="Ticket Analysis Assistant",
                        instructions="You are an expert in analyzing ticket chains for field service operations. Analyze ticket data from uploaded files, extracting detailed metrics (e.g., timeline, scope, outcome, revisits, delays) and issues. Provide structured JSON responses, citing specific ticket data (e.g., posts, notes) as evidence. Do not assume data beyond what's provided.",
                        model="gpt-4o",
                        tools=[{"type": "file_search"}],
                        tool_resources={"file_search": {"vector_store_ids": [vector_store_id]}}
                    )
                    assistant_id = assistant.id
                    assistant_created = True
                    logging.info(f"Created new assistant with ID: {assistant_id}")
                except Exception as new_e:
                    logging.error(f"Failed to create replacement assistant: {new_e}")
                    raise new_e

        # Update .env file if new IDs were created
        if vector_store_created or assistant_created:
            try:
                env_file = ".env"
                # DEBUG: Log .env file update
                logging.info(f"DEBUG: Updating .env file with new IDs. Vector store created: {vector_store_created}, Assistant created: {assistant_created}")
                
                lines = []
                if os.path.exists(env_file):
                    with open(env_file, "r") as f:
                        lines = f.readlines()
                    logging.info(f"DEBUG: Read {len(lines)} lines from existing .env file")
                else:
                    logging.info("DEBUG: No existing .env file found. Creating new file.")
                    
                new_lines = {}
                if assistant_id:
                    new_lines["ASSISTANT_ID"] = f"ASSISTANT_ID={assistant_id}\n"
                if vector_store_id:
                    new_lines["VECTOR_STORE_ID"] = f"VECTOR_STORE_ID={vector_store_id}\n"
                
                updated_lines = []
                keys_updated = set()
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
                
                for key, new_line in new_lines.items():
                    if key not in keys_updated:
                        updated_lines.append(new_line)
                
                with open(env_file, "w") as f:
                    f.writelines(updated_lines)
                    
                # Update environment variables in current process
                os.environ["VECTOR_STORE_ID"] = vector_store_id
                if assistant_id:
                    os.environ["ASSISTANT_ID"] = assistant_id
                    
                logging.info("Updated .env file with ASSISTANT_ID and/or VECTOR_STORE_ID")
                logging.info(f"DEBUG: Updated .env file now has {len(updated_lines)} lines")
            except Exception as e:
                logging.error(f"Error updating .env file: {e}")
                import traceback
                logging.error(f"DEBUG: .env update error traceback: {traceback.format_exc()}")

        return vector_store_id, assistant_id
    except Exception as e:
        logging.error(f"Failed to set up vector store or assistant: {e}")
        import traceback
        logging.error(f"DEBUG: Setup error traceback: {traceback.format_exc()}")
        
        if "invalid_api_key" in str(e).lower():
            logging.error("Invalid OpenAI API key. Please verify OPENAI_API_KEY in your .env file.")
        raise

def wait_for_vector_store_processing(client, vector_store_id, file_ids, timeout=300):
    """Wait for files to be processed by the vector store."""
    start_time = time.time()
    processed_files = set()
    all_files = set(file_ids)
    
    # DEBUG: Log initial state
    logging.info(f"DEBUG: Starting vector store processing wait. Files to process: {len(all_files)}")
    logging.info(f"DEBUG: Files IDs: {file_ids}")
    
    while time.time() - start_time < timeout:
        if len(all_files) == 0:
            logging.info("No files to process")
            return True
            
        all_processed = True
        # DEBUG: Log current check cycle
        elapsed = int(time.time() - start_time)
        logging.info(f"DEBUG: Checking file processing status at {elapsed}s. Remaining: {len(all_files - processed_files)}")
        
        for file_id in list(all_files - processed_files):
            try:
                # DEBUG: Log individual file check
                logging.info(f"DEBUG: Checking status of file {file_id}")
                
                try:
                    file_status = client.vector_stores.files.retrieve(vector_store_id=vector_store_id, file_id=file_id)
                    logging.info(f"DEBUG: File {file_id} status: {file_status.status}")
                except AttributeError:
                    logging.info(f"DEBUG: Using beta endpoint for file status check")
                    file_status = client.beta.vector_stores.files.retrieve(vector_store_id=vector_store_id, file_id=file_id)
                    logging.info(f"DEBUG: File {file_id} status: {file_status.status}")
                
                if file_status.status == 'completed':
                    processed_files.add(file_id)
                    logging.info(f"DEBUG: File {file_id} processing completed!")
                elif file_status.status in ['failed', 'cancelled']:
                    logging.error(f"File {file_id} failed: {file_status.status}")
                    logging.error(f"DEBUG: Full file status object: {file_status}")
                    all_files.remove(file_id)
                else:
                    logging.info(f"DEBUG: File {file_id} still processing. Current status: {file_status.status}")
                    all_processed = False
            except Exception as e:
                logging.error(f"Error checking file {file_id}: {e}")
                import traceback
                logging.error(f"DEBUG: Error traceback for file {file_id}: {traceback.format_exc()}")
                all_processed = False
                
        if all_processed:
            logging.info("All files processed")
            logging.info(f"DEBUG: Processing completed in {elapsed}s")
            return True
            
        logging.info(f"Waiting for {len(all_files - processed_files)} files... ({int(time.time() - start_time)}s)")
        time.sleep(5)
        
    logging.error(f"Timeout after {timeout}s. Unprocessed files: {all_files - processed_files}")
    logging.error(f"DEBUG: Processing timed out. Processed {len(processed_files)} of {len(file_ids)} files.")
    return False

def save_debug_data(prefix, data, ticket_id, content_type="json"):
    """Save debug data to a file for analysis."""
    debug_dir = 'PyChain/data/debug'
    os.makedirs(debug_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{debug_dir}/{prefix}_{ticket_id}_{timestamp}.{content_type}"
    
    try:
        if content_type == "json":
            with open(filename, 'w', encoding='utf-8') as f:
                if isinstance(data, str):
                    f.write(data)
                else:
                    json.dump(data, f, indent=2, ensure_ascii=False, cls=DateTimeEncoder)
        else:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(str(data))
        logging.info(f"Saved debug data to {filename}")
        return filename
    except Exception as e:
        logging.error(f"Failed to save debug data: {e}")
        return None

def create_ticket_files(chain_details, full_ticket_data, phase1_analysis_text):
    """Create JSON files for ticket data, chain metadata, and analysis with standardized structure for AI ingestion."""
    output_dir = 'PyChain/data/ticket_files'
    os.makedirs(output_dir, exist_ok=True)
    
    file_paths = []
    chain_hash = chain_details['chain_hash']
    chain_file = os.path.join(output_dir, f'chain_{chain_hash}.json')
    tickets_file = os.path.join(output_dir, f'tickets_{chain_hash}.json')
    
    # Save raw data for debugging
    debug_raw_path = save_debug_data("raw_chain", chain_details, chain_hash)
    debug_raw_tickets = save_debug_data("raw_tickets", full_ticket_data, chain_hash)
    
    # Chain metadata
    try:
        with open(chain_file, 'w') as f:
            json.dump(chain_details, f, indent=2, cls=DateTimeEncoder)
        file_paths.append(chain_file)
        logging.info(f"Created chain details file: {chain_file}")
    except Exception as e:
        logging.error(f"Error creating chain file {chain_file}: {e}")
    
    # Tickets with standardized structure
    try:
        standardized_ticket_data = {}
        tickets_with_content = 0
        
        for ticket in chain_details.get('tickets', []):
            ticket_id = str(ticket.get('ticket_id', 'unknown'))
            
            # Check if this ticket has content in the full_ticket_data
            if ticket_id not in full_ticket_data:
                logging.warning(f"No detailed data found for ticket {ticket_id}")
                continue
            
            data = full_ticket_data[ticket_id]
            
            # Save raw data for this specific ticket for debugging
            save_debug_data("raw_ticket_data", data, ticket_id)
            
            # Clean post content with improved sanitization
            clean_posts = []
            has_content = False
            
            # Save raw posts for debugging
            if 'posts' in data:
                save_debug_data("raw_posts", data['posts'], ticket_id)
                
            post_count = len(data.get('posts', []))
            note_count = len(data.get('notes', []))
            logging.info(f"DEBUG: Ticket {ticket_id} has {post_count} posts and {note_count} notes")
            
            # Clean and format posts for standardized output
            clean_posts = []
            for post in data.get('posts', []):
                try:
                    content = post.get('contents', '')
                    if content:
                        # Save raw post content
                        save_debug_data(f"raw_post_content_{post.get('id', 'unknown')}", content, ticket_id, "txt")
                        
                        # Ensure the content is properly sanitized and not empty
                        sanitized_content = sanitize_content_string(content)
                        
                        # Save sanitized content
                        save_debug_data(f"sanitized_post_content_{post.get('id', 'unknown')}", sanitized_content, ticket_id, "txt")
                        
                        if sanitized_content:
                            clean_post = {
                                "id": str(post.get('id', '')),
                                "timestamp": post.get('time', ''),
                                "author": post.get('user', ''),
                                "content": sanitized_content
                            }
                            clean_posts.append(clean_post)
                            has_content = True
                except Exception as e:
                    logging.error(f"DEBUG: Error cleaning post in ticket {ticket_id}: {e}")
                    import traceback
                    logging.error(f"DEBUG: Traceback: {traceback.format_exc()}")
                    
            # Save raw notes for debugging
            if 'notes' in data:
                save_debug_data("raw_notes", data['notes'], ticket_id)
                
            # Clean and format notes for standardized output
            clean_notes = []
            for note in data.get('notes', []):
                try:
                    content = note.get('contents', '')
                    if content:
                        # Save raw note content
                        save_debug_data(f"raw_note_content_{note.get('id', 'unknown')}", content, ticket_id, "txt")
                        
                        # Ensure the content is properly sanitized and not empty
                        sanitized_content = sanitize_content_string(content)
                        
                        # Save sanitized content
                        save_debug_data(f"sanitized_note_content_{note.get('id', 'unknown')}", sanitized_content, ticket_id, "txt")
                        
                        if sanitized_content:
                            clean_note = {
                                "id": str(note.get('id', '')),
                                "timestamp": note.get('time', ''),
                                "author": note.get('user', ''),
                                "content": sanitized_content
                            }
                            clean_notes.append(clean_note)
                            has_content = True
                except Exception as e:
                    logging.error(f"DEBUG: Error cleaning note in ticket {ticket_id}: {e}")
                    import traceback
                    logging.error(f"DEBUG: Traceback: {traceback.format_exc()}")
            
            # Extract technical details from posts and notes for easier analysis
            technical_details = []
            
            # From posts
            for post in clean_posts:
                content = post.get('content', '').lower()
                if any(kw in content for kw in ['cable', 'cat6', 'rack', 'network', 'config', 'fortinet', 'fortigate', 'fortiswitch', 'fortiap', 'equipment', 'install', 'upgrade']):
                    technical_details.append(f"Post ({post.get('timestamp', 'unknown')} by {post.get('author', 'unknown')}): {post.get('content', '')[:150]}...")
            
            # From notes
            for note in clean_notes:
                content = note.get('content', '').lower()
                if any(kw in content for kw in ['cable', 'cat6', 'rack', 'network', 'config', 'fortinet', 'fortigate', 'fortiswitch', 'fortiap', 'equipment', 'install', 'upgrade']):
                    technical_details.append(f"Note ({note.get('timestamp', 'unknown')} by {note.get('author', 'unknown')}): {note.get('content', '')[:150]}...")
            
            # Add additional metadata to explain missing data
            missing_data_notes = []
            if len(clean_posts) == 0:
                missing_data_notes.append("No post data available due to database sanitization issues.")
            if len(clean_notes) == 0:
                missing_data_notes.append("No note data available due to database sanitization issues.")
            if post_count > 0 and len(clean_posts) == 0:
                missing_data_notes.append(f"WARNING: {post_count} posts exist but could not be parsed due to sanitization issues.")
            if note_count > 0 and len(clean_notes) == 0:
                missing_data_notes.append(f"WARNING: {note_count} notes exist but could not be parsed due to sanitization issues.")
            
            # Extract first and last post content if available
            first_post_content = sanitize_content_string(data.get('first_post', '')) if data.get('first_post') else ''
            last_post_content = sanitize_content_string(data.get('last_post', '')) if data.get('last_post') else ''
            
            # If no clean posts but we have first_post content, add it as a dummy post
            if len(clean_posts) == 0 and first_post_content:
                clean_posts.append({
                    "id": "first_post",
                    "timestamp": data.get('created_date', 'unknown'),
                    "author": "Unknown (First Post)",
                    "content": first_post_content
                })
                logging.info(f"DEBUG: Added first post as dummy post for ticket {ticket_id}")
            
            # If no clean posts but we have last_post content, add it as a dummy post
            if len(clean_posts) == 0 and last_post_content and last_post_content != first_post_content:
                clean_posts.append({
                    "id": "last_post",
                    "timestamp": data.get('last_activity_date', 'unknown'),
                    "author": "Unknown (Last Post)",
                    "content": last_post_content
                })
                logging.info(f"DEBUG: Added last post as dummy post for ticket {ticket_id}")
            
            # Create the standardized ticket data structure
            standardized_ticket_data[ticket_id] = {
                "basic_info": {
                    "ticket_id": data.get('ticket_id', 'N/A'),
                    "subject": data.get('subject', 'N/A'),
                    "status": data.get('status', 'N/A'),
                    "department": data.get('department', 'N/A'),
                    "category": data.get('category', 'N/A'),
                    "parent_dispatch_id": data.get('parent_dispatch_id', 'N/A'),
                    "location_id": data.get('location_id', 'N/A'),
                    "site_number": data.get('site', {}).get('number', 'N/A'),
                    "project_id": data.get('project_id', 'N/A'),
                    "chain_hash": data.get('chain_hash', 'N/A')
                },
                "timeline": {
                    "created_date": data.get('created_date', 'N/A'),
                    "last_activity_date": data.get('last_activity_date', 'N/A'),
                    "closed_date": data.get('closed_date', 'N/A'),
                    "service_date": data.get('service_date', data.get('turnup_data', {}).get('service_date', 'N/A')) 
                },
                "details": {
                    "total_replies": data.get('total_replies', 0),
                    "technical_details": "\n".join(technical_details) if technical_details else "N/A",
                    "issues": data.get('issues', []),
                    "is_resolved": data.get('is_resolved', False),
                    "missing_data_notes": missing_data_notes
                },
                "interactions": {
                    "posts": clean_posts,
                    "notes": clean_notes,
                    "post_count": len(clean_posts),
                    "note_count": len(clean_notes),
                    "first_post": first_post_content,
                    "last_post": last_post_content
                },
                "location": {
                    "address": data.get('site', {}).get('address', 'N/A'),
                    "city": data.get('site', {}).get('city', 'N/A'),
                    "state": data.get('site', {}).get('state', 'N/A')
                },
                "related_data": {
                    "dispatch_data": data.get('dispatch_data'),
                    "turnup_data": data.get('turnup_data')
                }
            }
            
            # DEBUG: Log standardized ticket data stats
            logging.info(f"DEBUG: Ticket {ticket_id} standardized data contains:")
            logging.info(f"  - Posts: {len(clean_posts)}")
            logging.info(f"  - Notes: {len(clean_notes)}")
            logging.info(f"  - Issues: {len(data.get('issues', []))}")
            logging.info(f"  - Technical details length: {len(standardized_ticket_data[ticket_id]['details']['technical_details'])} chars")
            if standardized_ticket_data[ticket_id]['related_data']['dispatch_data']:
                logging.info(f"  - Has dispatch data: Yes")
            if standardized_ticket_data[ticket_id]['related_data']['turnup_data']:
                logging.info(f"  - Has turnup data: Yes")

        # Check if we have any usable post/note content and add fallback data if needed
        tickets_with_content = sum(1 for tid, data in standardized_ticket_data.items() 
                                  if data['interactions']['post_count'] > 0 or data['interactions']['note_count'] > 0)
        logging.info(f"DEBUG: {tickets_with_content} out of {len(standardized_ticket_data)} tickets have content")
        
        if tickets_with_content == 0:
            logging.warning(f"DEBUG: No tickets have content, adding fallback dummy data")
            # Add fallback text to help the AI understand the issue
            for tid, data in standardized_ticket_data.items():
                data['details']['missing_data_notes'].append(
                    "ALL TICKETS: No usable content could be parsed from the database. This is a data sanitization issue, not an indication that the tickets are empty."
                )
                # Add a dummy post with subject information
                data['interactions']['posts'].append({
                    "id": "dummy_post",
                    "timestamp": data['timeline']['created_date'],
                    "author": "System",
                    "content": f"Subject: {data['basic_info']['subject']}\nStatus: {data['basic_info']['status']}\nDepartment: {data['basic_info']['department']}"
                })
                data['interactions']['post_count'] = 1

        # Write the standardized ticket data to file with custom JSON serialization
        with open(tickets_file, 'w') as f:
            # Use a custom JSON encoder to handle any potential datetime objects
            json_data = json.dumps(standardized_ticket_data, indent=2, cls=DateTimeEncoder)
            f.write(json_data)
            # DEBUG: Log JSON file size
            logging.info(f"DEBUG: Tickets JSON file size: {len(json_data)} bytes")
        file_paths.append(tickets_file)
        logging.info(f"Created standardized tickets file: {tickets_file}")
    except Exception as e:
        logging.error(f"Error creating tickets file {tickets_file}: {e}")
        import traceback
        logging.error(f"DEBUG: Error traceback: {traceback.format_exc()}")

    # Phase 1 analysis
    phase1_file = os.path.join(output_dir, f'phase1_analysis_{chain_hash}.json')
    try:
        phase1_data = {"chain_hash": chain_hash, "phase1_summary": phase1_analysis_text}
        with open(phase1_file, 'w') as f:
            json.dump(phase1_data, f, indent=2)
        file_paths.append(phase1_file)
        logging.info(f"Created phase 1 analysis file: {phase1_file}")
    except Exception as e:
        logging.error(f"Error creating phase 1 analysis file {phase1_file}: {e}")

    # Relationships
    relationships_file = os.path.join(output_dir, f'relationships_{chain_hash}.json')
    try:
        dispatch_to_turnups = {}
        for tid, data in full_ticket_data.items():
            if data.get('category') == 'Turnup Tickets' and data.get('parent_dispatch_id'):
                dispatch_id = str(data['parent_dispatch_id'])
                if dispatch_id not in dispatch_to_turnups:
                    dispatch_to_turnups[dispatch_id] = []
                if tid not in dispatch_to_turnups[dispatch_id]:
                    dispatch_to_turnups[dispatch_id].append(tid)
        
        # Find non-1:1 relationships
        non_one_to_one = {}
        for dispatch_id, turnups in dispatch_to_turnups.items():
            if len(turnups) > 1:
                non_one_to_one[dispatch_id] = turnups
                logging.info(f"DEBUG: Dispatch {dispatch_id} has {len(turnups)} turnups: {', '.join(turnups)}")
        
        # Find orphaned tickets (tickets without relationship connections)
        orphaned_tickets = []
        for tid, data in full_ticket_data.items():
            if data.get('category') == 'Turnup Tickets' and not data.get('parent_dispatch_id'):
                orphaned_tickets.append(tid)
                logging.info(f"DEBUG: Turnup {tid} has no parent dispatch")
            elif data.get('category') == 'Dispatch Tickets' and tid not in dispatch_to_turnups:
                orphaned_tickets.append(tid)
                logging.info(f"DEBUG: Dispatch {tid} has no linked turnups")
        
        relationships_data = {
            "chain_hash": chain_hash,
            "dispatch_to_turnups": dispatch_to_turnups,
            "non_one_to_one_relationships": non_one_to_one,
            "orphaned_tickets": orphaned_tickets
        }
        
        with open(relationships_file, 'w') as f:
            json.dump(relationships_data, f, indent=2)
        file_paths.append(relationships_file)
        logging.info(f"Created relationships file: {relationships_file}")
    except Exception as e:
        logging.error(f"Error creating relationships file {relationships_file}: {e}")
    
    return file_paths

def sanitize_content_string(content):
    """Sanitize content string for JSON output - helper method for create_ticket_files"""
    if not content:
        return ""
    
    try:
        # Log original content for debugging (sample)
        content_preview = content[:100] + "..." if len(content) > 100 else content
        logging.info(f"Original content preview: {content_preview}")
        
        # Remove control characters 
        sanitized = re.sub(r'[\x00-\x1F\x7F]', '', content)
        # Replace common problematic characters
        sanitized = sanitized.replace('\0', '').replace('\r', ' ').replace('\u0000', '')
        
        # Additional sanitization steps
        sanitized = sanitized.encode('utf-8', 'ignore').decode('utf-8')
        
        # Log sanitized content
        sanitized_preview = sanitized[:100] + "..." if len(sanitized) > 100 else sanitized
        logging.info(f"Sanitized content preview: {sanitized_preview}")
        
        # Save debug files
        debug_dir = 'PyChain/data/debug'
        os.makedirs(debug_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save original content
        orig_file = f"{debug_dir}/original_content_{timestamp}.txt"
        try:
            with open(orig_file, 'w', encoding='utf-8', errors='ignore') as f:
                f.write(content)
            logging.info(f"Saved original content to {orig_file}")
        except Exception as e:
            logging.error(f"Error saving original content: {e}")
        
        # Save sanitized content
        sanitized_file = f"{debug_dir}/sanitized_content_{timestamp}.txt"
        try:
            with open(sanitized_file, 'w', encoding='utf-8') as f:
                f.write(sanitized)
            logging.info(f"Saved sanitized content to {sanitized_file}")
        except Exception as e:
            logging.error(f"Error saving sanitized content: {e}")
        
        return sanitized
    except Exception as e:
        logging.error(f"Error sanitizing content: {str(e)}")
        import traceback
        logging.error(f"Sanitization error traceback: {traceback.format_exc()}")
        return "Content unavailable due to sanitization error"

def upload_files(client, file_paths, max_retries=3):
    """Upload files to OpenAI with retries."""
    file_ids = []
    for path in file_paths:
        if not os.path.exists(path):
            logging.error(f"File not found: {path}")
            continue
        for attempt in range(max_retries):
            try:
                logging.info(f"Uploading {os.path.basename(path)} (Attempt {attempt + 1}/{max_retries})")
                with open(path, 'rb') as file:
                    upload = client.files.create(file=file, purpose="assistants")
                file_ids.append(upload.id)
                logging.info(f"Uploaded {os.path.basename(path)} -> File ID: {upload.id}")
                break
            except Exception as e:
                logging.warning(f"Upload failed for {path}, attempt {attempt + 1}: {e}")
                if attempt == max_retries - 1:
                    logging.error(f"Max retries reached for {path}")
                else:
                    time.sleep(2 ** attempt)
    return file_ids

def validate_response(response_text, expected_ticket_ids):
    """Validate and clean assistant response."""
    expected_ids = set(str(tid) for tid in expected_ticket_ids)
    if not response_text:
        logging.error("Empty response from assistant")
        return None
    try:
        response_text = response_text.strip()
        if response_text.startswith("```json"):
            response_text = response_text[7:]
        if response_text.startswith("```"):
            response_text = response_text[3:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]
        response_text = response_text.strip()
        response_json = json.loads(response_text)
        found_ids = set()
        processed_json = response_json

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
                            unique_items.append(item)
                            seen_ids.add(tid_str)
                        else:
                            logging.warning(f"Removed duplicate ticket ID {tid_str}")
                    else:
                        unique_items.append(item)
                else:
                    unique_items.append(item)
            processed_json = unique_items
        elif isinstance(response_json, dict):
            if "JobScopeTasks" in response_json and isinstance(response_json["JobScopeTasks"], list):
                unique_tasks = []
                seen_task_ids = set()
                for task in response_json["JobScopeTasks"]:
                         tid = task.get("TicketID", task.get("TaskID"))
                         if tid:
                             tid_str = str(tid)
                             found_ids.add(tid_str)
                             if tid_str not in seen_task_ids:
                                 unique_tasks.append(task)
                                 seen_task_ids.add(tid_str)
                             else:
                                 logging.warning(f"Removed duplicate task ID {tid_str}")
                         else:
                             unique_tasks.append(task)
                response_json["JobScopeTasks"] = unique_tasks
                processed_json = response_json

        missing_tickets = expected_ids - found_ids
        if missing_tickets:
            logging.warning(f"Missing ticket IDs: {missing_tickets}")
            extra_tickets = found_ids - expected_ids
            if extra_tickets:
                logging.warning(f"Unexpected ticket IDs: {extra_tickets}")

        unsupported_indicators = ["Customer Feedback Score", "Skill Match", "Completion Percentage"]
        response_str_lower = response_text.lower()
        for indicator in unsupported_indicators:
            if f'"{indicator.lower()}":' in response_str_lower:
                logging.warning(f"Unsupported field detected: {indicator}")

        return processed_json
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON: {e}\nRaw Response:\n{response_text}")
        return None
    except Exception as e:
        logging.error(f"Validation error: {e}\nRaw Response:\n{response_text}")
        return None

def run_assistant_query(client, thread_id, assistant_id, prompt, timeout_seconds=600):
    """Run a query against an assistant and return the response."""
    logging.info(f"Running query against assistant {assistant_id}")
    
    # DEBUG: Log query details
    logging.info(f"DEBUG: Thread ID: {thread_id}")
    logging.info(f"DEBUG: Assistant ID: {assistant_id}")
    logging.info(f"DEBUG: Prompt length: {len(prompt)} characters")
    logging.info(f"DEBUG: Prompt first 100 chars: {prompt[:100]}...")
    
    try:
        # DEBUG: Log message creation attempt
        logging.info(f"DEBUG: Creating message in thread {thread_id}")
        try:
            message = client.beta.threads.messages.create(
                thread_id=thread_id,
                role="user",
                content=prompt
            )
            logging.info(f"DEBUG: Message created with ID: {message.id}")
        except AttributeError as ae:
            logging.info(f"DEBUG: AttributeError in message creation: {ae}, trying non-beta endpoint")
            message = client.threads.messages.create(
                thread_id=thread_id,
                role="user",
                content=prompt
            )
            logging.info(f"DEBUG: Message created with ID: {message.id}")
        except Exception as e:
            logging.error(f"DEBUG: Error creating message: {e}")
            import traceback
            logging.error(f"DEBUG: Message creation error traceback: {traceback.format_exc()}")
            raise
    except Exception as e:
        logging.error(f"Error creating message: {e}")
        raise
    
    try:
        # DEBUG: Log run creation attempt
        logging.info(f"DEBUG: Creating run in thread {thread_id} with assistant {assistant_id}")
        try:
            run = client.beta.threads.runs.create(
                thread_id=thread_id,
                assistant_id=assistant_id
            )
            logging.info(f"DEBUG: Run created with ID: {run.id}")
        except AttributeError as ae:
            logging.info(f"DEBUG: AttributeError in run creation: {ae}, trying non-beta endpoint")
            run = client.threads.runs.create(
                thread_id=thread_id,
                assistant_id=assistant_id
            )
            logging.info(f"DEBUG: Run created with ID: {run.id}")
        except Exception as e:
            logging.error(f"DEBUG: Error creating run: {e}")
            import traceback
            logging.error(f"DEBUG: Run creation error traceback: {traceback.format_exc()}")
            raise
    except Exception as e:
        logging.error(f"Error creating run: {e}")
        raise
    
    logging.info(f"Run {run.id} started")
    
    # Monitor the run status with more detailed debug information
    start_time = time.time()
    run_status = None
    status_history = []
    
    while time.time() - start_time < timeout_seconds:
        try:
            # DEBUG: Log run status check
            elapsed = int(time.time() - start_time)
            logging.info(f"DEBUG: Checking run status at {elapsed}s")
            
            try:
                run_status = client.beta.threads.runs.retrieve(thread_id=thread_id, run_id=run.id)
            except AttributeError:
                logging.info(f"DEBUG: Using non-beta endpoint for run status")
                run_status = client.threads.runs.retrieve(thread_id=thread_id, run_id=run.id)
            
            # DEBUG: Keep track of status changes
            current_status = run_status.status
            if not status_history or status_history[-1] != current_status:
                status_history.append(current_status)
                logging.info(f"DEBUG: Run status changed to: {current_status}")
                logging.info(f"DEBUG: Full status history: {' -> '.join(status_history)}")
            
            if current_status == "completed":
                logging.info(f"Run {run.id} completed")
                logging.info(f"DEBUG: Run completed in {elapsed}s")
                break
            elif current_status in ["failed", "cancelled", "expired"]:
                error_details = getattr(run_status, 'last_error', 'No error details')
                logging.error(f"Run {run.id} {current_status}")
                logging.error(f"DEBUG: Run failed with error: {error_details}")
                raise Exception(f"Run {run.id} {current_status}")
            elif current_status == "requires_action":
                logging.warning(f"Run {run.id} requires action: {run_status.required_action}")
                logging.warning(f"DEBUG: Run requires action with details: {run_status.required_action}")
                time.sleep(15)
            else:
                logging.info(f"Waiting for run {run.id} (Status: {current_status})... ({elapsed}s)")
                time.sleep(10)
        except Exception as e:
            logging.error(f"Error checking run status: {e}")
            import traceback
            logging.error(f"DEBUG: Status check error traceback: {traceback.format_exc()}")
            time.sleep(10)
    else:
        logging.error(f"DEBUG: Run timed out after {timeout_seconds}s. Final status: {run_status.status if run_status else 'unknown'}")
        raise TimeoutError(f"Run {run.id} timed out after {timeout_seconds} seconds")

    # Retrieve the assistant's response
    try:
        # DEBUG: Log message retrieval attempt
        logging.info(f"DEBUG: Retrieving messages from thread {thread_id}")
        
        try:
            messages = client.beta.threads.messages.list(thread_id=thread_id, order="desc", limit=5)
        except AttributeError:
            logging.info(f"DEBUG: Using non-beta endpoint for message retrieval")
            messages = client.threads.messages.list(thread_id=thread_id, order="desc", limit=5)
        
        logging.info(f"DEBUG: Retrieved {len(messages.data)} messages")
        
        for msg in messages.data:
            if msg.role == "assistant" and msg.run_id == run.id:
                # DEBUG: Log message details
                msg_content = msg.content
                logging.info(f"DEBUG: Found assistant response message with ID: {msg.id}")
                logging.info(f"DEBUG: Message content type: {type(msg_content)}")
                logging.info(f"DEBUG: Message content structure: {msg_content}")
                
                if msg.content and isinstance(msg.content, list) and len(msg.content) > 0:
                    content_block = msg.content[0]
                    logging.info(f"DEBUG: Content block type: {type(content_block)}")
                    
                    if hasattr(content_block, 'text') and content_block.text:
                        response_text = content_block.text.value
                        logging.info(f"DEBUG: Response text length: {len(response_text)} characters")
                        logging.info(f"DEBUG: Response first 100 chars: {response_text[:100]}...")
                        return response_text
                    else:
                        logging.warning(f"DEBUG: Non-text content block: {content_block}")
                else:
                    logging.warning(f"DEBUG: Empty/invalid message content: {msg.content}")
        
        # DEBUG: If we got here, we didn't find the right message
        logging.error("DEBUG: No valid assistant message found with the correct run_id")
        all_msg_ids = [f"{m.id} (role: {m.role}, run_id: {m.run_id})" for m in messages.data]
        logging.error(f"DEBUG: All messages found: {all_msg_ids}")
        
        raise Exception("No valid assistant message found")
    except Exception as e:
        logging.error(f"Error retrieving assistant response: {e}")
        import traceback
        logging.error(f"DEBUG: Response retrieval error traceback: {traceback.format_exc()}")
        raise
    
    logging.error("No response from assistant")
    return "No response from assistant"

def create_ticket_batches(full_ticket_data):
    """Create batches of related tickets, prioritizing dispatch-turnup relationships."""
    batches = []
    dispatch_to_turnups = {}
    orphan_tickets = []

    # Map dispatch to turnup tickets
    for tid, data in full_ticket_data.items():
        if data.get('category') == 'Turnup Tickets' and data.get('parent_dispatch_id'):
            dispatch_id = str(data['parent_dispatch_id'])
            if dispatch_id not in dispatch_to_turnups:
                dispatch_to_turnups[dispatch_id] = []
            dispatch_to_turnups[dispatch_id].append(tid)
        else:
            orphan_tickets.append(tid)

    # Create batches for dispatch-turnup groups
    for dispatch_id, turnup_ids in dispatch_to_turnups.items():
        if dispatch_id in full_ticket_data:
            batch = [dispatch_id] + turnup_ids
            focus = f"Dispatch {dispatch_id} and related Turnups"
            if len(turnup_ids) > 1:
                focus += " (Non-1:1 relationship)"
            batches.append({
                'ticket_ids': batch,
                'focus': focus
            })

    # Handle orphans in smaller batches
    batch_size = 3
    for i in range(0, len(orphan_tickets), batch_size):
        batch = orphan_tickets[i:i + batch_size]
        batches.append({
            'ticket_ids': batch,
            'focus': "Orphan or Uncategorized Tickets"
        })

    return batches

def create_batch_analysis_prompt(chain_hash, batch, full_ticket_data):
    """Create a prompt for analyzing a batch of tickets."""
    ticket_ids_str = ", ".join(f'"{tid}"' for tid in batch['ticket_ids'])
    prompt = f"""
Analyze the ticket chain with hash {chain_hash} for the following batch of tickets: [{ticket_ids_str}].
Focus: {batch['focus']}.
Extract detailed metrics and issues for each ticket, including:
- Ticket ID, Category, Status, Subject, Department, Queue, Audit Status
- Timeline: Creation date, service date, completion date, closed date
- Scope: Technical details (e.g., cabling, equipment), technician details
- Outcome: Completion status, issues, cancellations, failure reasons
- Revisit: Required? If so, why?
- Metrics: Time on site, delays, accounting details (PO, amount, billing status)
- Relationships: Parent dispatch ID, linked tickets
- Issues: Data quality (e.g., epoch dates, location mismatches), non-1:1 relationships, orphaned records
Provide the response as a valid JSON list of objects, one per ticket. Cite evidence from ticket data (e.g., posts, notes, dispatch/turnup data) where possible. Refer to uploaded files for details.
"""
    for tid in batch['ticket_ids']:
        data = full_ticket_data.get(tid, {})
        technical_details = data.get('technical_details', 'N/A')
        if len(technical_details) > 200:
            technical_details = technical_details[:200] + "..."
        prompt += f"- Ticket {tid}:\n"
        prompt += f"  Category: {data.get('category', 'N/A')}\n"
        prompt += f"  Subject: {data.get('subject', 'N/A')}\n"
        prompt += f"  Status: {data.get('status', 'N/A')}\n"
        prompt += f"  Department: {data.get('department', 'N/A')}\n"
        prompt += f"  Queue: {data.get('queue', 'N/A')}\n"
        prompt += f"  Audit Status: {data.get('audit_status', 'N/A')}\n"
        prompt += f"  Created: {data.get('created_date', 'N/A')}\n"
        prompt += f"  Last Activity: {data.get('last_activity_date', 'N/A')}\n"
        prompt += f"  Closed: {data.get('closed_date', 'N/A')}\n"
        prompt += f"  Technical Details: {technical_details}\n"
        prompt += f"  Parent Dispatch ID: {data.get('parent_dispatch_id', 'N/A')}\n"
        prompt += f"  Issues: {', '.join(data.get('issues', [])) or 'None'}\n"
        prompt += f"  Posts/Notes: {len(data.get('posts', []))} posts, {len(data.get('notes', []))} notes\n"
        if data.get('turnup_data'):
            prompt += f"  Turnup Details:\n"
            prompt += f"    Technician: {data['turnup_data'].get('technician_name', 'N/A')}\n"
            prompt += f"    In Time: {data['turnup_data'].get('in_time', 'N/A')}\n"
            prompt += f"    Out Time: {data['turnup_data'].get('out_time', 'N/A')}\n"
            prompt += f"    Duration: {data['turnup_data'].get('duration', 'N/A')}\n"
        if data.get('accounting_details'):
            prompt += f"  Accounting Details:\n"
            prompt += f"    PO: {data['accounting_details'].get('po', 'N/A')}\n"
            prompt += f"    Billing Type: {data['accounting_details'].get('billing_type', 'N/A')}\n"
            prompt += f"    Amount: {data['accounting_details'].get('amount', 'N/A')}\n"
    prompt += "\nOutput ONLY valid JSON."
    return prompt

def compile_issues_index(batch_results):
    """Compile issues from batch analysis results."""
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
        """Generate follow-up questions for tickets with issues, focusing on dispatch, planning, work, and operational issues."""
        prompt = f"""
    Based on the analysis for chain {chain_hash}, generate specific follow-up questions to clarify issues identified in the tickets. Questions MUST focus on the dispatch itself, planning, work performed during the visit, issues requiring more details, reasons for failure or incomplete status, or other operational concerns. Do NOT generate questions about ticket relationships, missing relationships, or dispatch-turnup linkages (e.g., avoid questions like "Why is this turnup not linked to a dispatch?").

    Issues Index (filtered to exclude relationship-related issues):
    """
        # Filter out relationship-related issues
        filtered_issues = []
        for entry in issues_index.get("tickets_with_issues", []):
            filtered_entry = {
                "ticket_id": entry["ticket_id"],
                "issues": [
                    issue for issue in (entry["issues"] if isinstance(entry["issues"], list) else [entry["issues"]])
                    if not any(kw in issue.lower() for kw in ["orphaned", "linked", "relationship"])
                ]
            }
            if filtered_entry["issues"]:  # Only include entries with non-empty filtered issues
                filtered_issues.append(filtered_entry)

        if filtered_issues:
            for entry in filtered_issues:
                prompt += f"- Ticket {entry['ticket_id']}:\n  - Issues: {', '.join(entry['issues'])}\n"
        else:
            prompt += "No relevant issues found after filtering relationship-related issues.\n"

        prompt += f"""
    Provide ONLY a valid JSON object with a key `questions_by_ticket` mapping ticket IDs to a list of up to 3 specific questions to clarify the issues. Questions should address:
    - Details of the dispatch planning (e.g., scheduling, resource allocation).
    - Work performed during the visit (e.g., tasks completed, equipment installed).
    - Specific issues needing clarification (e.g., delays, missing materials).
    - Reasons for failure, cancellation, or incomplete status (e.g., environmental factors, technician issues).
    - Operational concerns (e.g., communication with site, safety issues).

    Examples of acceptable questions:
    - "What specific tasks were completed during the visit for this dispatch?"
    - "Why was the dispatch delayed, and what resources were missing?"
    - "What caused the cancellation of this turnup due to a snowstorm?"
    - "What additional details are needed to understand the technician's no-check-in issue?"
    - "Why was the work incomplete, and what is required to finish it?"

    Include all tickets [{ticket_ids_list_str}], using empty lists for tickets without questions. Output ONLY valid JSON.
    """
        return prompt

def get_tickets_with_questions(questions_json, user_questions, full_ticket_data):
    """Compile tickets needing re-analysis with questions."""
    tickets_with_questions = {}
    if questions_json and isinstance(questions_json, dict):
        questions_by_ticket = questions_json.get("questions_by_ticket", {})
        for ticket_id, questions in questions_by_ticket.items():
            if questions and isinstance(questions, list):
                tickets_with_questions[ticket_id] = {"questions": questions}

    user_q = user_questions.get("global_question", "")
    if user_q:
        for ticket_id, data in full_ticket_data.items():
            if data.get("category") in ["Dispatch Tickets", "Turnup Tickets"] and not data.get('accounting_details'):
                if ticket_id not in tickets_with_questions:
                    tickets_with_questions[ticket_id] = {"questions": []}
                tickets_with_questions[ticket_id]["questions"].append(user_q)

    return tickets_with_questions

def create_detailed_analysis_prompt(chain_hash, ticket_id, full_ticket_data, questions):
    """Create a prompt for detailed ticket re-analysis."""
    data = full_ticket_data.get(ticket_id, {})
    questions_str = "\n".join([f"- {q}" for q in questions])
    prompt = f"""
Perform a detailed analysis of ticket {ticket_id} in chain {chain_hash}, answering specific questions.

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
    prompt += f"- Posts: {len(data.get('posts', []))} entries (first few):\n"
    for i, post in enumerate(data.get('posts', [])[:3]):
        content = post.get('content', 'N/A')
        if len(content) > 150:
            content = content[:150] + "..."
        prompt += f"  Post {i+1} ({post.get('timestamp', 'N/A')} by {post.get('author', 'N/A')}): {content}\n"
    prompt += f"- Notes: {len(data.get('notes', []))} entries (first few):\n"
    for i, note in enumerate(data.get('notes', [])[:3]):
        content = note.get('content', 'N/A')
        if len(content) > 150:
            content = content[:150] + "..."
        prompt += f"  Note {i+1} ({note.get('timestamp', 'N/A')} by {note.get('author', 'N/A')}): {content}\n"
    prompt += f"""
Questions:
{questions_str}

Respond with a JSON object containing:
- `ticket_id`: The ticket ID
- `category`: The ticket category
- `detailed_metrics`: Timeline, scope, outcome, revisit info, metrics
- `answers`: Map each question to its answer with evidence
Output ONLY valid JSON.
"""
    return prompt

def consolidate_final_report(batch_results, issues_index, detailed_results, full_ticket_data, chain_hash):
    """Consolidate analysis results into a final report."""
    report = {
        "chain_hash": chain_hash,
        "timestamp": datetime.now().isoformat(),
        "project_id": next((d.get('project_id') for d in full_ticket_data.values() if d.get('project_id')), "N/A"),
        "site_number": next((d.get('site_number') for d in full_ticket_data.values() if d.get('site_number')), "N/A"),
        "customer": "Flynn",
        "location": {
            "name": "Wendy's FW008350",
            "address": "17786 Garland Groh Blvd",
            "city": "Hagerstown",
            "state": "MD",
            "zipcode": "21740",
            "phone": "301-797-4818",
            "timezone": "America/New_York"
        },
        "relationships": [],
        "tickets_analyzed": [],
        "metrics_summary": {},
        "issues_summary": issues_index,
        "detailed_analyses": [],
        "missing_data_notes": []
    }

    ticket_summaries = {}
    for batch in batch_results:
        if "result" in batch and isinstance(batch["result"], list):
            for ticket_analysis in batch["result"]:
                ticket_id = str(ticket_analysis.get("TicketID", ticket_analysis.get("ticket_id", "")))
                if ticket_id:
                    ticket_summaries[ticket_id] = ticket_analysis
                    report["tickets_analyzed"].append(ticket_id)

    for detail in detailed_results:
        if "result" in detail and isinstance(detail["result"], dict) and detail["result"].get("ticket_id"):
            report["detailed_analyses"].append(detail["result"])

    # Build relationships
    dispatch_to_turnups = {}
    for tid, data in full_ticket_data.items():
        if data.get('category') == 'Turnup Tickets' and data.get('parent_dispatch_id'):
            dispatch_id = data['parent_dispatch_id']
            if dispatch_id not in dispatch_to_turnups:
                dispatch_to_turnups[dispatch_id] = []
            dispatch_to_turnups[dispatch_id].append(tid)
    for dispatch_id, turnup_ids in dispatch_to_turnups.items():
        notes = "Direct linkage via DispatchId"
        confidence = "High"
        if len(turnup_ids) > 1:
            notes += f" (Non-1:1, {len(turnup_ids)} turnups)"
            confidence = "Medium"
        report["relationships"].append({
            "dispatch_ticket_id": dispatch_id,
            "turnup_ticket_ids": turnup_ids,
            "confidence": confidence,
            "notes": notes
        })
    for tid, data in full_ticket_data.items():
        if data.get('category') == 'Turnup Tickets' and not data.get('parent_dispatch_id'):
            report["relationships"].append({
                "dispatch_ticket_id": None,  # Fixed: Changed null to None
                "turnup_ticket_ids": [tid],
                "confidence": "Low",
                "notes": "Orphaned turnup, no linked dispatch"
            })

    # Metrics
    total_tickets = len(report["tickets_analyzed"])
    dispatch_count = sum(1 for tid in ticket_summaries if full_ticket_data.get(tid, {}).get("category") == "Dispatch Tickets")
    turnup_count = sum(1 for tid in ticket_summaries if full_ticket_data.get(tid, {}).get("category") == "Turnup Tickets")
    shipping_count = sum(1 for tid in ticket_summaries if full_ticket_data.get(tid, {}).get("category") == "Shipping Tickets")
    project_count = sum(1 for tid in ticket_summaries if full_ticket_data.get(tid, {}).get("category") == "Project Management Tickets")
    revisits = sum(1 for tid in ticket_summaries if ticket_summaries[tid].get("Revisit", {}).get("Required", False))
    orphans = sum(1 for r in report["relationships"] if r["dispatch_ticket_id"] is None)
    non_1_to_1 = sum(1 for r in report["relationships"] if len(r["turnup_ticket_ids"]) > 1)
    cancellations = sum(1 for tid in ticket_summaries if "Cancelled" in full_ticket_data.get(tid, {}).get("status", ""))
    failures = sum(1 for tid in ticket_summaries if "Failed" in full_ticket_data.get(tid, {}).get("status", ""))
    audit_completed = sum(1 for tid in ticket_summaries if full_ticket_data.get(tid, {}).get("audit_status") == "Audited")
    audit_pending = sum(1 for tid in ticket_summaries if full_ticket_data.get(tid, {}).get("audit_status") == "Cleanup")
    location_mismatches = sum(1 for tid in ticket_summaries if any("location mismatch" in issue.lower() for issue in full_ticket_data.get(tid, {}).get("issues", [])))
    epoch_dates = sum(1 for tid in ticket_summaries if any("epoch" in issue.lower() for issue in full_ticket_data.get(tid, {}).get("issues", [])))

    report["metrics_summary"] = {
        "total_tickets": total_tickets,
        "dispatch_tickets": dispatch_count,
        "turnup_tickets": turnup_count,
        "shipping_tickets": shipping_count,
        "project_management_tickets": project_count,
        "revisits_required": revisits,
        "orphaned_records": orphans,
        "non_1_to_1_relationships": non_1_to_1,
        "cancellations": cancellations,
        "failures": failures,
        "audit_pending": audit_pending,
        "audit_completed": audit_completed,
        "location_mismatches": location_mismatches,
        "epoch_dates": epoch_dates
    }

    # Missing data notes
    missing_notes = []
    for tid, data in full_ticket_data.items():
        if data.get('category') == 'Turnup Tickets' and (not data.get('turnup_data', {}).get('in_time') or not data.get('turnup_data', {}).get('out_time')):
            missing_notes.append(f"Ticket {tid} missing visit times (InTime/OutTime)")
        if data.get('closed_date') and '1969-12-31' in data.get('closed_date'):
            missing_notes.append(f"Ticket {tid} has epoch closed date")
        if not data.get('technical_details') or data.get('technical_details') == 'N/A':
            missing_notes.append(f"Ticket {tid} lacks technical details")
    report["missing_data_notes"] = list(set(missing_notes))

    return report

def cleanup_openai_resources(client, file_ids, vector_store_id=None, delete_vector_store=False):
    """Clean up OpenAI resources (files and vector store) when done."""
    logging.info("Cleaning up OpenAI resources...")
    
    if vector_store_id:
        for file_id in file_ids:
            try:
                client.vector_stores.files.delete(vector_store_id=vector_store_id, file_id=file_id)
                logging.info(f"Removed file {file_id} from vector store {vector_store_id}")
            except Exception as e:
                logging.error(f"Error removing file {file_id} from vector store: {e}")

    for file_id in file_ids:
        try:
            client.files.delete(file_id=file_id)
            logging.info(f"Deleted file {file_id}")
        except Exception as e:
            logging.error(f"Error deleting file {file_id}: {e}")

    if delete_vector_store and vector_store_id:
        try:
            client.vector_stores.delete(vector_store_id)
            logging.info(f"Deleted vector store {vector_store_id}")
        except Exception as e:
            logging.error(f"Error deleting vector store {vector_store_id}: {e}")
    return True

def create_nlp_extraction_prompt(chain_hash, ticket_id, ticket_data):
    """Create a prompt for advanced NLP data extraction from ticket text."""
    # Start with basic ticket information
    prompt = f"""
Analyze ticket {ticket_id} in chain {chain_hash} to extract structured data from the provided notes and posts.

Ticket Basic Information:
- Category: {ticket_data.get('category', 'N/A')}
- Subject: {ticket_data.get('subject', 'N/A')}
- Status: {ticket_data.get('status', 'N/A')}
- Created: {ticket_data.get('created_date', 'N/A')}
- Last Activity: {ticket_data.get('last_activity_date', 'N/A')}
- Closed: {ticket_data.get('closed_date', 'N/A')}
- Site: {ticket_data.get('site', {}).get('address', 'N/A')}, {ticket_data.get('site', {}).get('city', 'N/A')}, {ticket_data.get('site', {}).get('state', 'N/A')}
- Project ID: {ticket_data.get('project_id', 'N/A')}
"""

    # Add site location information if available
    if ticket_data.get('site', {}).get('number') != 'N/A':
        prompt += f"- Site Number: {ticket_data.get('site', {}).get('number', 'N/A')}\n"
    
    # Include first post content if available
    if ticket_data.get('first_post'):
        prompt += f"\nFirst Post:\n{ticket_data.get('first_post')[:500]}...\n"
    
    # Include technical details if available
    technical_details = ticket_data.get('details', {}).get('technical_details', 'N/A')
    if technical_details and technical_details != 'N/A':
        # Truncate if too long
        if len(technical_details) > 1000:
            prompt += f"\nTechnical Details:\n{technical_details[:1000]}...\n"
        else:
            prompt += f"\nTechnical Details:\n{technical_details}\n"

    # Add posts and notes content
    posts = ticket_data.get('interactions', {}).get('posts', [])
    notes = ticket_data.get('interactions', {}).get('notes', [])
    
    if posts:
        prompt += f"\nPosts ({len(posts)}):\n"
        # Include up to 10 posts, prioritizing posts with technical content
        technical_posts = []
        other_posts = []
        
        for post in posts:
            content = post.get('content', '')
            if any(kw in content.lower() for kw in ['cable', 'cat6', 'rack', 'network', 'config', 'install', 'upgrade', 'materials', 'equipment']):
                technical_posts.append(post)
            else:
                other_posts.append(post)
        
        # Show technical posts first, then others, up to a total of 10
        display_posts = technical_posts + other_posts
        display_posts = display_posts[:10]  # Limit to 10 posts
        
        for i, post in enumerate(display_posts):
            content = post.get('content', '')
            # Truncate content if too long
            if len(content) > 300:
                content = content[:300] + "..."
            prompt += f"  Post {i+1} ({post.get('timestamp', 'N/A')} by {post.get('author', 'N/A')}):\n  {content}\n\n"
    
    if notes:
        prompt += f"\nNotes ({len(notes)}):\n"
        # Include up to 5 notes
        for i, note in enumerate(notes[:5]):
            content = note.get('content', '')
            # Truncate content if too long
            if len(content) > 300:
                content = content[:300] + "..."
            prompt += f"  Note {i+1} ({note.get('timestamp', 'N/A')} by {note.get('author', 'N/A')}):\n  {content}\n\n"
    
    # Include any dispatch or turnup data if available
    dispatch_data = ticket_data.get('related_data', {}).get('dispatch_data')
    if dispatch_data:
        prompt += f"\nDispatch Data:\n"
        prompt += f"  - Status: {dispatch_data.get('status', 'N/A')}\n"
        prompt += f"  - Type: {dispatch_data.get('type', 'N/A')}\n"
        prompt += f"  - Service Date: {dispatch_data.get('service_date', 'N/A')}\n"
        prompt += f"  - Service Time: {dispatch_data.get('service_time', 'N/A')}\n"
        prompt += f"  - Priority: {dispatch_data.get('priority', 'N/A')}\n"
        prompt += f"  - Customer: {dispatch_data.get('customer_name', 'N/A')}\n"
    
    turnup_data = ticket_data.get('related_data', {}).get('turnup_data')
    if turnup_data:
        prompt += f"\nTurnup Data:\n"
        prompt += f"  - Status: {turnup_data.get('status', 'N/A')}\n"
        prompt += f"  - Service Date: {turnup_data.get('service_date', 'N/A')}\n"
        prompt += f"  - Technician: {turnup_data.get('technician_name', 'N/A')}\n"
        if turnup_data.get('notes'):
            prompt += f"  - Notes: {turnup_data.get('notes', 'N/A')[:300]}...\n"
    
    # Include any issues found
    issues = ticket_data.get('issues', [])
    if issues:
        prompt += f"\nIssues Identified:\n"
        for issue in issues:
            prompt += f"  - {issue}\n"
    
    # Include missing data notes
    missing_data_notes = ticket_data.get('details', {}).get('missing_data_notes', [])
    if missing_data_notes:
        prompt += f"\nMissing Data Notes:\n"
        for note in missing_data_notes:
            prompt += f"  - {note}\n"

    # Add extraction instructions
    prompt += """
From the provided ticket notes/posts, explicitly extract the following structured data fields:
- Ticket ID, Date, Start/End times
- Site Address, Site Contact
- Technicians (names, roles)
- Vendor name associated clearly
- SLA Metrics (response time, compliance status)
- Customer Feedback (rating/comments)
- Tasks performed (task ID, description, status, notes, dependencies)
- Issues encountered (issue ID, description, mitigation, remediation notes, root cause, status, escalations)
- Visit Outcomes (success/failure/partial, completion percentage, closeout notes)
- Materials & Inventory used (part numbers, quantities)
- Revisit required (Yes/No, reasons)
- Audit trail events (timestamps, actions, users)

IMPORTANT: Only extract information that is explicitly mentioned in the ticket data. If information is not available, use 'not specified' for text fields, empty arrays for list fields, or null for numeric fields.

Provide output strictly as structured JSON matching the following schema:
{
  "ticket_id": "string or number",
  "date": "string (YYYY-MM-DD)",
  "start_time": "string (HH:MM)",
  "end_time": "string (HH:MM)",
  "site_id": "string",
  "vendor_id": "string",
  "technicians": [
    {
      "technician_id": "string",
      "name": "string",
      "role": "string"
    }
  ],
  "sla_metrics": {
    "response_time": "string",
    "completion_deadline": "string (YYYY-MM-DDTHH:MM)",
    "sla_compliance": false
  },
  "customer_feedback": {
    "rating": null,
    "comments": "string"
  },
  "tasks": [
    {
      "task_id": "string",
      "description": "string",
      "status": "string",
      "completed": false,
      "notes": "string",
      "dependencies": ["string"]
    }
  ],
  "tasks_closeout": {
    "tasks_completed_percentage": 0,
    "all_tasks_completed": false,
    "total_tasks": 0,
    "tasks_failed": 0,
    "closeout_notes": "string"
  },
  "status": "string",
  "site_address": "string",
  "site_contact": "string",
  "customer_name": "string",
  "customer_signature": "string",
  "actual_duration_minutes": null,
  "travel_time_minutes": null,
  "materials_used": [
    {
      "item": "string",
      "quantity": 0,
      "part_number": "string"
    }
  ],
  "inventory": {
    "items": []
  },
  "issues_encountered": [
    {
      "issue_id": "string",
      "description": "string",
      "time_impact": "string",
      "mitigation": "string",
      "information": "string",
      "remediation_notes": "string",
      "status": "string",
      "escalation": {
        "escalated_to": "string",
        "status": "string"
      },
      "root_cause": "string"
    }
  ],
  "issues_closeout": {
    "total_issues": 0,
    "issues_resolved": 0,
    "issues_unresolved": 0,
    "revisits_triggered": 0,
    "closeout_notes": "string"
  },
  "resolutions": [],
  "revisit_required": false,
  "revisits_required": [],
  "notes": "string",
  "attachments": [],
  "financials": [],
  "financials_closeout": {
    "total_cost": 0,
    "cost_breakdown": {
      "Labor": 0,
      "Materials": 0,
      "Trip Charge": 0,
      "Tax": 0,
      "Equipment Rental": 0
    },
    "tax_included": false,
    "closeout_notes": "string"
  },
  "task_outcome": "string",
  "completion_percentage": 0,
  "audit_trail": []
}
Output ONLY valid JSON matching this schema.
"""
    return prompt

def run_phase_2_analysis(client, chain_details, phase1_analysis_text):
    """Run multi-stage Phase 2 analysis with turnup-dispatch pair focus."""
    try:
        vector_store_id, assistant_id = setup_vector_store_and_assistant(client, [])
        if not assistant_id or not vector_store_id:
            logging.error("Failed to set up assistant or vector store")
            return
    except Exception as e:
        logging.error(f"Error in setup: {e}")
        return

    logging.info(f"Starting Phase 2 Analysis (Assistant: {assistant_id}, Store: {vector_store_id})")
    expected_ticket_ids = [str(ticket.get('ticket_id')) for ticket in chain_details.get('tickets', [])]
    if not expected_ticket_ids:
        logging.error("No ticket IDs found for Phase 2")
        return

    chain_hash = chain_details['chain_hash']
    file_ids_uploaded = []
    file_paths_created = []
    session = None
    cissdm_session = None
    final_responses = {"chain_hash": chain_hash, "ticket_count": len(expected_ticket_ids), "stages": {}}

    # Setup
    session = get_db_session("primary")
    cissdm_session = get_db_session("cissdm")
    if not session:
        raise ConnectionError("Failed to connect to primary database")

    logging.info("Fetching full ticket data...")
    full_ticket_data = fetch_full_ticket_data(session, cissdm_session, expected_ticket_ids)
    if not full_ticket_data:
        raise ValueError("Failed to fetch ticket data")

    logging.info("Creating analysis files...")
    file_paths_created = create_ticket_files(chain_details, full_ticket_data, phase1_analysis_text)

    logging.info("Uploading files...")
    file_ids_uploaded = upload_files(client, file_paths_created)
    if not file_ids_uploaded:
        raise ValueError("File upload failed")

    logging.info(f"Adding {len(file_ids_uploaded)} files to vector store {vector_store_id}...")
    successful_adds = []
    for file_id in file_ids_uploaded:
        try:
            client.vector_stores.files.create(vector_store_id=vector_store_id, file_id=file_id)
            successful_adds.append(file_id)
            logging.info(f"Added file {file_id} to vector store")
        except Exception as e:
            logging.error(f"Error adding file {file_id}: {e}")
    if not successful_adds:
        raise ValueError("No files added to vector store")

    logging.info("Waiting for vector store processing...")
    if not wait_for_vector_store_processing(client, vector_store_id, successful_adds):
        raise TimeoutError("Vector store processing timed out")

    try:
        thread = client.beta.threads.create()
        logging.info(f"Analysis thread created: {thread.id}")
    except AttributeError:
        thread = client.threads.create()
        logging.info(f"Analysis thread created: {thread.id}")

    # Create ticket pairs focusing on turnup tickets as primary
    logging.info("Creating turnup-dispatch ticket pairs...")
    ticket_pairs = create_ticket_pairs(full_ticket_data)
    
    # Step 1: Analyze each ticket pair (turnup-dispatch pairs are prioritized)
    logging.info("Running Phase 2 Step 1: Turnup-Dispatch Pair Analysis")
    pair_analysis_results = []
    
    for i, pair in enumerate(ticket_pairs):
        primary_id = pair["primary_ticket_id"]
        related_id = pair["related_ticket_id"]
        pair_type = pair["pair_type"]
        
        pair_description = f"{pair['primary_type'].capitalize()} {primary_id}"
        if related_id:
            pair_description += f" with {pair['related_type'].capitalize()} {related_id}"
        
        logging.info(f"Analyzing Pair {i+1}/{len(ticket_pairs)}: {pair_description}")
        
        # Create a detailed prompt for this specific ticket pair
        pair_prompt = create_ticket_pair_analysis_prompt(chain_hash, pair, full_ticket_data)
        
        try:
            pair_response = run_assistant_query(client, thread.id, assistant_id, pair_prompt)
            validated_json = validate_response(pair_response, [primary_id] + ([related_id] if related_id else []))
            
            pair_result = {
                "primary_ticket_id": primary_id,
                "primary_type": pair["primary_type"],
                "related_ticket_id": related_id,
                "pair_type": pair_type,
                "result": validated_json if validated_json else {"error": "Invalid JSON", "raw": pair_response}
            }
            
            pair_analysis_results.append(pair_result)
            final_responses["stages"][f"Pair_Analysis_{primary_id}_{related_id if related_id else 'none'}"] = pair_result
            
            print(f"\n--- Pair Analysis: {pair_description} ---")
            if validated_json:
                print(json.dumps(validated_json, indent=2))
            else:
                print(f"ERROR: Invalid JSON.\nRaw:\n{pair_response}")
            print("-----------------------------------")
        except Exception as e:
            logging.error(f"Pair analysis for {pair_description} failed: {e}")
            pair_analysis_results.append({
                "primary_ticket_id": primary_id,
                "primary_type": pair["primary_type"],
                "related_ticket_id": related_id,
                "pair_type": pair_type,
                "error": str(e)
            })
    
    # Step 2: Compile issues across all analyzed pairs
    logging.info("Running Phase 2 Step 2: Compiling Issues Index")
    issues_index = {
        "chain_hash": chain_hash,
        "issues_by_pair": [],
        "issues_by_type": {
            "scheduling": [],
            "technical": [],
            "customer": [],
            "equipment": [],
            "other": []
        },
        "missing_data": []
    }
    
    for pair_result in pair_analysis_results:
        primary_id = pair_result["primary_ticket_id"]
        related_id = pair_result["related_ticket_id"]
        result_data = pair_result.get("result", {})
        
        if isinstance(result_data, dict) and not result_data.get("error"):
            # Extract issues from this pair's analysis
            pair_issues = {
                "primary_ticket_id": primary_id,
                "related_ticket_id": related_id,
                "issues": []
            }
            
            # Add issues encountered
            issues_encountered = result_data.get("issues_encountered", [])
            for issue in issues_encountered:
                if isinstance(issue, dict):
                    pair_issues["issues"].append(issue)
                    
                    # Categorize the issue
                    issue_desc = issue.get("description", "").lower() if issue.get("description") else ""
                    if any(kw in issue_desc for kw in ["schedule", "time", "date", "appointment", "cancel"]):
                        issues_index["issues_by_type"]["scheduling"].append(issue)
                    elif any(kw in issue_desc for kw in ["technical", "network", "config", "install", "error"]):
                        issues_index["issues_by_type"]["technical"].append(issue)
                    elif any(kw in issue_desc for kw in ["customer", "client", "site access"]):
                        issues_index["issues_by_type"]["customer"].append(issue)
                    elif any(kw in issue_desc for kw in ["equipment", "material", "device", "hardware"]):
                        issues_index["issues_by_type"]["equipment"].append(issue)
                    else:
                        issues_index["issues_by_type"]["other"].append(issue)
            
            # Add missing information
            missing_info = result_data.get("missing_information", [])
            if missing_info:
                pair_issues["missing_data"] = missing_info
                issues_index["missing_data"].extend(missing_info)
            
            issues_index["issues_by_pair"].append(pair_issues)
    
    final_responses["stages"]["Issues_Index"] = issues_index
    print("\n--- Issues Index ---")
    print(json.dumps(issues_index, indent=2))
    print("--------------------")
    
    # Step 3: User Input for specific questions
    logging.info("Running Phase 2 Step 3: User Input")
    try:
        user_question = input("\nEnter a specific question about the ticket chain (or 'no' to skip): ").strip()
        user_questions = {}
        if user_question.lower() != 'no' and user_question:
            user_questions = {"global_question": user_question}
            logging.info(f"User question: {user_question}")
            
            # Create a custom prompt for the user's question
            user_prompt = f"""
Analyze the ticket chain with ID {chain_hash} to answer this specific question:

{user_question}

Provide a detailed answer based on the data in the files. Only include information that is explicitly mentioned in the ticket data. If the information needed to answer the question isn't available, clearly state what's missing.

Respond with a JSON object with this structure:
{{
  "question": "{user_question}",
  "answer": "string",
  "evidence": [
    {{
      "ticket_id": "string",
      "content": "string",
      "source": "string (post/note)"
    }}
  ],
  "missing_information": [
    "string"
  ]
}}
"""
            
            try:
                user_response = run_assistant_query(client, thread.id, assistant_id, user_prompt)
                user_json = validate_response(user_response, expected_ticket_ids)
                user_questions["response"] = user_json if user_json else {"error": "Invalid JSON", "raw": user_response}
                
                print(f"\n--- Response to User Question ---")
                if user_json:
                    print(json.dumps(user_json, indent=2))
                else:
                    print(f"ERROR: Invalid JSON.\nRaw:\n{user_response}")
                print("----------------------------------")
            except Exception as e:
                logging.error(f"Error processing user question: {e}")
                user_questions["error"] = str(e)
        else:
            logging.info("User skipped custom question")
        
        final_responses["stages"]["User_Questions"] = user_questions
    except Exception as e:
        logging.error(f"Error getting user input: {e}")
        final_responses["stages"]["User_Questions"] = {"error": str(e)}
    
    # Step 4: Final Consolidated Report
    logging.info("Running Phase 2 Step 4: Final Consolidated Report")
    
    consolidation_prompt = f"""
Create a consolidated summary report for the ticket chain with ID {chain_hash} based on all the analysis performed.

Provide a comprehensive overview covering:
1. Overall timeline of events (all visits/contacts in chronological order)
2. Summary of work completed across all visits
3. Summary of technical specifications and configurations
4. Summary of all issues encountered and their resolutions
5. Current status of the service/project
6. Any follow-up actions required

Format the output as valid JSON matching this structure:
{{
  "chain_hash": "{chain_hash}",
  "timeline": [
    {{
      "date": "string (YYYY-MM-DD)",
      "ticket_id": "string",
      "event_type": "string",
      "description": "string"
    }}
  ],
  "work_summary": {{
    "overall_status": "string",
    "completion_percentage": number,
    "tasks_completed": [
      "string"
    ],
    "outstanding_tasks": [
      "string"
    ]
  }},
  "technical_summary": {{
    "equipment_installed": [
      "string"
    ],
    "configurations_performed": [
      "string"
    ],
    "materials_used": [
      "string"
    ]
  }},
  "issues_summary": [
    {{
      "issue_description": "string",
      "status": "string",
      "resolution": "string"
    }}
  ],
  "service_status": "string",
  "follow_up_actions": [
    "string"
  ],
  "data_quality_notes": [
    "string"
  ]
}}
"""
    
    try:
        consolidation_response = run_assistant_query(client, thread.id, assistant_id, consolidation_prompt)
        consolidated_json = validate_response(consolidation_response, expected_ticket_ids)
        final_responses["consolidated_report"] = consolidated_json if consolidated_json else {"error": "Invalid JSON", "raw": consolidation_response}
        
        print("\n--- Consolidated Final Report ---")
        if consolidated_json:
            print(json.dumps(consolidated_json, indent=2))
        else:
            print(f"ERROR: Invalid JSON.\nRaw:\n{consolidation_response}")
        print("-------------------------------")
    except Exception as e:
        logging.error(f"Consolidation failed: {e}")
        final_responses["consolidated_report"] = {"error": str(e)}

    # Save Output
    final_output_file = f"PyChain/data/analyses/Phase2_TurnupPairs_{chain_hash}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    try:
        with open(final_output_file, 'w') as f:
            json.dump(final_responses, f, indent=2)
        logging.info(f"Saved Phase 2 results to {final_output_file}")
    except Exception as e:
        logging.error(f"Failed to save Phase 2 JSON: {e}")
    except KeyboardInterrupt:
        logging.info("User interrupted Phase 2 analysis")
        print("\nPhase 2 analysis interrupted by user.")
    except Exception as e:
        logging.error(f"Critical error in Phase 2: {e}")
    finally:
        if session:
            try:
                session.close()
                logging.info("Primary database session closed")
            except Exception as e:
                logging.error(f"Error closing primary session: {e}")
        if cissdm_session:
            try:
                cissdm_session.close()
                logging.info("CISSDM database session closed")
            except Exception as e:
                logging.error(f"Error closing CISSDM session: {e}")
        for file_path in file_paths_created:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logging.info(f"Removed local file {file_path}")
            except Exception as e:
                logging.warning(f"Error removing file {file_path}: {e}")
        if file_ids_uploaded:
            try:
                cleanup_response = input("\nClean up OpenAI resources (files)? (y/n): ").strip().lower()
                if cleanup_response == 'y':
                    delete_store = input("Delete vector store? (y/n): ").strip().lower() == 'y'
                    cleanup_openai_resources(client, file_ids_uploaded, vector_store_id, delete_store)
            except KeyboardInterrupt:
                logging.info("User interrupted during cleanup prompt")
                print("\nCleanup interrupted by user. Resources may not be fully cleaned up.")
            except Exception as e:
                logging.error(f"Error during cleanup prompt: {e}")

def analyze_real_ticket(ticket_id: str, phase: str = "all"):
    """Analyze a ticket chain starting from the given ticket ID."""
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

        # Remove duplicates
        unique_tickets = {t['ticket_id']: t for t in chain_details.get('tickets', [])}.values()
        chain_details['tickets'] = list(unique_tickets)

        print("\nTicket Chain Details")
        print("------------------------------")
        print(f"Chain Hash: {chain_details.get('chain_hash')}")
        print(f"Number of Tickets: {len(chain_details.get('tickets', []))}\n")
        
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
        
        for category, tickets in category_groups.items():
            if tickets:
                print(f"{category} Tickets: {len(tickets)}")
                for ticket in tickets:
                    print(f"  - {ticket.get('ticket_id')}: {ticket.get('subject')}")
        print()

        ai_service = AIService()

        result = ""
        if phase in ["all", "phase1"]:
            logging.info(f"Running Phase 1 for chain {chain_details.get('chain_hash')}")
            prompt = f"""
            Analyze the ticket chain with chain hash {chain_details.get('chain_hash')} containing {len(chain_details.get('tickets', []))} tickets, starting with ticket ID {ticket_id}. The chain includes:
            {json.dumps(chain_details, indent=2, cls=DateTimeEncoder)}
            Provide a summary of relationships between all tickets in the chain, including:
            1. Parent-child relationships (use linked_tickets field, if available)
            2. Dispatch-turnup-billing linkages
            3. Any orphaned or unlinked records
            Extract specific details from posts and notes for each ticket, including:
            - Cable drop counts (e.g., 'Qty. X')
            - Completion status (e.g., 'Closing', 'Completed')
            - Revisit requirements (e.g., 'reschedule', 'pending')
            - Delays (e.g., 'did not ship', 'on hold')
            Cite ticket IDs and specific post/note details where applicable.
            """
            result = ai_service.analyze_chain(prompt, report_type="relationship_summary", provider="openai")
            
            print("\n==================================================")
            print("ANALYSIS: Ticket Relationship and Summary (Phase 1)")
            print("==================================================")
            logging.info("Phase 1 analysis complete")
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
            logging.info(f"Phase 1 Analysis saved to {analysis_file}")
        
        if phase in ["all", "phase2"]:
            proceed = input("\nRun Phase 2 analysis (detailed multi-stage)? (y/n): ").lower()
            if proceed != 'y':
                logging.info("Skipping Phase 2 analysis")
                return
            
            if not openai_client:
                logging.error("OpenAI client not initialized. Check OPENAI_API_KEY in .env")
                return
            
            run_phase_2_analysis(openai_client, chain_details, result if phase in ["all", "phase1"] else "")

    finally:
        if session:
            logging.info("Primary database session closed")
            session.close()

def create_ticket_pairs(full_ticket_data):
    """Create turnup-dispatch ticket pairs for analysis.
    
    This function identifies turnup tickets and pairs them with their related dispatch tickets.
    Turnup tickets are the primary focus since they typically contain the details of actual visits.
    """
    # Identify turnup and dispatch tickets
    turnup_tickets = {}
    dispatch_tickets = {}
    
    for tid, data in full_ticket_data.items():
        if data.get('category') == 'Turnup Tickets':
            turnup_tickets[tid] = data
        elif data.get('category') == 'Dispatch Tickets':
            dispatch_tickets[tid] = data
    
    # Create dispatch-to-turnup relationship mapping
    dispatch_to_turnups = {}
    turnup_to_dispatch = {}
    
    # Map turnups to their parent dispatch tickets
    for turnup_id, turnup_data in turnup_tickets.items():
        if turnup_data.get('parent_dispatch_id'):
            dispatch_id = str(turnup_data['parent_dispatch_id'])
            # Add to dispatch_to_turnups map
            if dispatch_id not in dispatch_to_turnups:
                dispatch_to_turnups[dispatch_id] = []
            if turnup_id not in dispatch_to_turnups[dispatch_id]:
                dispatch_to_turnups[dispatch_id].append(turnup_id)
            
            # Add to turnup_to_dispatch map
            turnup_to_dispatch[turnup_id] = dispatch_id
    
    # Create turnup-dispatch pairs for analysis
    ticket_pairs = []
    
    # First, handle all turnup tickets with a known related dispatch
    for turnup_id, dispatch_id in turnup_to_dispatch.items():
        if dispatch_id in dispatch_tickets:
            ticket_pairs.append({
                "primary_ticket_id": turnup_id,
                "primary_type": "turnup",
                "related_ticket_id": dispatch_id,
                "related_type": "dispatch",
                "pair_type": "turnup_with_dispatch"
            })
        else:
            # Turnup with missing dispatch ticket
            ticket_pairs.append({
                "primary_ticket_id": turnup_id,
                "primary_type": "turnup",
                "related_ticket_id": None,
                "related_type": None,
                "pair_type": "turnup_only"
            })
    
    # Next, handle any turnup tickets without a known dispatch relationship
    for turnup_id in turnup_tickets:
        if turnup_id not in turnup_to_dispatch:
            ticket_pairs.append({
                "primary_ticket_id": turnup_id,
                "primary_type": "turnup",
                "related_ticket_id": None,
                "related_type": None,
                "pair_type": "turnup_only"
            })
    
    # Finally, handle any dispatch tickets without a known turnup relationship
    for dispatch_id in dispatch_tickets:
        if dispatch_id not in dispatch_to_turnups:
            ticket_pairs.append({
                "primary_ticket_id": dispatch_id,
                "primary_type": "dispatch",
                "related_ticket_id": None,
                "related_type": None,
                "pair_type": "dispatch_only"
            })
    
    # Calculate statistics for logging
    turnup_with_dispatch_count = sum(1 for pair in ticket_pairs if pair["pair_type"] == "turnup_with_dispatch")
    turnup_only_count = sum(1 for pair in ticket_pairs if pair["pair_type"] == "turnup_only")
    dispatch_only_count = sum(1 for pair in ticket_pairs if pair["pair_type"] == "dispatch_only")
    
    logging.info(f"Created {len(ticket_pairs)} ticket pairs for analysis:")
    logging.info(f"  - Turnup tickets with dispatch: {turnup_with_dispatch_count}")
    logging.info(f"  - Turnup tickets without dispatch: {turnup_only_count}")
    logging.info(f"  - Dispatch tickets without turnup: {dispatch_only_count}")
    
    return ticket_pairs

def create_ticket_pair_analysis_prompt(chain_hash, ticket_pair, full_ticket_data):
    """Create a prompt for analyzing a turnup-dispatch ticket pair.
    
    This function generates a detailed prompt that combines data from both the turnup and dispatch
    tickets to provide comprehensive context for AI analysis.
    """
    primary_id = ticket_pair["primary_ticket_id"]
    related_id = ticket_pair["related_ticket_id"]
    primary_data = full_ticket_data.get(primary_id, {})
    related_data = full_ticket_data.get(related_id, {}) if related_id else {}
    
    # Start with header information
    if ticket_pair["pair_type"] == "turnup_with_dispatch":
        title = f"Turnup Ticket {primary_id} and related Dispatch Ticket {related_id}"
    elif ticket_pair["pair_type"] == "turnup_only":
        title = f"Turnup Ticket {primary_id} (No related dispatch found)"
    elif ticket_pair["pair_type"] == "dispatch_only":
        title = f"Dispatch Ticket {primary_id} (No related turnup found)"
    else:
        title = f"Ticket {primary_id}"
    
    prompt = f"""
Analyze the following ticket pair in chain {chain_hash}:
{title}

"""
    
    # Add primary ticket information
    prompt += f"""
PRIMARY TICKET ({ticket_pair["primary_type"].upper()} {primary_id}):
- Subject: {primary_data.get('subject', 'N/A')}
- Status: {primary_data.get('status', 'N/A')}
- Category: {primary_data.get('category', 'N/A')}
- Created: {primary_data.get('created_date', 'N/A')}
- Last Activity: {primary_data.get('last_activity_date', 'N/A')}
- Closed: {primary_data.get('closed_date', 'N/A')}
- Site: {primary_data.get('site', {}).get('address', 'N/A')}, {primary_data.get('site', {}).get('city', 'N/A')}, {primary_data.get('site', {}).get('state', 'N/A')}
"""

    # Add primary ticket's first post content if available
    if primary_data.get('first_post'):
        prompt += f"\nPrimary Ticket First Post:\n{primary_data.get('first_post')[:500]}...\n"
    
    # Add primary ticket's posts
    posts = primary_data.get('interactions', {}).get('posts', [])
    if posts:
        prompt += f"\nPrimary Ticket Posts ({len(posts)}):\n"
        for i, post in enumerate(posts[:8]):  # Limit to 8 posts
            content = post.get('content', '')
            if len(content) > 300:
                content = content[:300] + "..."
            prompt += f"  Post {i+1} ({post.get('timestamp', 'N/A')} by {post.get('author', 'N/A')}):\n  {content}\n\n"
    
    # Add primary ticket's notes
    notes = primary_data.get('interactions', {}).get('notes', [])
    if notes:
        prompt += f"\nPrimary Ticket Notes ({len(notes)}):\n"
        for i, note in enumerate(notes[:5]):  # Limit to 5 notes
            content = note.get('content', '')
            if len(content) > 300:
                content = content[:300] + "..."
            prompt += f"  Note {i+1} ({note.get('timestamp', 'N/A')} by {note.get('author', 'N/A')}):\n  {content}\n\n"
    
    # Add related ticket information (if exists)
    if related_id:
        prompt += f"""
RELATED TICKET ({ticket_pair["related_type"].upper()} {related_id}):
- Subject: {related_data.get('subject', 'N/A')}
- Status: {related_data.get('status', 'N/A')}
- Category: {related_data.get('category', 'N/A')}
- Created: {related_data.get('created_date', 'N/A')}
- Last Activity: {related_data.get('last_activity_date', 'N/A')}
- Closed: {related_data.get('closed_date', 'N/A')}
"""

        # Add related ticket's first post content if available
        if related_data.get('first_post'):
            prompt += f"\nRelated Ticket First Post:\n{related_data.get('first_post')[:500]}...\n"
        
        # Add related ticket's posts (fewer than primary)
        related_posts = related_data.get('interactions', {}).get('posts', [])
        if related_posts:
            prompt += f"\nRelated Ticket Posts ({len(related_posts)}):\n"
            for i, post in enumerate(related_posts[:5]):  # Limit to 5 posts for related ticket
                content = post.get('content', '')
                if len(content) > 300:
                    content = content[:300] + "..."
                prompt += f"  Post {i+1} ({post.get('timestamp', 'N/A')} by {post.get('author', 'N/A')}):\n  {content}\n\n"
        
        # Add any important notes from the related ticket
        related_notes = related_data.get('interactions', {}).get('notes', [])
        if related_notes:
            prompt += f"\nRelated Ticket Notes ({len(related_notes)}):\n"
            for i, note in enumerate(related_notes[:3]):  # Limit to 3 notes for related ticket
                content = note.get('content', '')
                if len(content) > 300:
                    content = content[:300] + "..."
                prompt += f"  Note {i+1} ({note.get('timestamp', 'N/A')} by {note.get('author', 'N/A')}):\n  {content}\n\n"
    
    # Add any additional turnup or dispatch specific data
    if ticket_pair["primary_type"] == "turnup":
        turnup_data = primary_data.get('related_data', {}).get('turnup_data')
        if turnup_data:
            prompt += f"\nTurnup Data (Primary Ticket):\n"
            prompt += f"  - Status: {turnup_data.get('status', 'N/A')}\n"
            prompt += f"  - Service Date: {turnup_data.get('service_date', 'N/A')}\n"
            prompt += f"  - Technician: {turnup_data.get('technician_name', 'N/A')}\n"
            if turnup_data.get('notes'):
                prompt += f"  - Notes: {turnup_data.get('notes', 'N/A')[:300]}...\n"
    
    if ticket_pair["primary_type"] == "dispatch" or (related_id and ticket_pair["related_type"] == "dispatch"):
        dispatch_data = None
        if ticket_pair["primary_type"] == "dispatch":
            dispatch_data = primary_data.get('related_data', {}).get('dispatch_data')
            dispatch_label = "Primary Ticket"
        else:
            dispatch_data = related_data.get('related_data', {}).get('dispatch_data')
            dispatch_label = "Related Ticket"
        
        if dispatch_data:
            prompt += f"\nDispatch Data ({dispatch_label}):\n"
            prompt += f"  - Status: {dispatch_data.get('status', 'N/A')}\n"
            prompt += f"  - Type: {dispatch_data.get('type', 'N/A')}\n"
            prompt += f"  - Service Date: {dispatch_data.get('service_date', 'N/A')}\n"
            prompt += f"  - Service Time: {dispatch_data.get('service_time', 'N/A')}\n"
            prompt += f"  - Priority: {dispatch_data.get('priority', 'N/A')}\n"
            prompt += f"  - Customer: {dispatch_data.get('customer_name', 'N/A')}\n"
    
    # Add any issues from either ticket
    primary_issues = primary_data.get('issues', [])
    related_issues = related_data.get('issues', []) if related_id else []
    all_issues = primary_issues + related_issues
    
    if all_issues:
        prompt += f"\nIssues Identified:\n"
        for issue in all_issues:
            prompt += f"  - {issue}\n"
    
    # Add missing data notes if any
    primary_missing_notes = primary_data.get('details', {}).get('missing_data_notes', [])
    related_missing_notes = related_data.get('details', {}).get('missing_data_notes', []) if related_id else []
    
    if primary_missing_notes or related_missing_notes:
        prompt += f"\nMissing Data Notes:\n"
        for note in primary_missing_notes:
            prompt += f"  - Primary ticket: {note}\n"
        for note in related_missing_notes:
            prompt += f"  - Related ticket: {note}\n"
    
    # Add analysis instructions
    prompt += """
Based on both tickets' data, perform the following analysis:

1. FIELD SERVICE VISIT DETAILS:
   - When was the visit scheduled/completed? Extract exact dates and times
   - What specific tasks were performed? Be explicit about work completed
   - What equipment or materials were used?
   - What technical issues were encountered?
   - What was the outcome of the visit?

2. SERVICE/PROJECT LIFECYCLE TRACKING:
   - What stage of the overall project does this visit represent?
   - Were there prior visits? Will future visits be needed?
   - How does this visit connect to the broader service lifecycle?

3. ISSUE ANALYSIS:
   - What specific problems occurred during this service event?
   - Were they resolved on-site or need follow-up?
   - Extract details about causes and impacts of issues

4. KEY TECHNICAL SPECIFICATIONS:
   - What networking equipment was configured?
   - What cabling was installed/modified?
   - What specific technical work was completed?

IMPORTANT: Only include information explicitly mentioned in the tickets. If information is missing, state this clearly rather than making assumptions.

Provide output as structured JSON with this schema:
{
  "primary_ticket_id": "string",
  "primary_ticket_type": "string (turnup/dispatch)",
  "related_ticket_id": "string or null",
  "pair_type": "string (turnup_with_dispatch/turnup_only/dispatch_only)",
  "visit_details": {
    "scheduled_date": "string or null",
    "actual_date": "string or null",
    "start_time": "string or null",
    "end_time": "string or null",
    "technicians": [],
    "status": "string",
    "was_completed": boolean,
    "cancellation_reason": "string or null"
  },
  "location_details": {
    "site_id": "string or null",
    "site_name": "string",
    "address": "string",
    "city": "string",
    "state": "string"
  },
  "tasks_performed": [
    {
      "description": "string",
      "status": "string",
      "notes": "string"
    }
  ],
  "technical_details": {
    "equipment_used": [],
    "materials_used": [],
    "configurations_performed": [],
    "network_changes": []
  },
  "issues_encountered": [
    {
      "description": "string",
      "resolution": "string",
      "impact": "string"
    }
  ],
  "service_lifecycle": {
    "current_stage": "string",
    "prior_visits": [],
    "future_visits_needed": boolean,
    "future_visit_reasons": []
  },
  "outcomes": {
    "visit_result": "string",
    "completion_percentage": number,
    "client_feedback": "string",
    "notes": "string"
  },
  "missing_information": []
}

Respond ONLY with valid JSON matching this schema.
"""
    return prompt

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze a ticket chain")
    parser.add_argument("--ticket", required=True, help="Ticket ID to analyze")
    parser.add_argument("--phase", choices=["all", "phase1", "phase2"], default="all", help="Analysis phase to run")
    args = parser.parse_args()
    
    analyze_real_ticket(args.ticket, args.phase)