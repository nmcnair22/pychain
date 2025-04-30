import argparse
import json
import logging
import sys
import os
import re
import time
import shutil
from datetime import datetime
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
        return super().default(obj)

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

def fetch_full_ticket_data(session, cissdm_session, ticket_ids):
    """Fetch detailed data for all tickets in chain."""
    if not ticket_ids:
        return {}
    
    ticket_data = {}
    
    try:
        # Create placeholders for SQL query
        placeholders = ','.join([f':tid_{i}' for i in range(len(ticket_ids))])
        
        # Primary table query (tickets)
        base_query = text(f"""
            SELECT
                t.ticketid, t.subject, t.ticketstatustitle, t.departmenttitle,
                FROM_UNIXTIME(t.dateline) AS created_date,
                FROM_UNIXTIME(t.lastactivity) AS last_activity_date,
                FROM_UNIXTIME(t.resolutiondateline) AS closed_date,
                t.totalreplies, t.locationid, t.isresolved
            FROM sw_tickets t
            WHERE t.ticketid IN ({placeholders})
        """)
        
        # Execute query with parameters
        params = {f'tid_{i}': int(tid) for i, tid in enumerate(ticket_ids)}
        results = session.execute(base_query, params).mappings().all()
        
        # Process all tickets in chain
        for row_mapping in results:
            row = dict(row_mapping)
            tid_str = str(row['ticketid'])
            
            # Initialize ticket_data structure
            ticket_data[tid_str] = {
                'ticket_id': tid_str,
                'subject': row['subject'],
                'status': row['ticketstatustitle'],
                'department': row['departmenttitle'],
                'created_date': str(row['created_date']),
                'last_activity_date': str(row['last_activity_date']),
                'closed_date': str(row['closed_date']) if row['closed_date'] else None,
                'is_resolved': bool(row['isresolved']),
                'is_dispatch': False,
                'is_turnup': False,
                'posts': [],
                'notes': [],
                'user_posts': [],
                'timeline': [],
                'issues': [],
                'category': 'Unknown',
                'site': {
                    'number': 'N/A',
                    'address': 'N/A',
                    'city': 'N/A',
                    'state': 'N/A'
                }
            }
            
            # Categorize by department
            if row['departmenttitle'] == 'Dispatch Tickets':
                ticket_data[tid_str]['category'] = 'Dispatch Tickets'
                ticket_data[tid_str]['is_dispatch'] = True
            elif row['departmenttitle'] == 'Turnup Tickets':
                ticket_data[tid_str]['category'] = 'Turnup Tickets'
                ticket_data[tid_str]['is_turnup'] = True
            
        # Get posts for each ticket
        for tid in ticket_ids:
            tid_str = str(tid)
            if tid_str not in ticket_data:
                continue
                
            posts_query = text("""
                SELECT ticketpostid, dateline AS post_dateline, userid, fullname, contents
                FROM sw_ticketposts
                WHERE ticketid = :ticket_id
                ORDER BY dateline
            """)
            posts_results = session.execute(posts_query, {'ticket_id': int(tid)}).mappings().all()
            
            for post_row in posts_results:
                post_dict = dict(post_row)
                post_time = datetime.fromtimestamp(post_dict['post_dateline'])
                
                # Add to main posts list
                post_data = {
                    'id': post_dict['ticketpostid'],
                    'time': str(post_time),
                    'user': post_dict['fullname'],
                    'user_id': post_dict['userid'],
                    'contents': post_dict['contents'],
                    'is_staff': True if post_dict['userid'] != 0 else False
                }
                ticket_data[tid_str]['posts'].append(post_data)
                
                # Add user posts to separate list
                if post_dict['userid'] == 0:
                    ticket_data[tid_str]['user_posts'].append(post_data)
                
                # Add to unified timeline
                ticket_data[tid_str]['timeline'].append({
                    'time': str(post_time),
                    'type': 'post',
                    'user': post_dict['fullname'],
                    'content': post_dict['contents']
                })
            
            # First and last post shortcuts
            if ticket_data[tid_str]['posts']:
                ticket_data[tid_str]['first_post'] = ticket_data[tid_str]['posts'][0]['contents']
                ticket_data[tid_str]['last_post'] = ticket_data[tid_str]['posts'][-1]['contents']
            
            # Get notes for each ticket
            notes_query = text("""
                SELECT ticketnoteid, linktypeid, dateline AS note_dateline, staffname, note
                FROM sw_ticketnotes
                WHERE linktypeid = :ticket_id
                ORDER BY note_dateline
            """)
            notes_results = session.execute(notes_query, {'ticket_id': int(tid)}).mappings().all()
            
            for note_row in notes_results:
                note_dict = dict(note_row)
                note_time = datetime.fromtimestamp(note_dict['note_dateline'])
                
                note_data = {
                    'id': note_dict['ticketnoteid'],
                    'time': str(note_time),
                    'user': note_dict['staffname'],
                    'contents': note_dict['note']
                }
                ticket_data[tid_str]['notes'].append(note_data)
                
                # Add to unified timeline
                ticket_data[tid_str]['timeline'].append({
                    'time': str(note_time),
                    'type': 'note',
                    'user': note_dict['staffname'],
                    'content': note_dict['note']
                })
        
        # Sort timeline for each ticket
        for tid_str in ticket_data:
            ticket_data[tid_str]['timeline'].sort(key=lambda x: x['time'])
        
        if cissdm_session:
            # Dispatch Tickets from CISSDM
            dispatch_ids = [tid for tid, data in ticket_data.items() if data['category'] == 'Dispatch Tickets']
            if dispatch_ids:
                dispatch_placeholders = ','.join([f':tid_{i}' for i in range(len(dispatch_ids))])
                dispatch_query = text(f"""
                    SELECT
                        id, id_turnup, id_wo, id_customer, customername, subject,
                        statusDispatch, statusTurnup, ticketType, serviceDate, serviceTime,
                        ticketPriority, postFirstDetails, postLastDetails, dateCreated,
                        department, projectId, billableRate, FSTHourlyCosts, FSTHourlyCostsToCustomer,
                        FSTFinalBilledToCIS, siteNumber, created_at, updated_at
                    FROM dispatches WHERE id IN ({dispatch_placeholders})
                """)
                params = {f'tid_{i}': tid for i, tid in enumerate(dispatch_ids)}
                dispatch_results = cissdm_session.execute(dispatch_query, params).mappings().all()
                
                for row_mapping in dispatch_results:
                    row = dict(row_mapping)
                    tid_str = str(row['id'])
                    if tid_str in ticket_data:
                        ticket_data[tid_str]['dispatch_data'] = {
                            'status': row.get('statusDispatch', 'N/A'),
                            'type': row.get('ticketType', 'N/A'),
                            'service_date': str(row.get('serviceDate')) if row.get('serviceDate') else 'N/A',
                            'service_time': row.get('serviceTime', 'N/A'),
                            'priority': row.get('ticketPriority', 'N/A'),
                            'customer_name': row.get('customername', 'N/A'),
                            'wo_id': row.get('id_wo', 'N/A'),
                            'turnup_id': row.get('id_turnup', 'N/A'),
                            'site_number': row.get('siteNumber', 'N/A'),
                            'project_id': row.get('projectId', 'N/A'),
                            'location': {
                                'address': row.get('location_address', 'N/A') if 'location_address' in row else 'N/A',
                                'city': row.get('location_city', 'N/A') if 'location_city' in row else 'N/A',
                                'state': row.get('location_state', 'N/A') if 'location_state' in row else 'N/A',
                                'zipcode': row.get('location_zipcode', 'N/A') if 'location_zipcode' in row else 'N/A',
                                'phone': row.get('location_phone', 'N/A') if 'location_phone' in row else 'N/A',
                                'timezone': row.get('location_timezone', 'N/A') if 'location_timezone' in row else 'N/A'
                            }
                        }
                        if row.get('location_city') and (row.get('location_city') != 'Hagerstown' or row.get('location_state') != 'MD'):
                            ticket_data[tid_str]['issues'].append(f"Dispatch location mismatch: {row.get('location_city')}, {row.get('location_state')} vs. Hagerstown, MD")
            
            # Turnup Tickets
            turnup_ids = [tid for tid, data in ticket_data.items() if data['category'] == 'Turnup Tickets']
            if turnup_ids:
                turnup_placeholders = ','.join([f':tid_{i}' for i in range(len(turnup_ids))])
                turnup_query = text(f"""
                    SELECT 
                        ticketid, 
                        DispatchId, 
                        subject, 
                        ticketstatustitle AS turnup_status, 
                        ServiceDate, 
                        CISTechnicianName,
                        InTime, 
                        OutTime, 
                        TurnupNotes, 
                        DispatchNotes, 
                        technicianGrade, 
                        technicianComment,
                        FailureCode, 
                        FailureCodeOther, 
                        pmreview, 
                        closeOutNotes, 
                        brief_summary_for_invoice,
                        isresolved, 
                        created_at AS turnup_created, 
                        last_activity AS turnup_last_activity, 
                        updated_at AS turnup_updated, 
                        closed_at AS turnup_closed, 
                        SiteNumber
                    FROM turnups 
                    WHERE ticketid IN ({turnup_placeholders})
                """)
                params = {f'tid_{i}': tid for i, tid in enumerate(turnup_ids)}
                turnup_results = cissdm_session.execute(turnup_query, params).mappings().all()
                for row_mapping in turnup_results:
                    row = dict(row_mapping)
                    tid_str = str(row.get('ticketid'))
                    if tid_str in ticket_data:
                        ticket_data[tid_str]['parent_dispatch_id'] = str(row.get('DispatchId')) if row.get('DispatchId') else None
                        ticket_data[tid_str]['turnup_data'] = {
                            'status': row.get('turnup_status', 'N/A'),
                            'service_date': str(row.get('ServiceDate')) if row.get('ServiceDate') else 'N/A',
                            'technician_name': row.get('CISTechnicianName', 'N/A'),
                            'in_time': row.get('InTime', 'N/A'),
                            'out_time': row.get('OutTime', 'N/A'),
                            'duration': None,
                            'notes': row.get('TurnupNotes', 'N/A'),
                            'dispatch_notes': row.get('DispatchNotes', 'N/A'),
                            'failure_code': row.get('FailureCode', 'N/A'),
                            'failure_code_other': row.get('FailureCodeOther', 'N/A'),
                            'is_resolved': bool(row.get('isresolved', False)),
                            'created_date': str(row.get('turnup_created')) if row.get('turnup_created') else 'N/A',
                            'last_activity_date': str(row.get('turnup_last_activity')) if row.get('turnup_last_activity') else 'N/A',
                            'closed_date': str(row.get('turnup_closed')) if row.get('turnup_closed') else 'N/A'
                        }
                        if row.get('InTime') and row.get('OutTime'):
                            try:
                                in_time = datetime.strptime(row.get('InTime'), '%H:%M:%S')
                                out_time = datetime.strptime(row.get('OutTime'), '%H:%M:%S')
                                duration = (out_time - in_time).total_seconds() / 60
                                ticket_data[tid_str]['turnup_data']['duration'] = f"{duration:.2f} minutes"
                            except ValueError:
                                pass
                        if not row.get('InTime') or not row.get('OutTime'):
                            ticket_data[tid_str]['issues'].append("Missing visit times (InTime/OutTime)")
                        if row.get('FailureCode'):
                            ticket_data[tid_str]['issues'].append(f"Failed/Cancelled: {row.get('FailureCode')} - {row.get('FailureCodeOther', '')}")
                        if row.get('ServiceDate') and '1969-12-31' in str(row.get('ServiceDate')):
                            ticket_data[tid_str]['issues'].append("Epoch turnup service date (1969-12-31), indicating data error")

    except Exception as e:
        logging.error(f"Error fetching from primary database: {e}")
        raise

    # Detect orphaned turnups and non-1:1 relationships
    dispatch_to_turnups = {}
    for tid, data in ticket_data.items():
        if data.get('category') == 'Turnup Tickets':
            if not data.get('parent_dispatch_id'):
                data['issues'].append("Orphaned turnup, no linked dispatch")
            else:
                dispatch_id = data['parent_dispatch_id']
                if dispatch_id not in dispatch_to_turnups:
                    dispatch_to_turnups[dispatch_id] = []
                dispatch_to_turnups[dispatch_id].append(tid)
    for dispatch_id, turnup_ids in dispatch_to_turnups.items():
        if len(turnup_ids) > 1 and dispatch_id in ticket_data:
            ticket_data[dispatch_id]['issues'].append(f"Non-1:1 relationship, linked to {len(turnup_ids)} turnups: {', '.join(turnup_ids)}")

    logging.info(f"Fetched details for {len(ticket_data)} tickets")
    return ticket_data

def setup_vector_store_and_assistant(client: OpenAI, ticket_files: list[str]):
    """Set up or reuse vector store and assistant, updating .env if needed."""
    try:
        vector_store_id = os.getenv("VECTOR_STORE_ID")
        assistant_id = os.getenv("ASSISTANT_ID")
        vector_store_created = False
        assistant_created = False

        # Vector Store Handling
        if not vector_store_id:
            logging.info("No VECTOR_STORE_ID found, creating new vector store...")
            try:
                vector_store = client.vector_stores.create(
                    name=f"Ticket Analysis Store - {datetime.now():%Y%m%d-%H%M%S}"
                )
            except AttributeError:
                vector_store = client.beta.vector_stores.create(
                    name=f"Ticket Analysis Store - {datetime.now():%Y%m%d-%H%M%S}"
                )
            vector_store_id = vector_store.id
            vector_store_created = True
            logging.info(f"Created vector store with ID: {vector_store_id}")
        else:
            logging.info(f"Using existing vector store ID: {vector_store_id}")
            try:
                client.vector_stores.retrieve(vector_store_id)
                logging.info(f"Vector store {vector_store_id} verified.")
            except Exception as e:
                logging.error(f"Failed to verify vector store {vector_store_id}: {e}")
                raise

        # Assistant Handling
        if not assistant_id:
            logging.info("No ASSISTANT_ID found, creating new assistant...")
            try:
                assistant = client.assistants.create(
                    name="Ticket Analysis Assistant",
                    instructions="You are an expert in analyzing ticket chains for field service operations. Analyze ticket data from uploaded files, extracting detailed metrics (e.g., timeline, scope, outcome, revisits, delays) and issues. Provide structured JSON responses, citing specific ticket data (e.g., posts, notes) as evidence. Do not assume data beyond what's provided.",
                    model="gpt-4o",
                    tools=[{"type": "file_search"}],
                    tool_resources={"file_search": {"vector_store_ids": [vector_store_id]}}
                )
            except AttributeError:
                assistant = client.beta.assistants.create(
                    name="Ticket Analysis Assistant",
                    instructions="You are an expert in analyzing ticket chains for field service operations. Analyze ticket data from uploaded files, extracting detailed metrics (e.g., timeline, scope, outcome, revisits, delays) and issues. Provide structured JSON responses, citing specific ticket data (e.g., posts, notes) as evidence. Do not assume data beyond what's provided.",
                    model="gpt-4o",
                    tools=[{"type": "file_search"}],
                    tool_resources={"file_search": {"vector_store_ids": [vector_store_id]}}
                )
            assistant_id = assistant.id
            assistant_created = True
            logging.info(f"Created assistant with ID: {assistant_id}")
        else:
            logging.info(f"Using existing assistant ID: {assistant_id}")
            try:
                client.assistants.update(
                    assistant_id=assistant_id,
                    tool_resources={"file_search": {"vector_store_ids": [vector_store_id]}}
                )
                logging.info(f"Updated assistant {assistant_id} with vector store {vector_store_id}")
            except AttributeError:
                client.beta.assistants.update(
                    assistant_id=assistant_id,
                    tool_resources={"file_search": {"vector_store_ids": [vector_store_id]}}
                )
                logging.info(f"Updated assistant {assistant_id} with vector store {vector_store_id}")

        # Update .env file if new IDs were created
        if vector_store_created or assistant_created:
            try:
                env_file = ".env"
                lines = []
                if os.path.exists(env_file):
                    with open(env_file, "r") as f:
                        lines = f.readlines()
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
                logging.info("Updated .env file with ASSISTANT_ID and/or VECTOR_STORE_ID")
            except Exception as e:
                logging.error(f"Error updating .env file: {e}")

        return vector_store_id, assistant_id
    except Exception as e:
        logging.error(f"Failed to set up vector store or assistant: {e}")
        if "invalid_api_key" in str(e).lower():
            logging.error("Invalid OpenAI API key. Please verify OPENAI_API_KEY in your .env file.")
        raise

def create_ticket_files(chain_details, full_ticket_data, phase1_analysis_text):
    """Create JSON files for ticket data, chain metadata, and analysis with standardized structure for AI ingestion."""
    output_dir = 'PyChain/data/ticket_files'
    os.makedirs(output_dir, exist_ok=True)
    chain_hash = chain_details['chain_hash']
    file_paths = []

    # Chain metadata
    chain_file = os.path.join(output_dir, f'chain_{chain_hash}.json')
    try:
        chain_meta = {
             "chain_hash": chain_hash,
            "ticket_count": chain_details.get('ticket_count', len(chain_details.get('tickets', []))),
            "ticket_ids": list(full_ticket_data.keys())
        }
        with open(chain_file, 'w') as f:
            json.dump(chain_meta, f, indent=2)
        file_paths.append(chain_file)
        logging.info(f"Created chain metadata file: {chain_file}")
    except Exception as e:
        logging.error(f"Error creating chain metadata file {chain_file}: {e}")

    # Consolidated ticket data with standardized structure
    tickets_file = os.path.join(output_dir, f'tickets_{chain_hash}.json')
    try:
        # Standardize the ticket data structure for AI ingestion
        standardized_ticket_data = {}
        for ticket_id, data in full_ticket_data.items():
            standardized_ticket_data[ticket_id] = {
                "basic_info": {
                    "ticket_id": data.get('ticket_id', 'N/A'),
                    "subject": data.get('subject', 'N/A'),
                    "status": data.get('status', 'N/A'),
                    "department": data.get('department', 'N/A'),
                    "category": data.get('category', 'N/A'),
                    "queue": data.get('queue', 'N/A'),
                    "audit_status": data.get('audit_status', 'N/A'),
                    "parent_dispatch_id": data.get('parent_dispatch_id', 'N/A'),
                    "location_id": data.get('location_id', 'N/A'),
                    "site_number": data.get('site_number', 'N/A') if 'site_number' in data else 'N/A',
                    "project_id": data.get('project_id', 'N/A') if 'project_id' in data else 'N/A'
                },
                "timeline": {
                    "created_date": data.get('created_date', 'N/A'),
                    "last_activity_date": data.get('last_activity_date', 'N/A'),
                    "closed_date": data.get('closed_date', 'N/A'),
                    "service_date": data.get('turnup_data', {}).get('service_date', 'N/A') if 'turnup_data' in data else 'N/A'
                },
                "details": {
                    "total_replies": data.get('total_replies', 0),
                    "technical_details": data.get('technical_details', 'N/A'),
                    "issues": data.get('issues', []),
                    "accounting_details": data.get('accounting_details', None)
                },
                "interactions": {
                    "posts": data.get('posts', []),
                    "notes": data.get('notes', [])
                },
                "related_data": {
                    "dispatch_data": data.get('dispatch_data', None),
                    "turnup_data": data.get('turnup_data', None)
                }
            }

        with open(tickets_file, 'w') as f:
            json.dump(standardized_ticket_data, f, indent=2, default=str)
        file_paths.append(tickets_file)
        logging.info(f"Created standardized tickets file: {tickets_file}")
    except Exception as e:
        logging.error(f"Error creating tickets file {tickets_file}: {e}")

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
        relationships_data = {
          "chain_hash": chain_hash,
          "relationships": [
                {"dispatch_ticket_id": disp_id, "turnup_ticket_ids": turnups, "confidence": "High"}
                for disp_id, turnups in dispatch_to_turnups.items()
            ]
        }
        with open(relationships_file, 'w') as f:
            json.dump(relationships_data, f, indent=2)
        file_paths.append(relationships_file)
        logging.info(f"Created relationships file: {relationships_file}")
    except Exception as e:
        logging.error(f"Error creating relationships file {relationships_file}: {e}")

    # Context rules
    rules_src_path = "PyChain/Ticket_Records_Information_and_Rules.txt"
    rules_dest_path = os.path.join(output_dir, 'Ticket_Records_Information_and_Rules.txt')
    try:
        if os.path.exists(rules_src_path):
            shutil.copyfile(rules_src_path, rules_dest_path)
            file_paths.append(rules_dest_path)
            logging.info(f"Copied rules file: {rules_dest_path}")
        else:
            logging.warning(f"Rules file not found at {rules_src_path}")
    except Exception as e:
        logging.error(f"Error copying rules file: {e}")

    return file_paths

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

def wait_for_vector_store_processing(client, vector_store_id, file_ids, timeout=300):
    """Wait for files to be processed by the vector store."""
    start_time = time.time()
    processed_files = set()
    all_files = set(file_ids)
    
    while time.time() - start_time < timeout:
        if len(all_files) == 0:
            logging.info("No files to process")
            return True
        all_processed = True
        for file_id in list(all_files - processed_files):
            try:
                file_status = client.vector_stores.files.retrieve(vector_store_id=vector_store_id, file_id=file_id)
                if file_status.status == 'completed':
                    processed_files.add(file_id)
                elif file_status.status in ['failed', 'cancelled']:
                    logging.error(f"File {file_id} failed: {file_status.status}")
                    all_files.remove(file_id)
                else:
                    all_processed = False
            except Exception as e:
                logging.error(f"Error checking file {file_id}: {e}")
                all_processed = False
        if all_processed:
            logging.info("All files processed")
            return True
        logging.info(f"Waiting for {len(all_files - processed_files)} files... ({int(time.time() - start_time)}s)")
        time.sleep(5)
    logging.error(f"Timeout after {timeout}s. Unprocessed files: {all_files - processed_files}")
    return False

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
    
    try:
        message = client.beta.threads.messages.create(
            thread_id=thread_id,
            role="user",
            content=prompt
        )
    except AttributeError:
        message = client.threads.messages.create(
            thread_id=thread_id,
            role="user",
            content=prompt
        )
    
    try:
        run = client.beta.threads.runs.create(
            thread_id=thread_id,
            assistant_id=assistant_id
        )
    except AttributeError:
        run = client.threads.runs.create(
            thread_id=thread_id,
            assistant_id=assistant_id
        )
    
    logging.info(f"Run {run.id} started")
    
    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        try:
            run_status = client.beta.threads.runs.retrieve(thread_id=thread_id, run_id=run.id)
        except AttributeError:
            run_status = client.threads.runs.retrieve(thread_id=thread_id, run_id=run.id)
        
        if run_status.status == "completed":
            logging.info(f"Run {run.id} completed")
            break
        elif run_status.status in ["failed", "cancelled", "expired"]:
            logging.error(f"Run {run.id} {run_status.status}")
            raise Exception(f"Run {run.id} {run_status.status}")
        elif run_status.status == "requires_action":
            logging.warning(f"Run {run.id} requires action: {run_status.required_action}")
            time.sleep(15)
        else:
            logging.info(f"Waiting for run {run.id} (Status: {run_status.status})... ({int(time.time() - start_time)}s)")
            time.sleep(10)
    else:
        raise TimeoutError(f"Run {run.id} timed out after {timeout_seconds} seconds")

    try:
        messages = client.beta.threads.messages.list(thread_id=thread_id, order="desc", limit=5)
    except AttributeError:
        messages = client.threads.messages.list(thread_id=thread_id, order="desc", limit=5)
    
    for msg in messages.data:
        if msg.role == "assistant" and msg.run_id == run.id:
            content = msg.content[0].text.value
            return content
    
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
    prompt = f"""
Analyze ticket {ticket_id} in chain {chain_hash} to extract structured data from the provided notes and posts.

Ticket Data:
- Category: {ticket_data.get('category', 'N/A')}
- Subject: {ticket_data.get('subject', 'N/A')}
- Status: {ticket_data.get('status', 'N/A')}
- Created: {ticket_data.get('created_date', 'N/A')}
- Last Activity: {ticket_data.get('last_activity_date', 'N/A')}
- Closed: {ticket_data.get('closed_date', 'N/A')}
"""
    technical_details = ticket_data.get('technical_details', 'N/A')
    if len(technical_details) > 300:
        technical_details = technical_details[:300] + "..."
    prompt += f"- Technical Details: {technical_details}\n"
    prompt += f"- Posts: {len(ticket_data.get('posts', []))} entries (first few):\n"
    for i, post in enumerate(ticket_data.get('posts', [])[:5]):
        content = post.get('content', 'N/A')
        if len(content) > 150:
            content = content[:150] + "..."
        prompt += f"  Post {i+1} ({post.get('timestamp', 'N/A')} by {post.get('author', 'N/A')}): {content}\n"
    prompt += f"- Notes: {len(ticket_data.get('notes', []))} entries (first few):\n"
    for i, note in enumerate(ticket_data.get('notes', [])[:5]):
        content = note.get('content', 'N/A')
        if len(content) > 150:
            content = content[:150] + "..."
        prompt += f"  Note {i+1} ({note.get('timestamp', 'N/A')} by {note.get('author', 'N/A')}): {content}\n"
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

Provide output strictly as structured JSON matching the following schema. Use 'not specified' or empty arrays for fields with no data:
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
    "sla_compliance": "boolean"
  },
  "customer_feedback": {
    "rating": "number (1-5)",
    "comments": "string"
  },
  "tasks": [
    {
      "task_id": "string",
      "description": "string",
      "status": "string",
      "completed": "boolean",
      "notes": "string",
      "dependencies": ["string"]
    }
  ],
  "tasks_closeout": {
    "tasks_completed_percentage": "number (0-100)",
    "all_tasks_completed": "boolean",
    "total_tasks": "number",
    "tasks_failed": "number",
    "closeout_notes": "string"
  },
  "status": "string",
  "site_address": "string",
  "site_contact": "string",
  "customer_name": "string",
  "customer_signature": "string",
  "actual_duration_minutes": "number",
  "travel_time_minutes": "number",
  "materials_used": [
    {
      "item": "string",
      "quantity": "number",
      "part_number": "string"
    }
  ],
  "inventory": {
    "items": [
      {
        "item_id": "string",
        "description": "string",
        "quantity_used": "number",
        "stock_remaining": "number"
      }
    ]
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
    "total_issues": "number",
    "issues_resolved": "number",
    "issues_unresolved": "number",
    "revisits_triggered": "number",
    "closeout_notes": "string"
  },
  "resolutions": ["string"],
  "revisit_required": "boolean",
  "revisits_required": ["string"],
  "notes": "string",
  "attachments": [
    {
      "type": "string",
      "url": "string"
    }
  ],
  "financials": [
    {
      "cost_id": "string",
      "type": "string",
      "description": "string",
      "amount": "number",
      "notes": "string"
    }
  ],
  "financials_closeout": {
    "total_cost": "number",
    "cost_breakdown": {
      "Labor": "number",
      "Materials": "number",
      "Trip Charge": "number",
      "Tax": "number",
      "Equipment Rental": "number"
    },
    "tax_included": "boolean",
    "closeout_notes": "string"
  },
  "task_outcome": "string",
  "completion_percentage": "number (0-100)",
  "audit_trail": [
    {
      "timestamp": "string (YYYY-MM-DDTHH:MM:SSZ)",
      "change": "string",
      "user": "string"
    }
  ]
}
Output ONLY valid JSON matching this schema.
"""
    return prompt

def run_phase_2_analysis(client, chain_details, phase1_analysis_text):
    """Run multi-stage Phase 2 analysis with advanced NLP data extraction."""
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

    # Step 1: Advanced NLP Data Extraction
    logging.info("Running Phase 2 Step 1: Advanced NLP Data Extraction")
    nlp_extraction_results = []
    for ticket_id in expected_ticket_ids:
        logging.info(f"Extracting data for ticket {ticket_id}")
        ticket_data = full_ticket_data.get(ticket_id, {})
        nlp_prompt = create_nlp_extraction_prompt(chain_hash, ticket_id, ticket_data)
        try:
            nlp_response = run_assistant_query(client, thread.id, assistant_id, nlp_prompt)
            validated_json = validate_response(nlp_response, [ticket_id])
            nlp_extraction_results.append({
                "ticket_id": ticket_id,
                "result": validated_json if validated_json else {"error": "Invalid JSON", "raw": nlp_response}
            })
            final_responses["stages"][f"NLP_Extraction_Ticket_{ticket_id}"] = nlp_extraction_results[-1]
            print(f"\n--- NLP Extraction for Ticket {ticket_id} (JSON) ---")
            if validated_json:
                print(json.dumps(validated_json, indent=2))
            else:
                print(f"ERROR: Invalid JSON.\nRaw:\n{nlp_response}")
            print("-----------------------------------")
        except Exception as e:
            logging.error(f"NLP extraction for ticket {ticket_id} failed: {e}")
            nlp_extraction_results.append({"ticket_id": ticket_id, "error": str(e)})
            final_responses["stages"][f"NLP_Extraction_Ticket_{ticket_id}"] = {"error": str(e)}

    # Step 2: Batch Analysis (previously Step 1)
    logging.info("Running Phase 2 Step 2: Batch Analysis")
    batches = create_ticket_batches(full_ticket_data)
    batch_results = []
    ticket_ids_list_str = ", ".join(f'"{tid}"' for tid in expected_ticket_ids)
    for i, batch in enumerate(batches):
        logging.info(f"Analyzing Batch {i+1}/{len(batches)} with {len(batch['ticket_ids'])} tickets")
        batch_prompt = create_batch_analysis_prompt(chain_hash, batch, full_ticket_data)
        try:
            batch_response = run_assistant_query(client, thread.id, assistant_id, batch_prompt)
            validated_json = validate_response(batch_response, batch['ticket_ids'])
            batch_results.append({
                "batch_id": i+1,
                "ticket_ids": batch['ticket_ids'],
                "result": validated_json if validated_json else {"error": "Invalid JSON", "raw": batch_response}
            })
            final_responses["stages"][f"Batch_{i+1}_Analysis"] = batch_results[-1]
            print(f"\n--- Batch {i+1} Analysis Result (JSON) ---")
            if validated_json:
                print(json.dumps(validated_json, indent=2))
            else:
                print(f"ERROR: Invalid JSON.\nRaw:\n{batch_response}")
            print("-----------------------------------")
        except Exception as e:
            logging.error(f"Batch {i+1} analysis failed: {e}")
            batch_results.append({"batch_id": i+1, "ticket_ids": batch['ticket_ids'], "error": str(e)})
            final_responses["stages"][f"Batch_{i+1}_Analysis"] = {"error": str(e)}

    # Step 3: Issue Indexing (previously Step 2)
    logging.info("Running Phase 2 Step 3: Issue Indexing")
    issues_index = compile_issues_index(batch_results)
    final_responses["stages"]["Issues_Index"] = issues_index
    print("\n--- Issues Index ---")
    print(json.dumps(issues_index, indent=2))
    print("--------------------")

    # Step 4: Follow-up Questions (previously Step 3)
    logging.info("Running Phase 2 Step 4: Follow-up Questions")
    questions_prompt = create_questions_prompt(chain_hash, issues_index, ticket_ids_list_str)
    try:
        questions_response = run_assistant_query(client, thread.id, assistant_id, questions_prompt)
        questions_json = validate_response(questions_response, expected_ticket_ids)
        final_responses["stages"]["Followup_Questions"] = questions_json if questions_json else {"error": "Invalid JSON", "raw": questions_response}
        print("\n--- Follow-up Questions (JSON) ---")
        if questions_json:
            print(json.dumps(questions_json, indent=2))
        else:
            print(f"ERROR: Invalid JSON.\nRaw:\n{questions_response}")
        print("--------------------------------")
    except Exception as e:
        logging.error(f"Follow-up question generation failed: {e}")
        final_responses["stages"]["Followup_Questions"] = {"error": str(e)}

    # Step 5: User Input (previously Step 4)
    logging.info("Running Phase 2 Step 5: User Input")
    try:
        user_question = input("\nEnter a specific question about the ticket chain (or 'no' to skip): ").strip()
        userQuestions = {}
        if user_question.lower() != 'no' and user_question:
            user_questions = {"global_question": user_question}
            logging.info(f"User question: {user_question}")
        else:
            logging.info("User skipped custom question")
        final_responses["stages"]["User_Questions"] = user_questions
    except Exception as e:
        logging.error(f"Error getting user input: {e}")
        final_responses["stages"]["User_Questions"] = {"error": str(e)}

    # Step 6: Detailed Re-Analysis (previously Step 5)
    logging.info("Running Phase 2 Step 6: Detailed Re-Analysis")
    detailed_results = []
    tickets_with_questions = get_tickets_with_questions(questions_json if questions_json else {}, user_questions, full_ticket_data)
    for ticket_id, q_data in tickets_with_questions.items():
        logging.info(f"Re-analyzing ticket {ticket_id}")
        detail_prompt = create_detailed_analysis_prompt(chain_hash, ticket_id, full_ticket_data, q_data["questions"])
        try:
            detail_response = run_assistant_query(client, thread.id, assistant_id, detail_prompt)
            detail_json = validate_response(detail_response, [ticket_id])
            detailed_results.append({
                "ticket_id": ticket_id,
                "result": detail_json if detail_json else {"error": "Invalid JSON", "raw": detail_response}
            })
            final_responses["stages"][f"Detailed_Analysis_Ticket_{ticket_id}"] = detailed_results[-1]
            print(f"\n--- Detailed Analysis for Ticket {ticket_id} (JSON) ---")
            if detail_json:
                print(json.dumps(detail_json, indent=2))
            else:
                print(f"ERROR: Invalid JSON.\nRaw:\n{detail_response}")
            print("-----------------------------------")
        except Exception as e:
            logging.error(f"Detailed analysis for ticket {ticket_id} failed: {e}")
            detailed_results.append({"ticket_id": ticket_id, "error": str(e)})
            final_responses["stages"][f"Detailed_Analysis_Ticket_{ticket_id}"] = {"error": str(e)}

    # Step 7: Consolidation (previously Step 6)
    logging.info("Running Phase 2 Step 7: Final Consolidation")
    consolidated_report = consolidate_final_report(batch_results, issues_index, detailed_results, full_ticket_data, chain_hash)
    final_responses["consolidated_report"] = consolidated_report
    print("\n--- Consolidated Final Report (JSON) ---")
    print(json.dumps(consolidated_report, indent=2))
    print("---------------------------------------")

    # Save Output
    final_output_file = f"PyChain/data/analyses/Phase2_BatchIterative_{chain_hash}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze a ticket chain")
    parser.add_argument("--ticket", required=True, help="Ticket ID to analyze")
    parser.add_argument("--phase", choices=["all", "phase1", "phase2"], default="all", help="Analysis phase to run")
    args = parser.parse_args()
    
    analyze_real_ticket(args.ticket, args.phase)