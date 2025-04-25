#!/usr/bin/env python3
"""
Set up the OpenAI Assistant for PyChain Phase 2 analysis with vector store for file search.
"""

import os
import argparse
from dotenv import load_dotenv
from openai import OpenAI

# Load environment variables
load_dotenv()

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

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

def create_vector_store(name):
    """Create a vector store for document embeddings"""
    try:
        print(f"Creating vector store: {name}...")
        vector_store = client.vector_stores.create(name=name)
        print(f"Vector store created with ID: {vector_store.id}")
        return vector_store
    except Exception as e:
        print(f"Error creating vector store: {e}")
        return None

def create_assistant_with_vector_store(vector_store_id):
    """Create a new OpenAI Assistant with file search using vector store"""
    try:
        print(f"Creating new assistant: {ASSISTANT_NAME}...")
        
        # Check if API key is set
        if not os.getenv("OPENAI_API_KEY"):
            print("ERROR: OPENAI_API_KEY not set in .env file.")
            return None
        
        # Create the assistant
        assistant = client.beta.assistants.create(
            name=ASSISTANT_NAME,
            description=ASSISTANT_DESCRIPTION,
            instructions=ASSISTANT_INSTRUCTIONS,
            tools=[{"type": "file_search"}],
            tool_resources={"file_search": {"vector_store_ids": [vector_store_id]}},
            model="gpt-4o"
        )
        
        print(f"Assistant created successfully!")
        print(f"Assistant ID: {assistant.id}")
        
        # Save the assistant ID and vector store ID to .env file
        update_env_file(assistant.id, vector_store_id)
        
        return assistant.id
    
    except Exception as e:
        print(f"Error creating assistant: {e}")
        return None

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

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Set up the OpenAI Assistant for PyChain Phase 2 analysis")
    parser.add_argument("--force", action="store_true", help="Force creation of a new assistant even if ASSISTANT_ID exists")
    parser.add_argument("--name", type=str, default="ticket_analysis_store", help="Name for the vector store")
    
    args = parser.parse_args()
    
    # Check if assistant ID already exists
    existing_assistant_id = os.getenv("ASSISTANT_ID")
    existing_vector_store_id = os.getenv("VECTOR_STORE_ID")
    
    if existing_assistant_id and existing_vector_store_id and not args.force:
        print(f"Assistant ID already exists: {existing_assistant_id}")
        print(f"Vector store ID already exists: {existing_vector_store_id}")
        print("Use --force to create new ones anyway.")
        return
    
    # Create a vector store
    vector_store = create_vector_store(args.name)
    if not vector_store:
        print("Failed to create vector store. Aborting setup.")
        return
        
    # Create a new assistant with vector store
    create_assistant_with_vector_store(vector_store.id)

if __name__ == "__main__":
    main() 