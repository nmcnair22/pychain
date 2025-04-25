#!/usr/bin/env python3
"""
Example script for creating an OpenAI assistant with file search capabilities
using vector stores.
"""

import os
import time
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI

# Load environment variables
load_dotenv()

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def create_vector_store(name):
    """
    Create a new vector store for document embeddings.
    
    Args:
        name: Name for the vector store
        
    Returns:
        Vector store object
    """
    try:
        vector_store = client.vector_stores.create(name=name)
        print(f"Vector store created with ID: {vector_store.id}")
        return vector_store
    except Exception as e:
        print(f"Error creating vector store: {e}")
        return None

def upload_file(file_path):
    """
    Upload a file to OpenAI for use with assistants.
    
    Args:
        file_path: Path to the file to upload
        
    Returns:
        File object
    """
    try:
        file_path = Path(file_path)
        if not file_path.exists():
            print(f"Error: File not found at {file_path}")
            return None
            
        print(f"Uploading {file_path.name}...")
        with open(file_path, "rb") as file:
            file_obj = client.files.create(
                file=file,
                purpose="assistants"
            )
        print(f"File uploaded with ID: {file_obj.id}")
        return file_obj
    except Exception as e:
        print(f"Error uploading file: {e}")
        return None

def add_file_to_vector_store(vector_store_id, file_id):
    """
    Add a file to a vector store.
    
    Args:
        vector_store_id: ID of the vector store
        file_id: ID of the file to add
        
    Returns:
        File association object
    """
    try:
        file_association = client.vector_stores.files.create(
            vector_store_id=vector_store_id,
            file_id=file_id
        )
        print(f"File {file_id} added to vector store {vector_store_id}")
        return file_association
    except Exception as e:
        print(f"Error adding file to vector store: {e}")
        return None

def create_assistant_with_file_search(name, instructions, vector_store_id):
    """
    Create an assistant with file search capabilities.
    
    Args:
        name: Name for the assistant
        instructions: Instructions for the assistant
        vector_store_id: ID of the vector store to use
        
    Returns:
        Assistant object
    """
    try:
        assistant = client.beta.assistants.create(
            name=name,
            instructions=instructions,
            model="gpt-4o",
            tools=[{"type": "file_search"}],
            tool_resources={
                "file_search": {
                    "vector_store_ids": [vector_store_id]
                }
            }
        )
        print(f"Assistant created with ID: {assistant.id}")
        return assistant
    except Exception as e:
        print(f"Error creating assistant: {e}")
        return None

def create_thread():
    """
    Create a new conversation thread.
    
    Returns:
        Thread object
    """
    try:
        thread = client.beta.threads.create()
        print(f"Thread created with ID: {thread.id}")
        return thread
    except Exception as e:
        print(f"Error creating thread: {e}")
        return None

def add_message_to_thread(thread_id, content):
    """
    Add a user message to a thread.
    
    Args:
        thread_id: ID of the thread
        content: Message content
        
    Returns:
        Message object
    """
    try:
        message = client.beta.threads.messages.create(
            thread_id=thread_id,
            role="user",
            content=content
        )
        print(f"Message added to thread {thread_id}")
        return message
    except Exception as e:
        print(f"Error adding message to thread: {e}")
        return None

def run_assistant(thread_id, assistant_id):
    """
    Run the assistant on a thread.
    
    Args:
        thread_id: ID of the thread
        assistant_id: ID of the assistant
        
    Returns:
        Run object
    """
    try:
        run = client.beta.threads.runs.create(
            thread_id=thread_id,
            assistant_id=assistant_id
        )
        
        # Poll for completion
        while True:
            run_status = client.beta.threads.runs.retrieve(
                thread_id=thread_id,
                run_id=run.id
            )
            
            if run_status.status == "completed":
                print("Assistant run completed")
                break
            elif run_status.status in ["failed", "cancelled", "expired"]:
                print(f"Run failed with status: {run_status.status}")
                break
            
            print(f"Waiting for assistant to complete... (status: {run_status.status})")
            time.sleep(2)
        
        return run_status
    except Exception as e:
        print(f"Error running assistant: {e}")
        return None

def get_messages(thread_id):
    """
    Get messages from a thread.
    
    Args:
        thread_id: ID of the thread
        
    Returns:
        List of messages
    """
    try:
        messages = client.beta.threads.messages.list(
            thread_id=thread_id
        )
        return messages.data
    except Exception as e:
        print(f"Error retrieving messages: {e}")
        return []

def main():
    """Main function demonstrating the assistant workflow"""
    # Create a vector store
    vector_store = create_vector_store("my_document_store")
    if not vector_store:
        return
    
    # Upload a file (replace with your actual file path)
    file_path = "PyChain/data/ticket_files/example_document.pdf"
    file_obj = upload_file(file_path)
    if not file_obj:
        return
    
    # Add the file to the vector store
    file_association = add_file_to_vector_store(vector_store.id, file_obj.id)
    if not file_association:
        return
    
    # Create an assistant with file search capabilities
    assistant = create_assistant_with_file_search(
        "Document Assistant",
        "You are a helpful assistant with document knowledge. Use the provided documents to answer questions.",
        vector_store.id
    )
    if not assistant:
        return
    
    # Create a thread
    thread = create_thread()
    if not thread:
        return
    
    # Add a message to the thread
    message = add_message_to_thread(
        thread.id,
        "What information can you find in the document about this topic?"
    )
    if not message:
        return
    
    # Run the assistant
    run = run_assistant(thread.id, assistant.id)
    if not run:
        return
    
    # Get the messages
    messages = get_messages(thread.id)
    
    # Print the conversation
    print("\nConversation:")
    print("-" * 50)
    for msg in messages:
        print(f"{msg.role.upper()}: {msg.content[0].text.value}")
        print("-" * 50)

if __name__ == "__main__":
    main() 