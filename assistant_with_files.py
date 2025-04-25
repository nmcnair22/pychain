import os
from openai import OpenAI
import time

# Initialize the OpenAI client
client = OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY")
)

def create_vector_store(name):
    """Create a new vector store for storing document embeddings"""
    print(f"Creating vector store: {name}")
    vector_store = client.vector_stores.create(name=name)
    print(f"Vector store created with ID: {vector_store.id}")
    return vector_store

def upload_file(file_path):
    """Upload a file to OpenAI and return the file object"""
    print(f"Uploading file: {file_path}")
    with open(file_path, "rb") as file:
        file_obj = client.files.create(
            file=file,
            purpose="assistants"
        )
    print(f"File uploaded with ID: {file_obj.id}")
    return file_obj

def add_file_to_vector_store(vector_store_id, file_id):
    """Add a file to a vector store to create embeddings"""
    print(f"Adding file {file_id} to vector store {vector_store_id}")
    result = client.vector_stores.files.create(
        vector_store_id=vector_store_id,
        file_id=file_id
    )
    print(f"File added to vector store: {result}")
    return result

def create_assistant_with_file_search(name, instructions, vector_store_id):
    """Create an assistant with file search capabilities using a vector store"""
    print(f"Creating assistant: {name}")
    assistant = client.beta.assistants.create(
        name=name,
        instructions=instructions,
        model="gpt-4o",
        tools=[{"type": "file_search"}],
        tool_resources={"file_search": {"vector_store_ids": [vector_store_id]}}
    )
    print(f"Assistant created with ID: {assistant.id}")
    return assistant

def create_thread():
    """Create a new thread for conversation"""
    thread = client.beta.threads.create()
    print(f"Thread created with ID: {thread.id}")
    return thread

def add_message_to_thread(thread_id, content):
    """Add a user message to a thread"""
    message = client.beta.threads.messages.create(
        thread_id=thread_id,
        role="user",
        content=content
    )
    print(f"Message added to thread: {message.id}")
    return message

def run_assistant(thread_id, assistant_id):
    """Run the assistant on the thread and wait for completion"""
    print(f"Running assistant {assistant_id} on thread {thread_id}")
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
        print(f"Run status: {run_status.status}")
        
        if run_status.status in ["completed", "failed", "cancelled"]:
            break
            
        time.sleep(1)
    
    return run_status

def get_messages(thread_id):
    """Get all messages from a thread"""
    messages = client.beta.threads.messages.list(
        thread_id=thread_id
    )
    return messages

def main():
    # 1. Create a vector store
    vector_store = create_vector_store("my_document_store")
    vector_store_id = vector_store.id
    
    # 2. Upload a file (replace with your actual file path)
    file_path = "document.pdf"  # Replace with your file path
    file_obj = upload_file(file_path)
    
    # 3. Add the file to the vector store
    add_file_to_vector_store(vector_store_id, file_obj.id)
    
    # Wait for processing
    print("Waiting for file processing...")
    time.sleep(5)
    
    # 4. Create an assistant with file search capability
    instructions = """You are a helpful assistant with access to documents.
    When asked about information in the documents, use the file_search tool to find relevant information.
    Always provide detailed answers based on the document content."""
    
    assistant = create_assistant_with_file_search(
        "Document Assistant",
        instructions,
        vector_store_id
    )
    
    # 5. Create a thread and add a message
    thread = create_thread()
    add_message_to_thread(
        thread.id,
        "What information can you find in the document about the main topic?"
    )
    
    # 6. Run the assistant
    run_status = run_assistant(thread.id, assistant.id)
    
    # 7. Get and display the messages
    if run_status.status == "completed":
        messages = get_messages(thread.id)
        print("\nConversation:")
        for msg in messages.data:
            role = msg.role
            content = msg.content[0].text.value if msg.content else "No content"
            print(f"{role.upper()}: {content}")
    else:
        print(f"Run ended with status: {run_status.status}")

if __name__ == "__main__":
    main() 