# PyChain Ticket Analysis Tool

An AI-powered tool for analyzing ticket chains in a service management system, providing insights about ticket relationships, timelines, and more.

## Overview

The PyChain Ticket Analysis Tool uses OpenAI's advanced language models to analyze ticket chains and extract meaningful insights, such as:

- Relationships between tickets
- Timeline of events
- Material shortages
- Reasons for revisits
- Billing information

## Setup

1. Clone this repository:
```
git clone https://github.com/your-username/pychain.git
cd pychain
```

2. Create a virtual environment and install dependencies:
```
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

3. Create a `.env` file with your OpenAI API key:
```
OPENAI_API_KEY=your_api_key_here
```

4. Set up the OpenAI Assistant for Phase 2 analysis:
```
python PyChain/setup_assistant.py
```

## Usage

### Phase 1 Analysis (Chat Completions API)

For basic ticket chain analysis:

```
python PyChain/ticket_chain_analysis.py --ticket TICKET_ID
```

This will provide a thorough analysis of the ticket chain using the Chat Completions API.

### Phase 2 Analysis (Assistant API)

For advanced analysis with structured data extraction:

```
python PyChain/ticket_chain_analysis_v2.py --ticket TICKET_ID
```

Phase 2 provides additional capabilities:
- Structured JSON output for easy data processing
- More detailed material shortage analysis
- Timeline extraction
- Revisit cause analysis

### Test Mode

To test with mock data:

```
python PyChain/ticket_chain_analysis_v2.py --test
```

### Using a Specific Assistant

If you have multiple assistants, you can specify which one to use:

```
python PyChain/ticket_chain_analysis_v2.py --ticket TICKET_ID --assistant-id YOUR_ASSISTANT_ID
```

## Analysis Output

Phase 1 creates a text report with insights about the ticket chain.

Phase 2 creates a structured JSON file with:
- Phase 1 summary
- Ticket IDs and phases
- Material shortages
- Timeline events
- Revisit information

Output files are saved in `PyChain/data/analyses/`.

## Requirements

- Python 3.8 or higher
- OpenAI API key
- Dependencies listed in requirements.txt

## License

[MIT License](LICENSE)

# OpenAI Assistants with File Search

This repository contains examples showing how to create and use OpenAI assistants with file search capabilities using vector stores.

## Requirements

- Python 3.8+
- OpenAI API key
- OpenAI Python SDK v1.75.0+

## Installation

```bash
pip install -r requirements.txt
```

## Setting Up Your Environment

1. Set your OpenAI API key as an environment variable:

```bash
export OPENAI_API_KEY=your_api_key_here
```

## Using Vector Stores with Assistants

The latest OpenAI API requires using vector stores to enable file search capabilities in assistants. The workflow is:

1. Create a vector store
2. Upload files to OpenAI
3. Add files to the vector store
4. Create an assistant with file search tool and vector store IDs

### Example Usage

Check out the complete example in `assistant_with_files.py`:

```python
# Create vector store
vector_store = client.vector_stores.create(name="my_document_store")

# Upload file
file_obj = client.files.create(
    file=open("document.pdf", "rb"),
    purpose="assistants"
)

# Add file to vector store
client.vector_stores.files.create(
    vector_store_id=vector_store.id,
    file_id=file_obj.id
)

# Create assistant with file search
assistant = client.beta.assistants.create(
    name="Document Assistant",
    instructions="...",
    model="gpt-4o",
    tools=[{"type": "file_search"}],
    tool_resources={"file_search": {"vector_store_ids": [vector_store.id]}}
)
```

## Key API Components

- `client.vector_stores.create()` - Creates a new vector store
- `client.files.create()` - Uploads a file to OpenAI
- `client.vector_stores.files.create()` - Associates a file with a vector store
- `client.beta.assistants.create()` - Creates an assistant with the `tool_resources` parameter specifying vector stores

## Common Issues

- Files need processing time after being added to a vector store before they can be used
- The `purpose` parameter when uploading files must be set to "assistants"
- Vector stores must be explicitly linked to assistants using the `tool_resources` parameter 