# PyChain

PyChain is a Python-based application for analyzing relationships between tickets in a ticketing system. It identifies connections between Dispatch and Turnup tickets, providing insights into service workflows and relationships.

## Features

- Connects to ticketing database to retrieve ticket information
- Groups tickets by category (Dispatch Tickets, Turnup Tickets)
- Identifies ticket chains using chain hash identifiers
- Uses OpenAI to analyze relationships between tickets
- Provides timeline of events, relationship mapping, and anomaly detection
- Stores analysis results in a local SQLite database for future reference
- Supports batch processing of multiple tickets in a single run

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/nmcnair22/pychain.git
   cd pychain
   ```

2. Create and activate a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows, use: venv\Scripts\activate
   ```

3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

4. Set up your environment variables in a `.env` file:
   ```
   # Database Configuration - CISSDM
   CISSDM_DB_HOST=your_host
   CISSDM_DB_PORT=3306
   CISSDM_DB_NAME=cissdm
   CISSDM_DB_USER=your_user
   CISSDM_DB_PASSWORD=your_password

   # Database Configuration - Ticketing
   TICKETING_DB_HOST=your_host
   TICKETING_DB_PORT=3306
   TICKETING_DB_NAME=ticketing
   TICKETING_DB_USER=your_user
   TICKETING_DB_PASSWORD=your_password

   # Use in-memory database for local development (true/false)
   USE_IN_MEMORY_DB=false

   # OpenAI Configuration
   OPENAI_API_KEY=your_openai_api_key
   ```

5. Create the data directory for the SQLite database:
   ```
   mkdir -p PyChain/data
   ```

## Setting Up on a New Machine

If you're setting up PyChain on a new machine after it's been developed on another machine, follow these steps:

1. Clone the repository or pull the latest changes:
   ```
   git clone https://github.com/nmcnair22/pychain.git
   ```
   or if already cloned:
   ```
   git pull origin
   git checkout report_v2
   ```

2. Create and activate the virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install or update dependencies:
   ```
   pip install -r requirements.txt
   ```

4. Create the data directory for the SQLite database:
   ```
   mkdir -p PyChain/data
   ```

5. Run any command with the script to initialize the database:
   ```
   python PyChain/ticket_chain_analysis.py list
   ```

The database file will be automatically created when you first run the script. The SQLite database is stored locally and does not require any additional setup.

## Usage

### Analyzing a Single Ticket

To analyze a single ticket, run:

```
python PyChain/ticket_chain_analysis.py analyze <ticket_id>
```

For example:
```
python PyChain/ticket_chain_analysis.py analyze 2399922
```

### Batch Processing Multiple Tickets

To analyze multiple tickets in a single run, use the batch command with comma-separated ticket IDs:

```
python PyChain/ticket_chain_analysis.py batch "2426369,2424785,2399922"
```

### Managing Analysis Results

To list all previously saved analyses:

```
python PyChain/ticket_chain_analysis.py list
```

To view a specific saved analysis:

```
python PyChain/ticket_chain_analysis.py show <analysis_id>
```

## Architecture

The application is structured as follows:

- `ticket_chain_analysis.py`: Main entry point
- `app/services/ticket_chain_service.py`: Core service for ticket chain analysis
- `app/services/ai_service.py`: Service for OpenAI integration
- `app/services/analysis_service.py`: Service for storing and retrieving analysis results
- `app/models/`: Data models for the application
- `app/models/analysis_result.py`: Model for storing analysis results in SQLite

## License

This project is licensed under the MIT License - see the LICENSE file for details. 