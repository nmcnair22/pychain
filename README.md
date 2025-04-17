# PyChain

PyChain is a Python-based application for analyzing relationships between tickets in a ticketing system. It identifies connections between Dispatch and Turnup tickets, providing insights into service workflows and relationships.

## Features

- Connects to ticketing database to retrieve ticket information
- Groups tickets by category (Dispatch Tickets, Turnup Tickets)
- Identifies ticket chains using chain hash identifiers
- Uses OpenAI to analyze relationships between tickets
- Provides timeline of events, relationship mapping, and anomaly detection

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

## Usage

To analyze tickets, run:

```
python ticket_chain_analysis.py analyze <ticket_id>
```

For example:
```
python ticket_chain_analysis.py analyze 2399922
```

## Architecture

The application is structured as follows:

- `ticket_chain_analysis.py`: Main entry point
- `app/services/ticket_chain_service.py`: Core service for ticket chain analysis
- `app/services/ai_service.py`: Service for OpenAI integration
- `app/models/`: Data models for the application

## License

This project is licensed under the MIT License - see the LICENSE file for details. 