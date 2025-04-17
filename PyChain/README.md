# PyChain

A Python application for connecting to MySQL databases (CISSDM and Ticketing) and using OpenAI for ticket analysis.

## Setup

1. Create a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure environment variables:
   - Copy `.env.example` to `.env`
   - Update the values in `.env` with your database and OpenAI API credentials
   - Set `USE_IN_MEMORY_DB=true` for local development without actual database connections

## Database Configuration

This application connects to two MySQL databases:

1. **CISSDM Database**
   - Host: cissdm.cis.local
   - User: view
   - Contains user information

2. **Ticketing Database**
   - Host: ticket10.cis.local
   - User: view
   - Contains ticket information for analysis

## Project Structure

- `app/`: Main application code
  - `models/`: Database models for both databases
  - `services/`: Business logic and database operations
  - `utils/`: Utility functions
- `config.py`: Configuration settings with database connections
- `main.py`: Application entry point
- `ticket_chain_analysis.py`: Tool for analyzing relationships between tickets

## Ticket Chain Analysis

The project includes a specialized tool for analyzing complex relationships between tickets in the ticketing system.

### Running Ticket Chain Analysis

#### Testing with Mock Data

```bash
python ticket_chain_analysis.py mock --complexity 2
```

Complexity levels:
- 1: Simple (1 dispatch ticket, 1 turnup ticket)
- 2: Moderate (2 dispatch tickets, 3 turnup tickets)
- 3: Complex (3 dispatch tickets, 5 turnup tickets)

#### Analyzing Real Tickets

```bash
python ticket_chain_analysis.py analyze TICKET-12345
```

Replace `TICKET-12345` with the actual ticket number you want to analyze.

### How It Works

1. The system finds all tickets connected to the specified ticket via the ticket chain hash
2. It retrieves details for all related tickets (dispatch and turnup)
3. It constructs a detailed prompt describing the tickets and their attributes
4. The OpenAI API analyzes the ticket data and identifies:
   - The chronological order of events
   - Which dispatch tickets spawned which turnup tickets
   - Any anomalies or issues with the ticket relationships
   - A summary of the entire service history

## Development Mode

For development without database access, set `USE_IN_MEMORY_DB=true` in the `.env` file. This will:
- Use SQLite in-memory databases instead of MySQL connections
- Create sample data automatically for testing
- Allow full development of the application logic without external database dependencies

## Usage

Run the main application:
```bash
python main.py
```

The application will:
1. Connect to both databases (or use in-memory SQLite if configured)
2. Create the necessary tables if using in-memory mode
3. Query and display sample data
4. Demonstrate OpenAI integration for ticket analysis 