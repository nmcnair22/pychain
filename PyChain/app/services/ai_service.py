import logging
import os
from openai import OpenAI
from anthropic import Anthropic

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize clients for different AI services
openai_client = None
anthropic_client = None
xai_client = None

# Check for API keys
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
X_AI_API_KEY = os.getenv("X_AI_API_KEY")

if OPENAI_API_KEY:
    try:
        openai_client = OpenAI(api_key=OPENAI_API_KEY)
        logging.info("OpenAI client initialized successfully.")
    except Exception as e:
        logging.error(f"Failed to initialize OpenAI client: {e}")
else:
    logging.warning("OpenAI API key not found. Skipping OpenAI client initialization.")

if ANTHROPIC_API_KEY:
    try:
        anthropic_client = Anthropic(api_key=ANTHROPIC_API_KEY)
        logging.info("Anthropic client initialized successfully.")
    except Exception as e:
        logging.error(f"Failed to initialize Anthropic client: {e}")
else:
    logging.warning("Anthropic API key not found. Skipping Anthropic client initialization.")

if X_AI_API_KEY:
    try:
        xai_client = OpenAI(api_key=X_AI_API_KEY, base_url="https://api.x.ai/v1")
        logging.info("x.ai client initialized successfully.")
    except Exception as e:
        logging.error(f"Failed to initialize x.ai client: {e}")
else:
    logging.warning("x.ai API key not found. Skipping x.ai client initialization.")

class AIService:
    """Service to interact with multiple AI providers for ticket analysis"""
    
    @staticmethod
    def analyze_ticket(ticket, provider="openai"):
        """
        Analyze a ticket using the specified AI provider
        
        Args:
            ticket: The ticket object to analyze
            provider: The AI provider to use ("openai", "anthropic", "xai")
            
        Returns:
            str: Analysis result from the AI provider
        """
        if not ticket.title or not ticket.description:
            logging.error("Ticket missing required fields (title or description)")
            return "Insufficient information for analysis"
        
        prompt = f"""
        Analyze the following support ticket and provide insights:
        
        Title: {ticket.title}
        Description: {ticket.description}
        Priority: {ticket.priority}
        Status: {ticket.status}
        
        Please analyze:
        1. What is the main issue described?
        2. Is the priority appropriate?
        3. What category does this issue fall into?
        4. Suggest next steps or possible solutions.
        """
        
        return AIService._send_request(provider, prompt, system_message="You are a helpful ticket analysis assistant.")
    
    @staticmethod
    def analyze_chain(prompt, report_type="relationship_summary", provider="openai"):
        """
        Analyze a ticket chain using the specified AI provider with different report types
        
        Args:
            prompt: The detailed prompt containing ticket chain information
            report_type: Type of report to generate (relationship_summary or timelines_outcomes)
            provider: The AI provider to use ("openai", "anthropic", "xai")
            
        Returns:
            str: Analysis result from the AI provider describing the relationships between tickets
        """
        system_content = "You are an expert field service analyst who specializes in understanding complex relationships between ticket records in a field service system."
        
        if report_type == "timelines_outcomes":
            # Append specialized instructions for the timelines and outcomes report
            system_content += """
            You will focus on creating clear timelines of visits with details about:
            
            1. The scope of each visit and the reason for each visit
            2. What scope was actually completed during each visit
            3. Issues or work that was not completed 
            4. Whether revisits were required due to incomplete work
            5. Specific information about cable drops and whether there were material shortages
            6. Do not guess or make up any information - only report what is evident in the provided data
            
            At the end, provide a summary that classifies each revisit as:
            - Not completed due to internal issues (our fault)
            - Not completed due to customer/site issues (client's responsibility)
            - Whether the revisit should be billable to the client and why
            
            Be factual and specific - cite ticket IDs and actual notes rather than inferring information.
            """
        
        return AIService._send_request(provider, prompt, system_message=system_content, max_tokens=4000)
    
    @staticmethod
    def ask_follow_up_question(prompt: str, context: str, provider: str = "openai") -> str:
        """
        Answer a follow-up question about a ticket chain using the specified AI provider.

        Args:
            prompt: The follow-up question prompt.
            context: The context data (e.g., ticket chain JSON) to include.
            provider: The AI provider to use ("openai", "anthropic", "xai").

        Returns:
            str: Response to the follow-up question.
        """
        full_prompt = f"""
        Context:
        {context}

        Follow-up Question:
        {prompt}

        Provide a detailed and factual response based on the provided context. Cite specific ticket IDs and details from posts or notes where applicable.
        """
        system_message = "You are an expert field service analyst answering follow-up questions about ticket chains."
        return AIService._send_request(provider, full_prompt, system_message=system_message, max_tokens=1000)
    
    @staticmethod
    def _send_request(provider, user_prompt, system_message="", max_tokens=2000):
        """
        Send a request to the specified AI provider
        
        Args:
            provider: The AI provider to use ("openai", "anthropic", "xai")
            user_prompt: The user prompt to send
            system_message: The system message for context
            max_tokens: Maximum tokens for the response
            
        Returns:
            str: Response content from the AI provider
        """
        try:
            if provider == "openai" and openai_client:
                openai_model = os.getenv("OPENAI_MODEL", "gpt-4o")
                response = openai_client.chat.completions.create(
                    model=openai_model, 
                    messages=[
                        {"role": "system", "content": system_message} if system_message else {"role": "system", "content": "You are a helpful assistant."},
                        {"role": "user", "content": user_prompt}
                    ],
                    max_tokens=max_tokens
                )
                return response.choices[0].message.content
            elif provider == "anthropic" and anthropic_client:
                response = anthropic_client.messages.create(
                    model="claude-3-opus-20240229",
                    max_tokens=max_tokens,
                    system=system_message if system_message else "You are a helpful assistant.",
                    messages=[
                        {"role": "user", "content": user_prompt}
                    ]
                )
                return response.content[0].text
            elif provider == "xai" and xai_client:
                response = xai_client.chat.completions.create(
                    model="grok-2",
                    messages=[
                        {"role": "system", "content": system_message} if system_message else {"role": "system", "content": "You are a helpful assistant."},
                        {"role": "user", "content": user_prompt}
                    ],
                    max_tokens=max_tokens
                )
                return response.choices[0].message.content
            else:
                logging.error(f"Provider {provider} not supported or not initialized")
                return f"Error: Provider {provider} not supported or not initialized."
        except Exception as e:
            logging.error(f"Error with {provider} request: {str(e)}")
            return f"Error with {provider} request: {str(e)}"