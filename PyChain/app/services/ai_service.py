from openai import OpenAI
from anthropic import Anthropic
import os

from config import OPENAI_API_KEY, OPENAI_MODEL

# Initialize clients for different AI services
openai_client = OpenAI(api_key=OPENAI_API_KEY)
anthropic_client = None
xai_client = None

# Check for Anthropic and x.ai API keys
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
X_AI_API_KEY = os.getenv("X_AI_API_KEY")

if ANTHROPIC_API_KEY:
    try:
        anthropic_client = Anthropic(api_key=ANTHROPIC_API_KEY)
        print("Anthropic client initialized successfully.")
    except Exception as e:
        print(f"Failed to initialize Anthropic client: {e}")
else:
    print("Anthropic API key not found. Skipping Anthropic client initialization.")

# Initialize x.ai (Grok) client using OpenAI SDK
if X_AI_API_KEY:
    try:
        xai_client = OpenAI(api_key=X_AI_API_KEY, base_url="https://api.x.ai/v1")
        print("x.ai client initialized successfully.")
    except Exception as e:
        print(f"Failed to initialize x.ai client: {e}")
else:
    print("x.ai API key not found. Skipping x.ai client initialization.")

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
        
        return AIService._send_request(provider, prompt, system_message=system_content, max_tokens=2000)
    
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
                response = openai_client.chat.completions.create(
                    model=OPENAI_MODEL, 
                    messages=[
                        {"role": "system", "content": system_message} if system_message else {"role": "system", "content": "You are a helpful assistant."},
                        {"role": "user", "content": user_prompt}
                    ],
                    max_tokens=max_tokens
                )
                return response.choices[0].message.content
            elif provider == "anthropic" and anthropic_client:
                response = anthropic_client.messages.create(
                    model="claude-3-opus-20240229",  # Use a suitable Anthropic model
                    max_tokens=max_tokens,
                    system=system_message if system_message else "You are a helpful assistant.",
                    messages=[
                        {"role": "user", "content": user_prompt}
                    ]
                )
                return response.content[0].text
            elif provider == "xai" and xai_client:
                response = xai_client.chat.completions.create(
                    model="grok-2",  # Use the appropriate Grok model
                    messages=[
                        {"role": "system", "content": system_message} if system_message else {"role": "system", "content": "You are a helpful assistant."},
                        {"role": "user", "content": user_prompt}
                    ],
                    max_tokens=max_tokens
                )
                return response.choices[0].message.content
            else:
                return f"Error: Provider {provider} not supported or not initialized."
        except Exception as e:
            return f"Error with {provider} request: {str(e)}" 