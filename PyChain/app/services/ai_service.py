import logging
import os
from typing import Optional, Union
from openai import OpenAI
from anthropic import Anthropic

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AIService:
    """Service to interact with multiple AI providers for ticket analysis"""

    # Default OpenAI model
    DEFAULT_OPENAI_MODEL = "gpt-4o"

    # Initialize clients as class attributes
    openai_client: Optional[OpenAI] = None
    anthropic_client: Optional[Anthropic] = None
    xai_client: Optional[OpenAI] = None

    def __init__(self):
        """Initialize AI clients for OpenAI, Anthropic, and xAI"""
        # Initialize OpenAI client
        openai_api_key = os.getenv("OPENAI_API_KEY")
        if openai_api_key:
            try:
                AIService.openai_client = OpenAI(api_key=openai_api_key)
                logging.info("OpenAI client initialized successfully.")
            except Exception as e:
                logging.error(f"Failed to initialize OpenAI client: {str(e)}")
        else:
            logging.warning("OpenAI API key not found. Skipping OpenAI client initialization.")

        # Initialize Anthropic client
        anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
        if anthropic_api_key:
            try:
                AIService.anthropic_client = Anthropic(api_key=anthropic_api_key)
                logging.info("Anthropic client initialized successfully.")
            except Exception as e:
                logging.error(f"Failed to initialize Anthropic client: {str(e)}")
        else:
            logging.warning("Anthropic API key not found. Skipping Anthropic client initialization.")

        # Initialize xAI (Grok) client using OpenAI SDK
        xai_api_key = os.getenv("X_AI_API_KEY")
        if xai_api_key:
            try:
                AIService.xai_client = OpenAI(api_key=xai_api_key, base_url="https://api.x.ai/v1")
                logging.info("xAI client initialized successfully.")
            except Exception as e:
                logging.error(f"Failed to initialize xAI client: {str(e)}")
        else:
            logging.warning("xAI API key not found. Skipping xAI client initialization.")

    @staticmethod
    def analyze_ticket(ticket, provider: str = "openai") -> str:
        """
        Analyze a ticket using the specified AI provider.

        Args:
            ticket: The ticket object to analyze, with attributes title, description, priority, status.
            provider: The AI provider to use ("openai", "anthropic", "xai").

        Returns:
            str: Analysis result from the AI provider.
        """
        if not hasattr(ticket, 'title') or not hasattr(ticket, 'description') or not ticket.title or not ticket.description:
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
    def analyze_chain(prompt: str, report_type: str = "relationship_summary", provider: str = "openai") -> str:
        """
        Analyze a ticket chain using the specified AI provider with different report types.

        Args:
            prompt: The detailed prompt containing ticket chain information.
            report_type: Type of report to generate ("relationship_summary" or "timelines_outcomes").
            provider: The AI provider to use ("openai", "anthropic", "xai").

        Returns:
            str: Analysis result from the AI provider describing the relationships between tickets.
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
    def _send_request(provider: str, user_prompt: str, system_message: str = "", max_tokens: int = 2000) -> str:
        """
        Send a request to the specified AI provider.

        Args:
            provider: The AI provider to use ("openai", "anthropic", "xai").
            user_prompt: The user prompt to send.
            system_message: The system message for context.
            max_tokens: Maximum tokens for the response.

        Returns:
            str: Response content from the AI provider.
        """
        try:
            if provider == "openai":
                if not AIService.openai_client:
                    logging.error("OpenAI client not initialized")
                    return "Error: OpenAI client not initialized"
                openai_model = os.getenv("OPENAI_MODEL", AIService.DEFAULT_OPENAI_MODEL)
                response = AIService.openai_client.chat.completions.create(
                    model=openai_model,
                    messages=[
                        {"role": "system", "content": system_message} if system_message else {"role": "system", "content": "You are a helpful assistant."},
                        {"role": "user", "content": user_prompt}
                    ],
                    max_tokens=max_tokens
                )
                return response.choices[0].message.content

            elif provider == "anthropic":
                if not AIService.anthropic_client:
                    logging.error("Anthropic client not initialized")
                    return "Error: Anthropic client not initialized"
                response = AIService.anthropic_client.messages.create(
                    model="claude-3-opus-20240229",
                    max_tokens=max_tokens,
                    system=system_message if system_message else "You are a helpful assistant.",
                    messages=[
                        {"role": "user", "content": user_prompt}
                    ]
                )
                return response.content[0].text

            elif provider == "xai":
                if not AIService.xai_client:
                    logging.error("xAI client not initialized")
                    return "Error: xAI client not initialized"
                response = AIService.xai_client.chat.completions.create(
                    model="grok-2",
                    messages=[
                        {"role": "system", "content": system_message} if system_message else {"role": "system", "content": "You are a helpful assistant."},
                        {"role": "user", "content": user_prompt}
                    ],
                    max_tokens=max_tokens
                )
                return response.choices[0].message.content

            else:
                logging.error(f"Unsupported provider: {provider}")
                return f"Error: Provider {provider} not supported"

        except Exception as e:
            logging.error(f"Error with {provider} request: {str(e)}")
            return f"Error with {provider} request: {str(e)}"