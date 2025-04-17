from openai import OpenAI
from config import OPENAI_API_KEY, OPENAI_MODEL

client = OpenAI(api_key=OPENAI_API_KEY)

class AIService:
    """Service to interact with OpenAI for ticket analysis"""
    
    @staticmethod
    def analyze_ticket(ticket):
        """
        Analyze a ticket using OpenAI
        
        Args:
            ticket: The ticket object to analyze
            
        Returns:
            str: Analysis result from OpenAI
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
        
        try:
            response = client.chat.completions.create(
                model=OPENAI_MODEL, 
                messages=[
                    {"role": "system", "content": "You are a helpful ticket analysis assistant."},
                    {"role": "user", "content": prompt}
                ]
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            return f"Error analyzing ticket: {str(e)}"
    
    @staticmethod
    def analyze_chain(prompt):
        """
        Analyze a ticket chain using OpenAI
        
        Args:
            prompt: The detailed prompt containing ticket chain information
            
        Returns:
            str: Analysis result from OpenAI describing the relationships between tickets
        """
        try:
            response = client.chat.completions.create(
                model=OPENAI_MODEL, 
                messages=[
                    {
                        "role": "system", 
                        "content": "You are an expert field service analyst who specializes in understanding complex relationships between ticket records in a field service system."
                    },
                    {"role": "user", "content": prompt}
                ],
                max_tokens=2000  # Allow for detailed analysis
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            return f"Error analyzing ticket chain: {str(e)}" 