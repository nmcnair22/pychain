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
    def analyze_chain(prompt, report_type="relationship_summary"):
        """
        Analyze a ticket chain using OpenAI with different report types
        
        Args:
            prompt: The detailed prompt containing ticket chain information
            report_type: Type of report to generate (relationship_summary or timelines_outcomes)
            
        Returns:
            str: Analysis result from OpenAI describing the relationships between tickets
        """
        try:
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
            
            response = client.chat.completions.create(
                model=OPENAI_MODEL, 
                messages=[
                    {
                        "role": "system", 
                        "content": system_content
                    },
                    {"role": "user", "content": prompt}
                ],
                max_tokens=2000  # Allow for detailed analysis
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            return f"Error analyzing ticket chain: {str(e)}" 