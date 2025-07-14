# llm_service.py
import os # [8]
import re # [8]
import json # [8]
import pandas as pd # [8]
import base64 # [8]
from io import BytesIO # [8]
from PIL import Image # [8]
import plotly.io as pio # [8]
import time # [8] # Though not used in functions moved here, keep imports from source
from langchain_community.chat_models import AzureChatOpenAI # [8]
from langchain_core.output_parsers import JsonOutputParser # [15]
from langchain_community.tools.tavily_search import TavilySearchResults # [8]
from langchain.schema import HumanMessage, AIMessage # [8] # Needed for history formatting
import openai

# Import memory management functions and state
from memory_manager import get_or_create_memory, format_conversation_history # Using helper too

# Import configuration
from config import (
    AZURE_OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT,
    AZURE_OPENAI_API_VERSION, AZURE_OPENAI_DEPLOYMENT_NAME,
    TAVILY_API_KEY
)

# openai.api_key = AZURE_OPENAI_API_KEY


# --- Initialize LLM and Tools ---

# Initialize the Azure OpenAI LLM [3]
# Configuration parameters are expected to be set via environment variables (handled by config.py)
try:
    llm = AzureChatOpenAI( # [3]
        openai_api_key=AZURE_OPENAI_API_KEY, # Use variables from config
        azure_endpoint=AZURE_OPENAI_ENDPOINT, # Use variables from config
        openai_api_version=AZURE_OPENAI_API_VERSION, # Use variables from config [3]
        deployment_name=AZURE_OPENAI_DEPLOYMENT_NAME, # Use variables from config [3]
        temperature=0.7 # [3]
    )
except Exception as e:
    # Log the error and re-raise [16]
    print(f"  Failed to Initialize Azure OpenAI Model: {e}") # [16] ❌
    raise # [16]

# Initialize Tavily Search Tool [6]
try:
    # TAVILY_API_KEY is set as env var in config.py
    tavily_search = TavilySearchResults(max_results=5) # [6]
except Exception as e:
    # Log the error [6]
    print(f"  Failed to Initialize Tavily Search: {e}") # [6] ❌
    tavily_search = None # [6]

# --- Core LLM/AI Functions ---

def visual_generate(llm, query, data, response):
    try:
        prompt = f"""
        Give me Python code that can generate an insightful graph or plot on the given dataframe for the query based on response using Plotly.
        Dataframe: {data}.
        Query: {query}
        Response: {response}

        Follow below color schema for the plot:
        - Background: black (#2B2C2E)
        - Text and labels: white (#FFFFFF)
        - Bars/Lines: either #4BC884 or #22A6BD
        Also, add data labels.

        DO NOT include any additional text other than the Python code in the response.
        Save the plot at the end using: fig.write_image('graph.png', engine='kaleido')

        If it's not possible to generate a graph, return 'No graph generated' as a response.
        """

        code = llm.invoke(prompt).content
        if "No graph generated" in code:
            encoded_image = ''
        else:
            code = re.sub(r'```python', '', code)
            code = re.sub(r'`', '', code)
            exec(code)
            # plt.savefig('graph.png', dpi=300, bbox_inches='tight', facecolor='black')
            with open('graph.png', 'rb') as image_file:
                encoded_image = base64.b64encode(image_file.read()).decode("utf-8")
    except Exception as e:
        print("⚠️ Graph generation error:", e)
        encoded_image = ''
    return encoded_image
    
def classify_query(llm, user_question):
    """
    Determines if a question is data-specific or general knowledge
    
    Parameters:
    llm: The language model
    user_question (str): The user's question
    
    Returns:
    dict: Classification with query_type and confidence
    """

    classification_prompt = f"""
    You are a query classifier that determines if a user question requires data analysis or general knowledge.
    
    Question: "{user_question}"
    
    Analyze this question and determine if it requires:
    1. DATA_ANALYSIS - Questions about orders, revenue, costs, deliveries, metrics, or anything that would need database access
    2. GENERAL_KNOWLEDGE - General questions, factual information, advice, or anything not related to company data
    
    Output your answer in JSON format with these fields:
    - query_type: Either "DATA_ANALYSIS" or "GENERAL_KNOWLEDGE"
    - confidence: A number between 0 and 1 indicating your confidence in this classification
    - reasoning: A brief explanation of your reasoning
    
    JSON format only with no additional text:
    """
    
    parser = JsonOutputParser()
    
    try:
        # First get the raw response from the LLM
        response = llm.invoke(classification_prompt)
        
        # Then parse the JSON
        try:
            # Try to parse directly
            import json
            result = json.loads(response.content)
        except json.JSONDecodeError:
            # If direct parsing fails, try to use the parser
            result = parser.invoke(response.content)
            
        return result
    except Exception as e:
        print(f"❌ Classification error: {str(e)}")
        # Default to data analysis if classification fails
        return {"query_type": "DATA_ANALYSIS", "confidence": 0.5, "reasoning": "Classification failed"}
    
# def get_general_knowledge_response(llm, user_question):
#     """
#     Generates a response for general knowledge questions using Tavily search
    
#     Parameters:
#     llm: The language model
#     user_question (str): The user's question
    
#     Returns:
#     str: The generated response
#     """
#     if not tavily_search:
#         return "I'm having trouble accessing my knowledge tools right now. Please try again later."
    
#     try:
#         # Get search results
#         search_results = tavily_search.invoke(user_question)
        
#         # Create prompt with search results
#         search_prompt = f"""
#         You need to answer the user's question based on the search results provided.
        
#         User question: "{user_question}"
        
#         Search results:
#         {search_results}
        
#         Provide a helpful, accurate, and concise response based on these search results.
#         If the search results don't provide sufficient information, acknowledge this
#         and provide the best answer you can based on your knowledge.
#         """
        
#         # Get response
#         response = llm.invoke(search_prompt).content.strip()
#         return response
#     except Exception as e:
#         print(f"❌ General knowledge response error: {str(e)}")
#         return "I'm having trouble finding information about that right now. Please try again later."
    

def get_general_knowledge_response(llm, user_question):
    """
    Generates a response for general knowledge questions using the OpenAI LLM.

    Parameters:
    llm: The language model instance with an 'invoke' method
    user_question (str): The user's question

    Returns:
    str: The generated response
    """
    print(f"Inside get_general_knowledge_response_openai ")
    try:

        print(f"Inside get_general_knowledge_response_openai try bloc")
        # Create a prompt for the LLM
        prompt = f"""
        Answer the following general knowledge question accurately and concisely.

        Question: "{user_question}"

        Provide a helpful, factual, and concise response.
        If you are unsure or the information is not available, please state that.
        """

        # Invoke the language model with the prompt
        response = llm.invoke(prompt)

        # Extract and return the content from the response
        return response.content.strip()


    except Exception as e:
        print(f"❌ Error generating response: {str(e)}")
        return "I'm having trouble finding information about that right now. Please try again later."


# def get_general_knowledge_response(llm, user_question):
#     """
#     Generates a response for general knowledge questions using OpenAI's Responses API with web search.

#     Parameters:
#     user_question (str): The user's question

#     Returns:
#     str: The generated response
#     """

#     print(f"Inside responses api ")
#     try:

#         print(f"Inside responses api ")
#         # Create a response with web search tool enabled
#         response = openai.responses.create(
#             model="gpt-4o",  # or another supported model
#             input=user_question,
#             tools=[{"type": "web_search"}]
#         )

#         # Extract and return the content from the response
#         return response.output[0].content[0].text.strip()

#     except Exception as e:
#         print(f"❌ Error generating response: {str(e)}")
#         return "I'm having trouble finding information about that right now. Please try again later."


def handle_database_info_query(conn, user_question, llm):
    """
    Handle queries about database structure, tables, or metadata
    
    Parameters:
    conn: Snowflake connection
    user_question (str): User's question about database
    llm: Language model
    
    Returns:
    dict: Response with table information
    """
    try:
        # Fetch all table names and their descriptions
        metadata_query = """
        SELECT 
            TABLE_NAME, 
            CASE 
                WHEN TABLE_TYPE = 'BASE TABLE' THEN 'Standard table'
                WHEN TABLE_TYPE = 'VIEW' THEN 'View'
                ELSE TABLE_TYPE 
            END AS TABLE_TYPE
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'sop_da';
        """
        
        cursor = conn.cursor()
        cursor.execute(metadata_query)
        tables = cursor.fetchall()
        cursor.close()
        
        # Prepare table information
        table_info = [
            {
                "name": table[0], 
                "type": table[1]
            } for table in tables
        ]
        
        # Generate a human-readable description
        description_prompt = f"""
        Given the following database tables:
        {json.dumps(table_info, indent=2)}
        
        Create a concise, informative description that explains the purpose and contents of these tables.
        Focus on how these tables might be related and what kind of business insights they could provide.
        """
        
        # Get description from LLM
        description = llm.invoke(description_prompt).content.strip()
        
        return {
            "table_count": len(tables),
            "tables": table_info,
            "description": description
        }
    
    except Exception as e:
        print(f"❌ Database info query error: {str(e)}")
        return {
            "table_count": 0,
            "tables": [],
            "description": "Unable to retrieve database information."
        }


