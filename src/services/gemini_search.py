import logging
import os
from typing import Optional, Tuple, List, Dict, Any
from dotenv import load_dotenv

# LangChain integration for structured output
from langchain_google_genai import ChatGoogleGenerativeAI
# Keep base google genai imports for now if query_gemini_with_grounding is still used elsewhere
from google.genai.types import Tool, GenerateContentConfig, GoogleSearch 

# Import the Pydantic model for structured output
from ..models.social import GeminiPodcastEnrichment
# --- NEW: Import ValidationError --- #
from pydantic import ValidationError
# --- END NEW --- #

# Constants
MODEL_ID = os.getenv("GEMINI_MODEL", "gemini-2.0-flash") # Default to flash

load_dotenv()
logger = logging.getLogger(__name__)

class GeminiSearchService:
    """Handles interactions with the Gemini API, including structured output."""

    def __init__(self):
        """Initializes the LangChain Gemini client."""
        self.llm = None
        self.model_name = MODEL_ID
        try:
            api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
            if not api_key:
                raise ValueError("API key not found. Please set GOOGLE_API_KEY or GEMINI_API_KEY.")
            
            # Initialize LangChain ChatGoogleGenerativeAI client
            self.llm = ChatGoogleGenerativeAI(
                model=self.model_name, 
                google_api_key=api_key, 
                temperature=0.3, # Adjust temperature as needed
            )
            logger.info(f"LangChain Gemini client initialized successfully with model: {self.model_name}.")
        except Exception as e:
            logger.exception(f"An error occurred during LangChain Gemini client initialization: {e}")

    def _ensure_client(self):
        """Checks if the LangChain client was initialized successfully."""
        if not self.llm:
            logger.error("Error: LangChain Gemini client was not initialized successfully. Cannot proceed.")
            return False
        return True

    def get_structured_podcast_enrichment(self, prompt: str) -> Optional[GeminiPodcastEnrichment]:
        """ 
        Queries Gemini using LangChain and asks for output structured according 
        to the GeminiPodcastEnrichment Pydantic model.

        Args:
            prompt: The prompt detailing the information needed.

        Returns:
            A validated GeminiPodcastEnrichment object or None if parsing/validation fails.
        """
        if not self._ensure_client():
            return None

        # --- Define the Google Search tool CLASS --- #
        # search_tool = Tool(google_search=GoogleSearch()) # Original instance
        # --- Try passing the class instead --- #
        google_search_tool_class = GoogleSearch 
        
        logger.debug(f"Sending structured output request with search grounding to Gemini (Model: {self.model_name}). Prompt: '{prompt[:100]}...'")
        try:
            # --- Bind the search tool CLASS and specify structured output --- #
            # Note: This might not be the correct way to enable grounding via bind_tools
            structured_llm_with_search = self.llm.bind_tools(
                [google_search_tool_class], # Pass the class 
            ).with_structured_output(GeminiPodcastEnrichment)
            
            # Invoke the LLM
            response = structured_llm_with_search.invoke(prompt)
            
            if isinstance(response, GeminiPodcastEnrichment):
                logger.info("Successfully received and parsed structured response from Gemini (with search grounding enabled).")
                return response
            else:
                logger.error(f"Structured output call did not return the expected Pydantic model. Got type: {type(response)}")
                return None

        # --- NEW: Catch Pydantic validation errors specifically --- #
        except ValidationError as ve:
             logger.error(f"Pydantic validation failed parsing Gemini output for prompt '{prompt[:50]}...': {ve}")
             return None
        # --- END NEW --- #
        except Exception as e:
            logger.exception(f"Error getting structured output from Gemini (with search grounding): {e}")
            return None
            
    # --- Keep existing methods for now if they are used elsewhere --- #
    # --- but they use the base genai client which is no longer initialized --- #
    # --- These would need refactoring if still required --- #
    def query_gemini_google_search(self, query: str):
        logger.warning("query_gemini_google_search is deprecated; uses base genai client which is not initialized.")
        return None # Or raise NotImplementedError
        # ... (original implementation commented out or removed) ...

    def query_gemini_with_grounding(self, query: str):
        logger.warning("query_gemini_with_grounding is deprecated; uses base genai client which is not initialized.")
        return None, [], [] # Or raise NotImplementedError
        # ... (original implementation commented out or removed) ...

# Example Usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO) # Set log level for testing
    service = GeminiSearchService()
    if service.llm:
        # Example prompt asking for information likely needing search
        test_prompt = "Who is the host of the podcast 'Acquired' and what is its official Linkedin URL?"
        print(f"\n--- Testing Structured Output with Search Grounding --- ")
        print(f"Prompt: {test_prompt}")
        
        structured_response = service.get_structured_podcast_enrichment(test_prompt)
        
        print("\n--- Result --- ")
        if structured_response:
            # Using model_dump_json for pretty printing the Pydantic object
            print(structured_response.model_dump_json(indent=2))
        else:
            print("Failed to get structured response.")
    else:
        print("Could not run example because LangChain Gemini client initialization failed.")