import logging
import os
from typing import Optional, Tuple, List, Dict, Any, Type
from dotenv import load_dotenv

# LangChain integration for structured output
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.pydantic_v1 import BaseModel
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
                # Removed grounding tool here, as it's applied per-call if needed
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

    # --- NEW Generic Structured Data Method ---
    def get_structured_data(self, prompt: str, response_model: Type[BaseModel]) -> Optional[BaseModel]:
        """Queries Gemini using LangChain and asks for output structured 
           according to the provided Pydantic model.

        Args:
            prompt: The prompt detailing the information needed.
            response_model: The Pydantic model class for the desired structure.

        Returns:
            A validated Pydantic object of type response_model or None if parsing/validation fails.
        """
        if not self._ensure_client():
            return None

        logger.debug(f"Sending structured output request to Gemini (Model: {self.model_name}). Target Model: {response_model.__name__}. Prompt: '{prompt[:100]}...'")
        try:
            # Bind the structured output format to the LLM call
            structured_llm = self.llm.with_structured_output(response_model)
            
            # Invoke the LLM
            response = structured_llm.invoke(prompt)
            
            # LangChain's with_structured_output should ideally return the validated model instance
            if isinstance(response, response_model):
                logger.info(f"Successfully received and parsed structured response as {response_model.__name__}.")
                return response
            else:
                # This case might indicate an internal LangChain issue or unexpected LLM response format
                logger.error(f"Structured output call did not return the expected Pydantic model type ({response_model.__name__}). Got type: {type(response)}. Response: {response}")
                # Attempt manual validation if possible (though less likely to succeed if LC failed)
                try:
                    validated_response = response_model.parse_obj(response) # Try parsing if it looks like a dict
                    logger.warning("Manual Pydantic validation succeeded after structured output failure.")
                    return validated_response
                except (ValidationError, TypeError) as manual_exc:
                    logger.error(f"Manual Pydantic validation also failed: {manual_exc}")
                    return None
                    
        except ValidationError as ve:
             # This might catch errors if the LLM returns JSON that *almost* matches but fails validation
             logger.error(f"Pydantic validation failed processing Gemini output for model {response_model.__name__}. Error: {ve}. Prompt: '{prompt[:50]}...'")
             return None
        except Exception as e:
            # Catch other potential errors (API issues, etc.)
            logger.exception(f"Error getting structured output ({response_model.__name__}) from Gemini: {e}")
            return None

    # --- Specific method for podcast enrichment (can now use the generic one) ---
    def get_structured_podcast_enrichment(self, prompt: str) -> Optional['GeminiPodcastEnrichment']:
        """ 
        Specific method to get GeminiPodcastEnrichment. Uses the generic get_structured_data.
        DEPRECATED (using generic method is preferred), kept for backward compatibility if needed.
        """
        from ..models.social import GeminiPodcastEnrichment # Import locally if needed
        logger.warning("Using deprecated get_structured_podcast_enrichment. Consider calling get_structured_data directly.")
        result = self.get_structured_data(prompt, GeminiPodcastEnrichment)
        # Ensure the return type matches the specific annotation if needed, though generic should work
        if isinstance(result, GeminiPodcastEnrichment):
            return result
        return None # Return None if the type doesn't match somehow
            
    # --- Deprecated methods using base genai client --- #
    def query_gemini_google_search(self, query: str):
        logger.warning("query_gemini_google_search is deprecated; uses base genai client which is not initialized.")
        return None

    def query_gemini_with_grounding(self, query: str):
        logger.warning("query_gemini_with_grounding is deprecated; uses base genai client which is not initialized.")
        return None, [], []

# Example Usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG) # Use DEBUG for testing
    service = GeminiSearchService()
    if service.llm:
        # --- Test 1: Existing Enrichment Example (using deprecated wrapper) ---
        from ..models.social import GeminiPodcastEnrichment # Need for testing
        test_prompt_enrich = "Who is the host of the podcast 'Acquired' and what is its official Linkedin URL?" 
        print(f"\n--- Testing DEPRECATED get_structured_podcast_enrichment --- ")
        print(f"Prompt: {test_prompt_enrich}")
        structured_response_enrich = service.get_structured_podcast_enrichment(test_prompt_enrich)
        print("\n--- Result 1 --- ")
        if structured_response_enrich:
            print(structured_response_enrich.model_dump_json(indent=2))
        else:
            print("Failed to get structured response.")
            
        # --- Test 2: Generic Method Example (using LLMVettingOutput) ---
        from ..models.llm_outputs import LLMVettingOutput # Need for testing
        test_prompt_vet = "Podcast: Title=AI Insights, Desc=Talks about ML. Guest: Bio=AI Researcher, Points=Ethics. Ideal: Deep AI Ethics discussions. Rate match 0-100 and explain briefly. Return JSON: {match_score: int, explanation: str}" 
        print(f"\n--- Testing GENERIC get_structured_data with LLMVettingOutput --- ")
        print(f"Prompt: {test_prompt_vet}")
        structured_response_vet = service.get_structured_data(test_prompt_vet, LLMVettingOutput)
        print("\n--- Result 2 --- ")
        if structured_response_vet:
            print(structured_response_vet.model_dump_json(indent=2))
        else:
            print("Failed to get structured response.")
            
    else:
        print("Could not run example because LangChain Gemini client initialization failed.")