import logging
import os
# Remove Anthropic client import
# from anthropic import Anthropic
# Add Google Generative AI client import
import google.generativeai as genai 
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

class KeywordGenerationService:
    """Generates search keywords from a description using Google Gemini."""

    def __init__(self):
        """Initializes the service and the Google Generative AI client."""
        # Check for Google API Key
        self.api_key = os.getenv("GEMINI_API_KEY")
        # Use GEMINI_MODEL env var, default to gemini-2.0-flash
        self.model_name = os.getenv("GEMINI_MODEL", "gemini-2.0-flash") 
        if not self.api_key:
            logger.error("GEMINI_API_KEY not found in environment variables.")
            raise ValueError("Google API key is required for Gemini.")
        
        try:
            # Configure the Google client
            genai.configure(api_key=self.api_key)
            # Create the model instance
            self.model = genai.GenerativeModel(self.model_name)
            logger.info(f"KeywordGenerationService initialized with Gemini model: {self.model_name}")
        except Exception as e:
            logger.exception("Failed to initialize Google Generative AI client.")
            raise RuntimeError("Could not initialize KeywordGenerationService") from e

    def generate_keywords(self, description: str, num_keywords: int = 20) -> list[str]:
        """
        Generates a list of search keywords based on the provided description using Gemini.

        Args:
            description: The user-provided description of the desired podcasts.
            num_keywords: The target number of keywords to generate.

        Returns:
            A list of generated keywords.
        """
        if not description:
            logger.warning("No description provided for keyword generation.")
            return []

        prompt = f"""Based on the following description of desired podcasts, please generate a concise list of exactly {num_keywords} relevant and diverse search keywords or short phrases (2-4 words max each) that could be used to find these podcasts using podcast search APIs. Focus on core topics, themes, niches, and potentially relevant guest types or industries mentioned. Output *only* the keywords, separated by newlines. Do not include numbering or any other text.

Description:
"{description}"

Keywords:"""

        try:
            logger.info(f"Generating {num_keywords} keywords using {self.model_name} for description: '{description[:100]}...'")
            
            # Configure generation parameters (optional)
            generation_config = genai.types.GenerationConfig(
                # candidate_count=1, # Default is 1
                # stop_sequences=None,
                # max_output_tokens=400, # Set token limit if needed
                temperature=0.5 
            )
            
            # Call the Gemini API
            response = self.model.generate_content(
                prompt,
                generation_config=generation_config
                # safety_settings=... # Add safety settings if needed
            )

            # Extract keywords from the response content
            if response.parts:
                raw_keywords = response.text.strip() # Access text directly
                keywords = [kw.strip() for kw in raw_keywords.split('\n') if kw.strip()]
                logger.info(f"Generated {len(keywords)} keywords: {keywords}")
                return keywords[:num_keywords] 
            else:
                 # Handle cases where the response might be blocked or empty
                 logger.warning(f"Gemini response for keyword generation was empty or blocked. Prompt: '{prompt[:100]}...'")
                 # Log safety feedback if available
                 if response.prompt_feedback:
                      logger.warning(f"Prompt Feedback: {response.prompt_feedback}")
                 if response.candidates and response.candidates[0].finish_reason != genai.types.FinishReason.STOP:
                     logger.warning(f"Finish Reason: {response.candidates[0].finish_reason}")
                 return []

        except Exception as e:
            logger.exception(f"Error generating keywords using Google Gemini API: {e}")
            return []

# Example usage (for testing)
if __name__ == '__main__':
    test_description = "Podcasts focused on early-stage SaaS startups, particularly those discussing product-market fit, bootstrapping, and interviews with founders who have recently raised seed funding in the fintech or healthtech sectors."
    # Ensure .env is loaded or keys are set when running standalone
    # load_dotenv() 
    service = KeywordGenerationService()
    generated_keywords = service.generate_keywords(test_description)
    print("Generated Keywords:")
    for kw in generated_keywords:
        print(f"- {kw}") 