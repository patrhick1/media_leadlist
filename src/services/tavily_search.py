import os
import asyncio
import logging
from langchain_community.utilities.tavily_search import TavilySearchAPIWrapper # Keep for type hint reference if any, but main client will change
from tavily import TavilyClient # Import the direct TavilyClient
from dotenv import load_dotenv
from typing import Dict, Any, Optional # Added for type hinting

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

# Initialize the TavilyClient at module level
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")
_tavily_client: Optional[TavilyClient] = None # Changed variable name and added type hint

if not TAVILY_API_KEY:
    logger.warning("TAVILY_API_KEY not found in environment variables. Tavily search will not work.")
    # You might want to raise an error or handle this more gracefully
    # _tavily_client remains None
else:
    try:
        _tavily_client = TavilyClient(api_key=TAVILY_API_KEY) # Use TavilyClient
        logger.info("TavilyClient initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize TavilyClient: {e}")
        # _tavily_client remains None

async def async_tavily_search(
    query: str, 
    max_results: int = 3, 
    search_depth: str = "advanced",
    include_answer: bool = False  # New parameter to control fetching the LLM answer
) -> Optional[Dict[str, Any]]: # Return type is now the full response dict or None
    """Async wrapper for Tavily search using the direct TavilyClient.

    Args:
        query: The search query.
        max_results: The maximum number of results to return.
        search_depth: The depth of the search ("basic" or "advanced").
        include_answer: Whether to include the LLM-generated answer in the response.

    Returns:
        A dictionary containing the Tavily search response (including 'answer' if requested
        and available, and 'results' list) or None on error or if client not initialized.
    """
    if not _tavily_client:
        logger.error("TavilyClient is not initialized. Cannot perform search.")
        return None # Return None if client is not available
        
    try:
        log_message = (
            f"Performing Tavily search for query: '{query}' "
            f"(max_results={max_results}, depth={search_depth}, include_answer={include_answer})"
        )
        logger.info(log_message)

        search_kwargs = {
            "query": query,
            "max_results": max_results,
            "search_depth": search_depth,
            "include_answer": include_answer # Pass the boolean directly
        }
        
        # The TavilyClient.search method is synchronous,
        # so we run it in a thread pool to make it non-blocking for asyncio.
        response_data = await asyncio.to_thread(
            _tavily_client.search,
            **search_kwargs
        )
        
        answer_available = bool(response_data.get('answer'))
        results_count = len(response_data.get('results', []))
        logger.info(f"Tavily search returned. Answer available: {answer_available}. Results count: {results_count}.")
        # logger.debug(f"Tavily response data: {response_data}") # Can be very verbose
        return response_data # Return the full dictionary
    except Exception as e:
        logger.error(f"Tavily search failed for query '{query}': {str(e)}")
        # Return None on error
        return None

if __name__ == "__main__":
    async def run_test():
        print("--- Testing Tavily Search Service ---")
        if not TAVILY_API_KEY:
            print("TAVILY_API_KEY is not set. Please set it in your .env file to run the test.")
            return

        # Test query 1: with include_answer=True
        test_query_1 = "Instagram profile for 'Best of the Left' podcast"
        print(f"Test Query 1: {test_query_1} (with include_answer=True)")

        response_1 = await async_tavily_search(test_query_1, max_results=3, include_answer=True)

        if response_1:
            print(f"\nQuery 1 Response (Full):")
            # print(response_1) # Raw print
            if response_1.get("answer"):
                print(f"  Answer: {response_1['answer']}")
            else:
                print("  No answer provided in response.")
            
            results_1 = response_1.get("results", [])
            print(f"  Found {len(results_1)} result snippets:")
            for i, result in enumerate(results_1):
                print(f"    Result {i+1}:")
                print(f"      Title: {result.get('title')}")
                print(f"      URL: {result.get('url')}")
                print(f"      Content Snippet: {result.get('content', '')[:100]}...") # Print first 100 chars
                print("-" * 10)
        else:
            print("\nNo results found for query 1 or an error occurred.")
        
        # Test query 2: default include_answer=False
        test_query_2 = "Official YouTube channel for 'The Daily' podcast by The New York Times"
        print(f"\nTest Query 2: {test_query_2} (with include_answer=False)")
        response_2 = await async_tavily_search(test_query_2, max_results=2) # include_answer defaults to False
        
        if response_2:
            print(f"\nQuery 2 Response (Full):")
            # print(response_2) # Raw print
            if response_2.get("answer"): # Should be None or not present
                print(f"  Answer: {response_2['answer']}")
            else:
                print("  No answer provided in response (as expected for include_answer=False).")

            results_2 = response_2.get("results", [])
            print(f"  Found {len(results_2)} result snippets:")
            for i, result in enumerate(results_2):
                print(f"    Result {i+1}:")
                print(f"      Title: {result.get('title')}")
                print(f"      URL: {result.get('url')}")
                print(f"      Content Snippet: {result.get('content', '')[:100]}...")
                print("-" * 10)
        else:
            print("\nNo results found for query 2 or an error occurred.")

    asyncio.run(run_test())
    print("\n--- Tavily Search Test Script Finished ---") 