import os
import asyncio
import logging
from langchain_community.utilities.tavily_search import TavilySearchAPIWrapper
from dotenv import load_dotenv

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

# Initialize the TavilySearchAPIWrapper at module level
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")
if not TAVILY_API_KEY:
    logger.warning("TAVILY_API_KEY not found in environment variables. Tavily search will not work.")
    # You might want to raise an error or handle this more gracefully
    tavily_search = None 
else:
    try:
        tavily_search = TavilySearchAPIWrapper()
        logger.info("TavilySearchAPIWrapper initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize TavilySearchAPIWrapper: {e}")
        tavily_search = None

async def async_tavily_search(query: str, max_results: int = 3, search_depth: str = "advanced"):
    """Async wrapper for Tavily search.

    Args:
        query: The search query.
        max_results: The maximum number of results to return.
        search_depth: The depth of the search ("basic" or "advanced").

    Returns:
        A list of search results (dictionaries) or an empty list on error.
    """
    if not tavily_search:
        logger.error("TavilySearchAPIWrapper is not initialized. Cannot perform search.")
        return []
        
    try:
        logger.info(f"Performing Tavily search for query: '{query}' (max_results={max_results}, depth={search_depth})")
        # The TavilySearchAPIWrapper.results method is synchronous,
        # so we run it in a thread pool to make it non-blocking for asyncio.
        results = await asyncio.to_thread(
            tavily_search.results,
            query,
            max_results=max_results,
            search_depth=search_depth 
        )
        logger.info(f"Tavily search returned {len(results)} results.")
        # logger.debug(f"Tavily results: {results}") # Can be verbose
        return results
    except Exception as e:
        logger.error(f"Tavily search failed for query '{query}': {str(e)}")
        # Return empty results on error
        return []

if __name__ == "__main__":
    async def run_test():
        print("--- Testing Tavily Search Service ---")
        if not TAVILY_API_KEY:
            print("TAVILY_API_KEY is not set. Please set it in your .env file to run the test.")
            return

        # Test query
        test_query = "Instagram profile for 'Best of the Left' podcast"
        print(f"Test Query: {test_query}")

        results = await async_tavily_search(test_query, max_results=3)

        if results:
            print(f"\nFound {len(results)} results:")
            for i, result in enumerate(results):
                print(f"  Result {i+1}:")
                print(f"    Title: {result.get('title')}")
                print(f"    URL: {result.get('url')}")
                print(f"    Content Snippet: {result.get('content', '')[:200]}...") # Print first 200 chars of content
                print("-" * 20)
        else:
            print("\nNo results found or an error occurred.")
        
        # --- Test a more general query ---
        test_query_2 = "Official YouTube channel for 'The Daily' podcast by The New York Times"
        print(f"\nTest Query 2: {test_query_2}")
        results_2 = await async_tavily_search(test_query_2, max_results=2)
        if results_2:
            print(f"\nFound {len(results_2)} results:")
            for i, result in enumerate(results_2):
                print(f"  Result {i+1}:")
                print(f"    Title: {result.get('title')}")
                print(f"    URL: {result.get('url')}")
                print(f"    Content Snippet: {result.get('content', '')[:200]}...")
                print("-" * 20)
        else:
            print("\nNo results found for query 2 or an error occurred.")

    asyncio.run(run_test())
    print("\n--- Tavily Search Test Script Finished ---") 