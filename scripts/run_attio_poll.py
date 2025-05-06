# scripts/run_attio_poll.py

import logging
import os
import sys
from dotenv import load_dotenv

# Adjust the path to import from the src directory
# This assumes the script is run from the project root directory
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.services.polling_service import PollingService
from src.persistence.mongodb import connect_to_mongo, close_mongo_connection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Main function to run the Attio polling service."""
    logger.info("--- Starting Attio Polling Script ---")
    
    # Load environment variables from .env file
    load_dotenv()
    
    db_connection = None
    try:
        # Connect to MongoDB
        db_connection = connect_to_mongo()
        if not db_connection:
            logger.error("Failed to establish database connection. Exiting.")
            return

        # Initialize the PollingService
        # It will use the established DB connection via get_db()
        polling_service = PollingService()

        # Run the polling and syncing process
        polling_service.poll_and_sync_attio_status()

    except Exception as e:
        logger.exception(f"An error occurred during the polling process: {e}")
    finally:
        # Ensure the database connection is closed
        if db_connection:
            close_mongo_connection()
        logger.info("--- Attio Polling Script Finished ---")

if __name__ == "__main__":
    main() 