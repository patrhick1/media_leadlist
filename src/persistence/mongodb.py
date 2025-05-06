import os
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import ConnectionFailure
from dotenv import load_dotenv
import logging
from typing import Type, TypeVar, Optional, List
from pydantic import BaseModel
import re
from datetime import date, datetime

# Import models relatively
from ..models.campaign import CampaignConfiguration
from ..models.lead import PodcastLead
from ..models.state import AgentState
from ..models.guests import Guest, GuestAppearance

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

MONGO_URI = os.getenv("MONGO_DB_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "podcast_vetting_db")

client: MongoClient | None = None
db = None

def connect_to_mongo():
    """Establishes a connection to the MongoDB database."""
    global client, db
    if client is not None:
        logger.info("Already connected to MongoDB.")
        return db

    if not MONGO_URI:
        logger.error("MONGO_DB_URI environment variable not set.")
        raise ValueError("MongoDB URI not configured")

    try:
        logger.info(f"Connecting to MongoDB at {MONGO_URI}...")
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000) # 5 second timeout
        # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster')
        db = client[MONGO_DB_NAME]
        logger.info(f"Successfully connected to MongoDB database: {MONGO_DB_NAME}")
        return db
    except ConnectionFailure as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        client = None
        db = None
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during MongoDB connection: {e}")
        client = None
        db = None
        raise

def get_db():
    """Returns the database instance, connecting if necessary."""
    if db is None:
        return connect_to_mongo()
    return db

def close_mongo_connection():
    """Closes the MongoDB connection."""
    global client, db
    if client:
        client.close()
        client = None
        db = None
        logger.info("MongoDB connection closed.")

# Define collection names
CAMPAIGNS_COLLECTION = "campaigns"
LEADS_COLLECTION = "leads"
STATES_COLLECTION = "agent_states"
GUESTS_COLLECTION = "guests"
GUEST_APPEARANCES_COLLECTION = "guest_appearances"

# Generic Type Variable for Pydantic models
T = TypeVar('T', bound=BaseModel)

def _get_collection(collection_name: str):
    """Gets a specific collection from the database."""
    db = get_db()
    if db is None:
        raise ConnectionFailure("Database connection not established.")
    return db[collection_name]

def initialize_collections():
    """Creates collections if they don't exist and ensures indexes."""
    try:
        db = get_db()
        if not db:
            logger.error("Cannot initialize collections, DB connection not available.")
            return
            
        collection_names = db.list_collection_names()
        
        # Campaign Collection
        if CAMPAIGNS_COLLECTION not in collection_names:
            db.create_collection(CAMPAIGNS_COLLECTION)
            logger.info(f"Created collection: {CAMPAIGNS_COLLECTION}")
        campaigns = db[CAMPAIGNS_COLLECTION]
        campaigns.create_index("campaign_id", unique=True)
        logger.info(f"Ensured index on campaign_id for {CAMPAIGNS_COLLECTION}")

        # Leads Collection
        if LEADS_COLLECTION not in collection_names:
            db.create_collection(LEADS_COLLECTION)
            logger.info(f"Created collection: {LEADS_COLLECTION}")
        leads = db[LEADS_COLLECTION]
        # Ensure existing and new indexes
        leads.create_index("podcast_id", unique=True) # Assuming podcast_id should be unique
        logger.info(f"Ensured unique index on podcast_id for {LEADS_COLLECTION}")
        leads.create_index("categories") # For neighborhood mapping
        logger.info(f"Ensured index on categories for {LEADS_COLLECTION}")
        leads.create_index("network")    # For neighborhood mapping
        logger.info(f"Ensured index on network for {LEADS_COLLECTION}")
        leads.create_index("tags")       # For neighborhood mapping (using 'tags' field)
        logger.info(f"Ensured index on tags for {LEADS_COLLECTION}")
        # Potentially add indexes for other common query fields if needed

        # Agent States Collection
        if STATES_COLLECTION not in collection_names:
            db.create_collection(STATES_COLLECTION)
            logger.info(f"Created collection: {STATES_COLLECTION}")
        states = db[STATES_COLLECTION]
        # Example indexes (adjust based on actual query patterns)
        states.create_index("execution_status")
        logger.info(f"Ensured index on execution_status for {STATES_COLLECTION}")
        states.create_index("current_step")
        logger.info(f"Ensured index on current_step for {STATES_COLLECTION}")
        # If AgentState gets a unique ID like campaign_id or a specific execution_id:
        # states.create_index("campaign_id", unique=True) 
        # logger.info(f"Ensured unique index on campaign_id for {STATES_COLLECTION}")

        # Guests Collection
        if GUESTS_COLLECTION not in collection_names:
            db.create_collection(GUESTS_COLLECTION)
            logger.info(f"Created collection: {GUESTS_COLLECTION}")
        guests = db[GUESTS_COLLECTION]
        guests.create_index("guest_id", unique=True)
        logger.info(f"Ensured unique index on guest_id for {GUESTS_COLLECTION}")
        # Create a compound text index for searching name and aliases
        guests.create_index([("name", "text"), ("aliases", "text")], name="guest_name_alias_text_index")
        logger.info(f"Ensured text index on name/aliases for {GUESTS_COLLECTION}")
        guests.create_index("popularity_score") # For potential sorting/filtering
        logger.info(f"Ensured index on popularity_score for {GUESTS_COLLECTION}")

        # Guest Appearances Collection
        if GUEST_APPEARANCES_COLLECTION not in collection_names:
            db.create_collection(GUEST_APPEARANCES_COLLECTION)
            logger.info(f"Created collection: {GUEST_APPEARANCES_COLLECTION}")
        appearances = db[GUEST_APPEARANCES_COLLECTION]
        appearances.create_index("appearance_id", unique=True)
        logger.info(f"Ensured unique index on appearance_id for {GUEST_APPEARANCES_COLLECTION}")
        appearances.create_index("guest_id")
        logger.info(f"Ensured index on guest_id for {GUEST_APPEARANCES_COLLECTION}")
        appearances.create_index("podcast_id")
        logger.info(f"Ensured index on podcast_id for {GUEST_APPEARANCES_COLLECTION}")
        appearances.create_index("appearance_date") # For sorting by recency
        logger.info(f"Ensured index on appearance_date for {GUEST_APPEARANCES_COLLECTION}")

        logger.info("Collections initialization and index check complete.")
    except Exception as e:
        logger.error(f"Error initializing collections or indexes: {e}")
        # Decide if this should re-raise or just log
        # raise # Uncomment to propagate the error

# --- CRUD Operations --- #

# Campaign Configuration CRUD
def save_campaign_config(config: CampaignConfiguration) -> CampaignConfiguration:
    """Saves or updates a campaign configuration."""
    collection = _get_collection(CAMPAIGNS_COLLECTION)
    config_dict = config.model_dump(mode='json')
    result = collection.find_one_and_replace(
        {"campaign_id": config.campaign_id},
        config_dict,
        upsert=True, # Insert if not found
        return_document=ReturnDocument.AFTER
    )
    logger.info(f"Saved/Updated campaign config: {config.campaign_id}")
    # The result should be the document saved, parse it back
    return CampaignConfiguration(**result)

def get_campaign_config(campaign_id: str) -> Optional[CampaignConfiguration]:
    """Retrieves a campaign configuration by its ID."""
    collection = _get_collection(CAMPAIGNS_COLLECTION)
    result = collection.find_one({"campaign_id": campaign_id})
    if result:
        # Remove MongoDB's internal _id before parsing
        result.pop('_id', None)
        return CampaignConfiguration(**result)
    logger.warning(f"Campaign config not found: {campaign_id}")
    return None

# Podcast Lead CRUD
def save_podcast_lead(lead: PodcastLead) -> PodcastLead:
    """Saves or updates a podcast lead."""
    # Assuming podcast_id is the unique identifier for a lead across all campaigns for now
    # If leads are campaign-specific, the filter/update logic might need adjustment
    collection = _get_collection(LEADS_COLLECTION)
    lead_dict = lead.model_dump(mode='json')
    result = collection.find_one_and_replace(
        {"podcast_id": lead.podcast_id},
        lead_dict,
        upsert=True,
        return_document=ReturnDocument.AFTER
    )
    logger.info(f"Saved/Updated podcast lead: {lead.podcast_id}")
    return PodcastLead(**result)

def save_podcast_leads(leads: List[PodcastLead]) -> List[PodcastLead]:
    """Saves multiple podcast leads (upserts based on podcast_id)."""
    # This is a basic implementation; for large volumes, consider bulk operations
    saved_leads = [save_podcast_lead(lead) for lead in leads]
    logger.info(f"Saved/Updated {len(saved_leads)} podcast leads.")
    return saved_leads

def get_podcast_leads(filter_criteria: Optional[dict] = None) -> List[PodcastLead]:
    """Retrieves podcast leads based on filter criteria."""
    collection = _get_collection(LEADS_COLLECTION)
    query = filter_criteria if filter_criteria else {}
    results = collection.find(query)
    leads = []
    for result in results:
        result.pop('_id', None)
        try:
            leads.append(PodcastLead(**result))
        except Exception as e:
            logger.error(f"Error parsing lead from DB: {result}. Error: {e}")
            # Optionally skip invalid leads or handle differently
    return leads

def get_podcast_lead(podcast_id: str) -> Optional[PodcastLead]:
    """Retrieves a single podcast lead by its podcast_id."""
    collection = _get_collection(LEADS_COLLECTION)
    result = collection.find_one({"podcast_id": podcast_id})
    if result:
        result.pop('_id', None)
        try:
            return PodcastLead(**result)
        except Exception as e:
            logger.error(f"Error parsing lead from DB: {result}. Error: {e}")
            return None
    logger.warning(f"Podcast lead not found for ID: {podcast_id}")
    return None

# Agent State CRUD
def save_agent_state(state: AgentState, state_id_field: str = 'campaign_id') -> AgentState:
    """Saves or updates an agent state, identified by a specific field (default: campaign_id)."""
    collection = _get_collection(STATES_COLLECTION)
    state_dict = state.model_dump(mode='json')
    # Use the specified field from the state object as the unique identifier
    identifier_value = getattr(state, state_id_field, None)
    if identifier_value is None:
         raise ValueError(f"State identifier field '{state_id_field}' not found or is None in AgentState")

    filter_query = {state_id_field: identifier_value}

    result = collection.find_one_and_replace(
        filter_query,
        state_dict,
        upsert=True,
        return_document=ReturnDocument.AFTER
    )
    logger.info(f"Saved/Updated agent state identified by {state_id_field}={identifier_value}")
    return AgentState(**result)

def get_agent_state(identifier_value: str, state_id_field: str = 'campaign_id') -> Optional[AgentState]:
    """Retrieves an agent state by a specific identifier field."""
    collection = _get_collection(STATES_COLLECTION)
    filter_query = {state_id_field: identifier_value}
    result = collection.find_one(filter_query)
    if result:
        result.pop('_id', None)
        return AgentState(**result)
    logger.warning(f"Agent state not found for {state_id_field}={identifier_value}")
    return None

def update_agent_state(identifier_value: str, update_data: dict, state_id_field: str = 'campaign_id') -> Optional[AgentState]:
    """Updates specific fields of an agent state."""
    collection = _get_collection(STATES_COLLECTION)
    filter_query = {state_id_field: identifier_value}
    update_doc = {'$set': update_data}

    result = collection.find_one_and_update(
        filter_query,
        update_doc,
        return_document=ReturnDocument.AFTER
    )
    if result:
        logger.info(f"Partially updated agent state for {state_id_field}={identifier_value}")
        result.pop('_id', None)
        return AgentState(**result)
    logger.warning(f"Agent state not found for update: {state_id_field}={identifier_value}")
    return None

# --- Guest CRUD (Added) ---

def save_guest(guest: Guest) -> Guest:
    """Saves or updates a guest entity."""
    collection = _get_collection(GUESTS_COLLECTION)
    guest_dict = guest.model_dump(mode='json')
    # Convert date fields if necessary (Pydantic v2 should handle this better)
    result = collection.find_one_and_replace(
        {"guest_id": guest.guest_id},
        guest_dict,
        upsert=True,
        return_document=ReturnDocument.AFTER
    )
    if not result:
         # Should not happen with upsert=True and AFTER, but handle defensively
         logger.error(f"Failed to save/update guest {guest.guest_id}, find_one_and_replace returned None")
         raise RuntimeError(f"Failed to save/update guest {guest.guest_id}")
    logger.info(f"Saved/Updated guest: {guest.guest_id}")
    return Guest(**result)

def get_guest(guest_id: str) -> Optional[Guest]:
    """Retrieves a guest by their unique ID."""
    collection = _get_collection(GUESTS_COLLECTION)
    result = collection.find_one({"guest_id": guest_id})
    if result:
        result.pop('_id', None)
        return Guest(**result)
    return None

def find_guests(filter_criteria: dict, limit: int = 100) -> List[Guest]:
    """Finds guests based on various criteria (e.g., text search)."""
    collection = _get_collection(GUESTS_COLLECTION)
    results = collection.find(filter_criteria).limit(limit)
    guests = []
    for result in results:
        result.pop('_id', None)
        try:
            guests.append(Guest(**result))
        except Exception as e:
            logger.error(f"Error parsing guest from DB: {result}. Error: {e}")
    return guests

# --- Guest Appearance CRUD (Added) ---

def save_appearance(appearance: GuestAppearance) -> GuestAppearance:
    """Saves or updates a guest appearance."""
    collection = _get_collection(GUEST_APPEARANCES_COLLECTION)
    appearance_dict = appearance.model_dump(mode='json')
    # Pydantic v2 handles date serialization well, ensure it's datetime for mongo
    if isinstance(appearance_dict.get('appearance_date'), date):
         appearance_dict['appearance_date'] = datetime.combine(appearance_dict['appearance_date'], datetime.min.time())

    result = collection.find_one_and_replace(
        {"appearance_id": appearance.appearance_id},
        appearance_dict,
        upsert=True,
        return_document=ReturnDocument.AFTER
    )
    if not result:
         logger.error(f"Failed to save/update appearance {appearance.appearance_id}")
         raise RuntimeError(f"Failed to save/update appearance {appearance.appearance_id}")
    logger.info(f"Saved/Updated appearance: {appearance.appearance_id}")
    # Convert datetime back to date if needed upon retrieval
    if isinstance(result.get('appearance_date'), datetime):
         result['appearance_date'] = result['appearance_date'].date()
    return GuestAppearance(**result)

def get_appearances(filter_criteria: dict, sort_by: Optional[str] = None, limit: int = 100) -> List[GuestAppearance]:
    """Retrieves guest appearances based on filter criteria."""
    collection = _get_collection(GUEST_APPEARANCES_COLLECTION)
    cursor = collection.find(filter_criteria)
    if sort_by:
        # Add basic sorting, e.g., sort_by='appearance_date' or '-appearance_date'
        sort_field = sort_by.lstrip('-')
        sort_order = -1 if sort_by.startswith('-') else 1
        cursor = cursor.sort(sort_field, sort_order)
    
    results = cursor.limit(limit)
    appearances = []
    for result in results:
        result.pop('_id', None)
        # Convert datetime back to date if needed
        if isinstance(result.get('appearance_date'), datetime):
            result['appearance_date'] = result['appearance_date'].date()
        try:
            appearances.append(GuestAppearance(**result))
        except Exception as e:
            logger.error(f"Error parsing appearance from DB: {result}. Error: {e}")
    return appearances

# Example: Get appearances for a specific guest, sorted by date descending
# get_appearances({"guest_id": "guest123"}, sort_by='-appearance_date', limit=50)

# --- End CRUD Operations --- #

# Example Usage (optional, for testing)
if __name__ == "__main__":
    try:
        connect_to_mongo()
        initialize_collections() # Ensure collections and indexes exist

        # Example: Create and save a campaign
        # campaign = CampaignConfiguration(
        #     target_audience="Tech Startups",
        #     key_messages=["Increase efficiency", "Scale faster"],
        #     tone_preferences="Professional but approachable"
        # )
        # saved_campaign = save_campaign_config(campaign)
        # print(f"Saved Campaign: {saved_campaign.campaign_id}")
        # retrieved_campaign = get_campaign_config(saved_campaign.campaign_id)
        # print(f"Retrieved Campaign: {retrieved_campaign}")

        # Example: Create and save leads
        # lead1 = PodcastLead(podcast_id="pod123", name="Tech Talks", description="Weekly tech news")
        # lead2 = PodcastLead(podcast_id="pod456", name="Startup Stories", description="Interviews with founders")
        # saved_leads = save_podcast_leads([lead1, lead2])
        # print(f"Saved {len(saved_leads)} leads.")
        # all_leads = get_podcast_leads()
        # print(f"Retrieved {len(all_leads)} leads.")

        # Example: Create and save agent state
        # if retrieved_campaign:
        #     agent_state = AgentState(
        #         current_step="initial",
        #         campaign_config=retrieved_campaign,
        #         execution_status="running"
        #     )
        #     saved_state = save_agent_state(agent_state)
        #     print(f"Saved Agent State for campaign: {saved_state.campaign_config.campaign_id}")
        #     retrieved_state = get_agent_state(saved_state.campaign_config.campaign_id)
        #     print(f"Retrieved State: {retrieved_state.current_step}, Status: {retrieved_state.execution_status}")
        #     # Example Update
        #     updated_state = update_agent_state(retrieved_campaign.campaign_id, {"current_step": "discovery", "execution_status": "active"})
        #     print(f"Updated State: {updated_state.current_step}, Status: {updated_state.execution_status}")

    except ValueError as e:
        print(f"Configuration Error: {e}")
    except ConnectionFailure as e:
        print(f"Database Connection Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        close_mongo_connection() 