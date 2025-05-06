import os
import logging
from typing import List, Optional, Type, Generator
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, String, Text, Float, ARRAY
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from sqlalchemy.exc import SQLAlchemyError

# Assuming PodcastLead model exists for type hinting and reference
from ..models.lead import PodcastLead

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Database Configuration from Environment Variables
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# SQLAlchemy setup
engine = None
SessionLocal = None
Base = declarative_base()

# --- Database Models ---

class Media(Base):
    __tablename__ = "media"

    # Map fields from PodcastLead
    podcast_id = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False, index=True)
    description = Column(Text)
    email = Column(String, nullable=True)
    host_information = Column(Text, nullable=True)
    audience_demographics = Column(Text, nullable=True)
    contact_details = Column(Text, nullable=True)
    rss_url = Column(String, nullable=True)
    relevance_score = Column(Float, nullable=True)
    categories = Column(ARRAY(String), nullable=True)
    network = Column(String, nullable=True, index=True)
    tags = Column(ARRAY(String), nullable=True)

    def __repr__(self):
        return f"<Media(podcast_id='{self.podcast_id}', name='{self.name}')>"

# --- Database Connection Management ---

def connect_to_postgres():
    """Establishes the database connection and sessionmaker."""
    global engine, SessionLocal
    if engine is not None:
        logger.info("Already connected to PostgreSQL.")
        return

    if not all([DB_USER, DB_PASSWORD, DB_NAME]):
        logger.error("Missing PostgreSQL connection details in environment variables.")
        raise ValueError("PostgreSQL connection details not fully configured.")

    try:
        logger.info(f"Connecting to PostgreSQL database: {DB_NAME} at {DB_HOST}:{DB_PORT}")
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        # Test connection
        with engine.connect() as connection:
             logger.info("Successfully connected to PostgreSQL.")
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    except SQLAlchemyError as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        engine = None
        SessionLocal = None
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during PostgreSQL connection: {e}")
        engine = None
        SessionLocal = None
        raise

def get_db() -> Generator[Session, None, None]:
    """Provides a database session."""
    if SessionLocal is None:
        # Attempt to connect if not already connected
        try:
            connect_to_postgres()
        except Exception:
             logger.error("Failed to get DB session: Connection not established.")
             return None
    if SessionLocal is None:
         logger.error("SessionLocal is still None after connection attempt.")
         return None
         
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def create_tables():
    """Creates database tables based on the defined models."""
    if engine is None:
        logger.error("Cannot create tables, PostgreSQL engine not initialized.")
        try:
            connect_to_postgres()
        except Exception as e:
             logger.error(f"Failed to connect before creating tables: {e}")
             return # Exit if connection fails
             
    if engine is None:
         logger.error("Engine still None after connection attempt in create_tables.")
         return
         
    try:
        logger.info("Creating database tables...")
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables checked/created successfully.")
    except SQLAlchemyError as e:
        logger.error(f"Error creating PostgreSQL tables: {e}")
        # Depending on the error, you might want to raise it
    except Exception as e:
        logger.error(f"An unexpected error occurred during table creation: {e}")


# --- CRUD Operations (Stubs) ---

def save_media_item(db: Session, lead: PodcastLead) -> Optional[Media]:
    """Saves or updates a single media item (PodcastLead) in the database."""
    logger.debug(f"Attempting to save/update media item: {lead.podcast_id}")
    # TODO: Implement upsert logic
    # Example:
    # existing_item = db.query(Media).filter(Media.podcast_id == lead.podcast_id).first()
    # if existing_item:
    #     # Update fields
    #     for key, value in lead.model_dump().items():
    #         setattr(existing_item, key, value)
    #     logger.info(f"Updating existing media item: {lead.podcast_id}")
    # else:
    #     # Create new
    #     new_item = Media(**lead.model_dump())
    #     db.add(new_item)
    #     logger.info(f"Adding new media item: {lead.podcast_id}")
    #     existing_item = new_item # Return the new item
    # try:
    #     db.commit()
    #     db.refresh(existing_item) # Refresh to get any DB-generated values
    #     return existing_item
    # except SQLAlchemyError as e:
    #     db.rollback()
    #     logger.error(f"Error saving media item {lead.podcast_id}: {e}")
    #     return None
    try:
        existing_item = db.query(Media).filter(Media.podcast_id == lead.podcast_id).first()
        if existing_item:
            # Update existing item
            logger.info(f"Updating existing media item: {lead.podcast_id}")
            # Create a dictionary from the Pydantic model, excluding unset fields if necessary
            update_data = lead.model_dump(exclude_unset=True) 
            for key, value in update_data.items():
                if hasattr(existing_item, key):
                    setattr(existing_item, key, value)
            item_to_return = existing_item
        else:
            # Create new item
            logger.info(f"Adding new media item: {lead.podcast_id}")
            # Ensure all required fields are present if creating new
            new_item = Media(**lead.model_dump()) 
            db.add(new_item)
            item_to_return = new_item # Return the newly created item instance

        db.commit()
        db.refresh(item_to_return) # Refresh to get any DB-generated values or state
        return item_to_return
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Database error saving media item {lead.podcast_id}: {e}")
        return None
    except Exception as e:
        db.rollback()
        logger.error(f"Unexpected error saving media item {lead.podcast_id}: {e}")
        return None


def save_media_items(db: Session, leads: List[PodcastLead]) -> List[Media]:
    """Saves or updates multiple media items (PodcastLeads) in the database."""
    logger.debug(f"Attempting to save/update {len(leads)} media items.")
    # TODO: Implement bulk upsert logic for efficiency
    # Example (simple iteration, refine for bulk):
    # saved_items = []
    # for lead in leads:
    #     saved = save_media_item(db, lead) # Uses the single save logic
    #     if saved:
    #         saved_items.append(saved)
    # return saved_items
    # pass # Replace with actual implementation
    # return [] # Placeholder
    saved_items: List[Media] = []
    successful_count = 0
    failed_count = 0
    # Note: This iterates and commits one by one. For high performance,
    # consider SQLAlchemy bulk operations or session.merge().
    for lead in leads:
        try:
            # Use merge for a more concise upsert pattern
            logger.debug(f"Merging media item: {lead.podcast_id}")
            # Create a dictionary of the data to merge
            lead_data = lead.model_dump()
            # Create a managed Media object or update an existing one
            merged_item = db.merge(Media(**lead_data))
            # `merge` returns the persisted object, but doesn't automatically add it 
            # to the list unless it was already persisted or newly added in this session.
            # It's safer to query after commit or handle based on merge's behavior.
            # For simplicity here, we assume merge handles the upsert correctly
            # and we'll retrieve the object state after commit.
            # Let's commit after each merge for simplicity now. Bulk is better.
            db.commit()
            # Refresh the specific merged item to ensure its state is current
            # This might require querying it again if merge doesn't track it perfectly for refresh
            # Let's append the potentially updated/inserted object based on the data
            # We might need to query it back if we need the DB state *immediately*
            # For now, just count success/failure based on the commit attempt per item
            saved_items.append(merged_item) # Appending the merged object
            successful_count += 1
        except SQLAlchemyError as e:
            db.rollback()
            logger.error(f"Database error saving media item {lead.podcast_id}: {e}. Skipping this item.")
            failed_count += 1
        except Exception as e:
            db.rollback()
            logger.error(f"Unexpected error saving media item {lead.podcast_id}: {e}. Skipping this item.")
            failed_count += 1

    # Alternative: Add all then commit once (better, but error handling is broader)
    # items_to_save = [Media(**lead.model_dump()) for lead in leads]
    # try:
    #    # Using session.merge() might be better here within a loop or using bulk_save_objects
    #    db.add_all(items_to_save) # This won't update existing ones easily
    #    db.commit()
    #    successful_count = len(items_to_save)
    #    saved_items = items_to_save # Note: These objects might not be refreshed
    # except SQLAlchemyError as e:
    #    db.rollback()
    #    logger.error(f"Database error during bulk save: {e}")
    #    # More complex logic needed to know which ones failed
    # except Exception as e:
    #    db.rollback()
    #    logger.error(f"Unexpected error during bulk save: {e}")
    
    logger.info(f"Finished saving media items. Successful: {successful_count}, Failed: {failed_count}")
    # Returning the list of objects processed, potentially with updated state from merge/commit
    # For definitive state, a re-query might be needed depending on exact transaction handling.
    return saved_items


def get_media_item(db: Session, podcast_id: str) -> Optional[Media]:
    """Retrieves a single media item by its podcast_id."""
    logger.debug(f"Querying for media item with ID: {podcast_id}")
    # TODO: Implement query
    # Example:
    # try:
    #     return db.query(Media).filter(Media.podcast_id == podcast_id).first()
    # except SQLAlchemyError as e:
    #     logger.error(f"Error retrieving media item {podcast_id}: {e}")
    #     return None
    # pass # Replace with actual implementation
    try:
        return db.query(Media).filter(Media.podcast_id == podcast_id).first()
    except SQLAlchemyError as e:
        logger.error(f"Database error retrieving media item {podcast_id}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error retrieving media item {podcast_id}: {e}")
        return None


def get_media_items(db: Session, filter_criteria: Optional[dict] = None, limit: int = 1000) -> List[Media]:
    """Retrieves media items based on filter criteria."""
    logger.debug(f"Querying for media items with criteria: {filter_criteria}, limit: {limit}")
    # TODO: Implement query with filtering
    # Example:
    # try:
    #     query = db.query(Media)
    #     if filter_criteria:
    #         # Basic filtering example, needs refinement for complex queries
    #         for key, value in filter_criteria.items():
    #             if hasattr(Media, key):
    #                 # Handle potential list/array filtering differently if needed
    #                 query = query.filter(getattr(Media, key) == value)
    #             else:
    #                 logger.warning(f"Ignoring unknown filter key: {key}")
    #     return query.limit(limit).all()
    # except SQLAlchemyError as e:
    #     logger.error(f"Error retrieving media items: {e}")
    #     return []
    # pass # Replace with actual implementation
    # return [] # Placeholder
    try:
        query = db.query(Media)
        if filter_criteria:
            for key, value in filter_criteria.items():
                if hasattr(Media, key):
                    column = getattr(Media, key)
                    # Basic equality filter, may need adjustment for specific types like ARRAY
                    # For ARRAY containment, you might use column.contains([value]) or similar
                    if isinstance(value, list):
                        # Example: Assumes you want rows where the column contains ANY of the list items
                        # This might need adjustment based on exact requirement (ANY vs ALL)
                        # query = query.filter(column.contains(value)) # Requires ARRAY type usually
                        # For simple equality on the whole list (if supported/makes sense)
                        # query = query.filter(column == value)
                        # Or filter by individual elements if that's the intent:
                        # query = query.filter(column.any(v for v in value)) # Example for checking any element
                        logger.warning(f"Applying basic equality filter for list type column: {key}. Review if specific array operations are needed.")
                        query = query.filter(column == value) # Apply the filter
                    else:
                        query = query.filter(column == value) # Apply filter for non-list types
                else:
                    logger.warning(f"Filter key '{key}' not found in Media model, ignoring.")

        return query.limit(limit).all()
    except SQLAlchemyError as e:
        logger.error(f"Database error retrieving media items: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error retrieving media items: {e}")
        return []

# --- Example Usage (Optional) ---
if __name__ == "__main__":
    print("Running PostgreSQL persistence example...")
    # Ensure environment variables are set before running
    try:
        connect_to_postgres()
        create_tables()

        # Example Usage:
        # Note: Need to manage the session scope explicitly when calling directly
        db_gen = get_db()
        if db_gen:
             db = next(db_gen) # Get the session
             try:
                 # Create dummy data
                 lead1 = PodcastLead(
                     podcast_id="pg_test_1",
                     name="Postgres Pod",
                     description="A podcast about SQL",
                     categories=["Technology", "Databases"],
                     tags=["sql", "postgres"]
                 )
                 lead2 = PodcastLead(
                     podcast_id="pg_test_2",
                     name="Another DB Show",
                     description="More database fun",
                     email="test@example.com",
                     relevance_score=0.85,
                     network="DataNet"
                 )

                 # Save items (using placeholder function names for now)
                 # saved_item1 = save_media_item(db, lead1)
                 # saved_items = save_media_items(db, [lead1, lead2]) # Assuming bulk save
                 # print(f"Saved item 1: {saved_item1}")
                 # print(f"Saved {len(saved_items)} items in bulk.")

                 # Get items
                 # retrieved_item = get_media_item(db, "pg_test_1")
                 # print(f"Retrieved item 1: {retrieved_item}")
                 # all_items = get_media_items(db)
                 # print(f"Retrieved {len(all_items)} total items.")
                 # filtered_items = get_media_items(db, filter_criteria={"network": "DataNet"})
                 # print(f"Retrieved {len(filtered_items)} items from network DataNet.")

                 print("\nExample operations complete (using stubs). Implement CRUD functions.")

             except Exception as e:
                 print(f"Error during example operations: {e}")
             finally:
                 db.close() # Close the session obtained from get_db()
        else:
             print("Could not get database session.")

    except ValueError as e:
        print(f"Configuration Error: {e}")
    except SQLAlchemyError as e:
        print(f"Database Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    print("PostgreSQL example finished.") 