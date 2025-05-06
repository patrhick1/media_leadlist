import logging
from typing import Optional, AsyncIterator
from datetime import datetime # Import datetime for thread_ts
import os

# LangGraph imports
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.checkpoint.base import Checkpoint, CheckpointMetadata, CheckpointTuple
from langchain_core.runnables.config import RunnableConfig # Updated import path

# --- MongoDB/Beanie Imports (COMMENTED OUT) ---
# from beanie import Document, Indexed, init_beanie, Link # Use Link if referencing other Beanie docs
# from motor.motor_asyncio import AsyncIOMotorClient
# from pymongo import ASCENDING, IndexModel

# Import config and existing db functions
# Note: The custom checkpointer assumes Beanie is initialized elsewhere
# or needs initialization logic added. We'll rely on the startup event
# in main.py for basic MongoDB connection, but Beanie init might be needed.
# from ..config import MONGO_URI, DB_NAME, CHECKPOINTS_COLLECTION # Removed - Use Settings object via DI
from ..config import Settings # Import Settings class for type hinting
# We might not need get_db/close_mongo_connection directly here if Beanie handles it
# from .mongodb import get_db, close_mongo_connection 

logger = logging.getLogger(__name__)

# --- Custom Checkpointer Implementation (COMMENTED OUT) ---
# Source: https://stackoverflow.com/questions/78426461/nosql-database-mongodb-checkpointer-classes-in-langgraph
# User: longdistancerunner

# --- Define the Beanie Document model for storing checkpoints (COMMENTED OUT) ---
# class CheckPoints(Document):
#   thread_id: Indexed[str] # Index for faster lookup
#   thread_ts: Indexed[str] # Timestamp/ID of the checkpoint state
#   parent_ts: Optional[str] = None # Timestamp/ID of the parent state
#   # Store checkpoint and metadata as serialized strings (like the SQLite example)
#   # Consider using Pydantic fields or EmbeddedDocument for richer querying if needed
#   checkpoint: str
#   metadata: Optional[str] = None # Make metadata optional based on original code

#     class Settings():
#       name = CHECKPOINTS_COLLECTION # Use collection name from config
#       keep_nulls = False # Save space by not storing None values
#       # Example compound index matching SQLite structure
#       # Note: Beanie automatically creates an index on `id`
#       indexes = [
#           IndexModel(
#               [("thread_id", ASCENDING), ("thread_ts", ASCENDING)],
#               # unique=True, # Original had unique, might cause issues if retrying steps? Review if needed.
#               name="thread_ts_idx"
#           ),
#           # Add other indexes as needed, e.g., on thread_id alone
#           [("thread_id", ASCENDING)],
#       ]

# --- Define the Async Checkpointer Class (COMMENTED OUT) ---
# class AsyncMongoDBSaver(BaseCheckpointSaver):
#     """
#     An asynchronous MongoDB checkpointer implementation using Beanie ODM.
#     Based on the SQLiteSaver structure and Stack Overflow example.
#     """
#     serde = JsonPlusSerializer() # Use the default serializer

#     def __init__(self):
#         # Initialization logic here if needed (e.g., ensuring Beanie is initialized)
#         # For now, assume Beanie init happens elsewhere (e.g., FastAPI startup)
#         super().__init__(serde=self.serde) # Pass the serializer to the parent

#     @classmethod
#     async def from_conn_string(
#         cls, conn_string: str, db_name: str, collection_name: str = CHECKPOINTS_COLLECTION
#     ) -> "AsyncMongoDBSaver":
#         """Alternative constructor to initialize Beanie and return an instance."""
#         # This requires initializing Beanie globally or managing the client connection
#         # Be cautious with global initializations in complex apps.
#         client = AsyncIOMotorClient(conn_string)
#         db = client[db_name]
#         # Ensure the CheckPoints model is known to Beanie
#         await init_beanie(database=db, document_models=[CheckPoints])
#         logger.info(f"Beanie initialized for checkpointer via conn string: db='{db_name}', collection='{collection_name}'")
#         # Update Settings dynamically if needed (not standard Beanie practice)
#         # CheckPoints.Settings.name = collection_name
#         return cls()

#     async def aget_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
#         """Get the checkpoint tuple for a given config."""
#         thread_id = config["configurable"]["thread_id"]
#         # thread_ts is the specific checkpoint ID to fetch (optional)
#         thread_ts = config["configurable"].get("thread_ts")

#         if thread_ts:
#             # Fetch specific checkpoint by thread_id and thread_ts (checkpoint ID)
#             checkpoint_doc = await CheckPoints.find_one(
#                 CheckPoints.thread_id == thread_id, CheckPoints.thread_ts == thread_ts
#             )
#         else:
#             # Fetch the latest checkpoint for the thread_id
#             checkpoint_doc = await CheckPoints.find(
#                 CheckPoints.thread_id == thread_id
#             ).sort("-thread_ts").limit(1).first_or_none() # Use first_or_none

#         if checkpoint_doc:
#             parent_config = (
#                 {"configurable": {"thread_id": thread_id, "thread_ts": checkpoint_doc.parent_ts}}
#                 if checkpoint_doc.parent_ts
#                 else None
#             )
#             # Return the CheckpointTuple with deserialized data
#             return CheckpointTuple(
#                 config={"configurable": {"thread_id": thread_id, "thread_ts": checkpoint_doc.thread_ts}}, # Config for THIS checkpoint
#                 checkpoint=self.serde.loads(checkpoint_doc.checkpoint),
#                 metadata=self.serde.loads(checkpoint_doc.metadata) if checkpoint_doc.metadata else None,
#                 parent_config=parent_config,
#             )
#         return None

#     async def alist(
#         self,
#         config: RunnableConfig,
#         *,
#         before: Optional[RunnableConfig] = None,
#         limit: Optional[int] = None,
#         # filter: Optional[Dict[str, Any]] = None, # Metadata filter (complex to implement with serialized metadata)
#     ) -> AsyncIterator[CheckpointTuple]:
#         """List checkpoints for a thread, optionally filtering."""
#         thread_id = config["configurable"]["thread_id"]
#         query = CheckPoints.find(CheckPoints.thread_id == thread_id)

#         # Apply 'before' filter based on thread_ts (checkpoint ID)
#         if before and "thread_ts" in before["configurable"]:
#             query = query.find(CheckPoints.thread_ts < before["configurable"]["thread_ts"])

#         # Apply sorting (latest first)
#         query = query.sort("-thread_ts")

#         # Apply limit
#         if limit is not None:
#             query = query.limit(limit)

#         # TODO: Implement metadata filtering if needed.
#         # This is tricky because metadata is stored as a serialized string.
#         # Would require either deserializing during query (inefficient)
#         # or storing metadata fields directly in the CheckPoints document.
#         # if filter:
#         #     logger.warning("Metadata filtering is not implemented for AsyncMongoDBSaver.")
#             # Example conceptual filter (if metadata was a dict field):
#             # mongo_filter = {"metadata." + k: v for k, v in filter.items()}
#             # query = query.find(mongo_filter)

#         # Yield CheckpointTuples
#         async for checkpoint_doc in query:
#             parent_config = (
#                  {"configurable": {"thread_id": thread_id, "thread_ts": checkpoint_doc.parent_ts}}
#                  if checkpoint_doc.parent_ts
#                  else None
#             )
#             yield CheckpointTuple(
#                 config={"configurable": {"thread_id": checkpoint_doc.thread_id, "thread_ts": checkpoint_doc.thread_ts}},
#                 checkpoint=self.serde.loads(checkpoint_doc.checkpoint),
#                 metadata=self.serde.loads(checkpoint_doc.metadata) if checkpoint_doc.metadata else None,
#                 parent_config=parent_config,
#             )

#     async def aput(
#         self, config: RunnableConfig, checkpoint: Checkpoint, metadata: CheckpointMetadata
#     ) -> RunnableConfig:
#         """Save a checkpoint tuple."""
#         # Checkpoint ID is usually a timestamp or UUID managed by LangGraph internally
#         checkpoint_id = checkpoint["id"]
#         
#         cp_doc = CheckPoints(
#             thread_id=config["configurable"]["thread_id"],
#             thread_ts=checkpoint_id,
#             # Parent thread_ts comes from the config if resuming/branching
#             parent_ts=config["configurable"].get("thread_ts"),
#             checkpoint=self.serde.dumps(checkpoint), # Serialize Checkpoint dict
#             metadata=self.serde.dumps(metadata) if metadata else None, # Serialize Metadata dict
#         )
#         # Use Beanie's save method (upserts based on _id, which Beanie manages)
#         # Or use insert directly if duplicates based on (thread_id, thread_ts) are impossible/handled
#         # Using insert might be safer if unique index is enabled
#         # await cp_doc.insert()
#         await cp_doc.save() # Using save for potential upsert behavior if needed, matches original example

#         # Return the config for the checkpoint just saved
#         return RunnableConfig(
#             configurable={
#                 "thread_id": config["configurable"]["thread_id"],
#                 "thread_ts": checkpoint_id,
#             }
#         )

# --- Function to get the checkpointer instance (MODIFIED) ---
_checkpointer_instance = None

def get_checkpoint_saver() -> Optional[BaseCheckpointSaver]: # Return Optional
    """
    Configures and returns a checkpointer instance. MongoDB functionality is currently PAUSED.
    Returns None as checkpointing is disabled.
    """
    logger.warning("MongoDB checkpointing is currently disabled. Returning None for checkpointer.")
    return None
    # global _checkpointer_instance
    # # This basic singleton approach might have issues in highly concurrent scenarios
    # # Consider more robust dependency injection if needed.
    # if _checkpointer_instance is None:
    #     try:
    #         _checkpointer_instance = AsyncMongoDBSaver()
    #         logger.info("AsyncMongoDBSaver checkpointer created.")
    #     except Exception as e:
    #         logger.error(f"Error creating AsyncMongoDBSaver instance: {e}")
    #         raise RuntimeError("Failed to initialize checkpoint saver.") from e
    # return _checkpointer_instance

# --- Beanie Initialization (MODIFIED) ---
async def initialize_beanie_for_checkpointer(settings: Settings): # Accept settings object
    """Initializes Beanie using the provided Settings object. Functionality is currently PAUSED."""
    logger.warning("Beanie initialization for checkpointer is currently disabled.")
    # try:
    #     mongo_uri = settings.MONGO_URI
    #     db_name = settings.DB_NAME
    #     if not mongo_uri or not db_name:
    #          raise ValueError("MONGO_URI and MONGO_DB_NAME must be set for Beanie initialization.")
    #     client = AsyncIOMotorClient(mongo_uri)
    #     db = client[db_name]
    #     await init_beanie(database=db, document_models=[CheckPoints])
    #     logger.info(f"Beanie initialized successfully for DB: '{db_name}' with CheckPoints model.")
    # except Exception as e:
    #     logger.error(f"Failed to initialize Beanie for checkpointer: {e}")
    #     raise RuntimeError("Beanie initialization failed.") from e
    pass # No-op

# --- Example usage/test (COMMENTED OUT) ---
# async def _test_checkpointer():
#    try:
# ... (rest of _test_checkpointer function commented out) ...
#    print("Async test finished.")

# if __name__ == '__main__':
#     import asyncio
#     print("Running async test...")
#     asyncio.run(_test_checkpointer())
#     print("Async test finished.") 