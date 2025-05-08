import logging
import hmac
import hashlib
import os
import csv
import io
import json
from fastapi import FastAPI, HTTPException, Query, Request, Header, BackgroundTasks, Depends, status, Response, Cookie, File, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Literal, Dict, Any
import math
from datetime import datetime, timedelta
from ..config import Settings, get_settings
from dotenv import load_dotenv, find_dotenv
from jose import JWTError, jwt

# Import our existing models (adjust path if needed)
# Assuming PodcastLead includes the necessary display info
from ..models.lead import PodcastLead
# Assuming VettingResult holds the tier/explanation
from ..models.vetting import VettingResult
# Import the new webhook processor service
from ..services.webhook_processor import process_attio_update
# Import the AnalyticsService
from ..services.analytics_service import AnalyticsService
# --- NEW: Import workflow components ---
from ..models.campaign import CampaignConfiguration
from ..models.state import AgentState
from ..graph.state_graph import run_workflow
from ..persistence.state_manager import get_checkpoint_saver, initialize_beanie_for_checkpointer
# Import MongoDB utils for startup/shutdown
# from ..persistence.mongodb import connect_to_mongo, close_mongo_connection, initialize_collections
# --- End NEW Imports ---

# --- NEW: Import SearchAgent for standalone actions ---
from ..agents.search_agent import SearchAgent
# --- END NEW --- 

# --- NEW: Import EnrichmentAgent and EnrichedPodcastProfile ---
from ..agents.enrichment_agent import EnrichmentAgent
from ..models.podcast_profile import EnrichedPodcastProfile # For response model
# --- END NEW ---

# --- NEW: Import VettingAgent --- #
from ..agents.vetting_agent import VettingAgent 
# --- END NEW ---

# --- NEW: Pydantic Models for Standalone Vetting --- #
class StandaloneVettingRequest(BaseModel):
    enriched_profiles: List[EnrichedPodcastProfile] = Field(..., description="List of enriched podcast profiles to vet.")
    ideal_podcast_description: str = Field(..., description="Description of the ideal podcast for the guest/client.")
    guest_bio: str = Field(..., description="Biography or background of the guest/client.")
    guest_talking_points: List[str] = Field(..., description="Key talking points or angles for the guest/client.")
    source_campaign_id: Optional[str] = Field(None, description="Optional campaign ID to link this vetting run, can be from a search or enrichment run.")

class StandaloneVettingResponse(BaseModel):
    message: str
    count: int = Field(..., description="Number of profiles processed for vetting.")
    vetting_results: List[VettingResult] = Field(default_factory=list, description="List of vetting results for each profile.")
    csv_file_path: Optional[str] = Field(None, description="Web-accessible path to the CSV file containing vetting results.")
    error: Optional[str] = Field(None, description="Overall error message if the vetting process encountered a major issue.")
# --- END NEW VETTING MODELS --- 

# --- NEW: Model for Vetting Criteria from CSV form data --- #
class VettingCriteriaFormData(BaseModel):
    ideal_podcast_description: str
    guest_bio: str
    guest_talking_points_str: str # Will be a comma-separated string or newline-separated

    @property
    def guest_talking_points(self) -> List[str]:
        # Split by newline first, then filter out empty strings
        points = [p.strip() for p in self.guest_talking_points_str.split('\n') if p.strip()]
        if not points and self.guest_talking_points_str.strip(): # Fallback for comma separation if newline yields nothing but string is not empty
            points = [p.strip() for p in self.guest_talking_points_str.split(',') if p.strip()]
        return points

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Load .env and Instantiate Settings --- #
# load_dotenv(find_dotenv()) # This might be redundant if get_settings handles it or if run from a context where .env is already loaded
# app_settings = get_settings() # Using dependency injection for settings now

# --- FastAPI App Initialization --- #
app = FastAPI(
    title="Podcast Vetting Review API & Workflow Trigger",
    description="API for human review of vetted podcast leads and triggering the backend workflow.",
    version="0.1.0"
)

# --- CORS Middleware --- #
# Allow requests from typical frontend development ports
# TODO: Restrict origins in production
origins = [
    "http://localhost",
    "http://localhost:3000", # Common React dev port
    "http://localhost:8080", # Common Vue dev port
    "http://localhost:5173", # Common Vite dev port
    "https://medialeadlist.replit.app"
    # Add your Replit frontend URL if it's different and fixed
    # e.g., "https://your-replit-project-name.replit.dev"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True, # IMPORTANT: Must be True for cookies to be sent/received
    allow_methods=["*"], # Allow all methods
    allow_headers=["*"], # Allow all headers
)

# NEW: Mount static directory for CSV downloads
# This will make files in the "data" directory accessible via "/static" URL path
# e.g., data/campaigns/xyz/file.csv -> http://localhost:8000/static/campaigns/xyz/file.csv
# Ensure the 'data' directory exists at the root relative to where main.py is run (i.e., project root)
app.mount("/static", StaticFiles(directory="data"), name="static")

# --- Serve Static Frontend Files (for Replit deployment) ---
FRONTEND_BUILD_DIR = "review_ui/build/client" # Path confirmed from react-router build logs
STATIC_ASSETS_DIR = os.path.join(FRONTEND_BUILD_DIR, "assets")

# Serve static assets (CSS, JS, images) from the frontend build
if os.path.exists(STATIC_ASSETS_DIR):
    app.mount("/assets", StaticFiles(directory=STATIC_ASSETS_DIR), name="static_frontend_assets")
    logger.info(f"Mounted static frontend assets from: {STATIC_ASSETS_DIR}")
else:
    logger.warning(f"Frontend static assets directory not found at {STATIC_ASSETS_DIR}. Frontend assets will not be served via /assets.")
# --- End Serve Static Frontend Files ---

# --- API Models --- #

class LeadForReview(BaseModel):
    """Data structure for displaying a lead in the review UI."""
    lead_info: PodcastLead
    vetting_info: Optional[VettingResult] = None # Vetting result might not exist yet
    # --- NEW: Add review status --- #
    review_status: Literal["pending", "approved", "rejected"] = "pending" 
    # Add any other relevant fields for display

class PaginatedLeadsResponse(BaseModel):
    """Response model for paginated leads."""
    total_leads: int
    leads: List[LeadForReview]
    page: int
    page_size: int

class LeadReviewDecision(BaseModel):
    """Request model for submitting a review decision."""
    approved: bool
    feedback: Optional[str] = None

# --- NEW: Pydantic model for Attio Webhook Payload ---
class AttioWebhookPayload(BaseModel):
    event_type: str
    payload: Dict[str, Any]
    # Add more specific fields based on expected Attio structure if known

# --- NEW: Pydantic models for Bulk Review --- #
class BulkReviewDecision(BaseModel):
    podcast_ids: List[str] = Field(..., min_length=1)
    approved: bool
    # Optional: feedback applicable to all reviewed items in the batch
    feedback: Optional[str] = None 

class BulkReviewResponse(BaseModel):
    processed_count: int
    success_count: int
    failed_count: int
    # Optional: List of IDs that failed, with reasons
    failures: Optional[List[Dict[str, str]]] = None 

# --- NEW: Pydantic models for Standalone Search Actions --- #
class StandaloneTopicSearchRequest(BaseModel):
    target_audience: str = Field(..., description="The target audience profile for keyword generation.")
    key_messages: Optional[List[str]] = Field(None, description="Optional key messages to refine keyword generation. For the UI, this will be the 'topic to speak on'.")
    num_keywords_to_generate: int = Field(10, ge=1, le=30, description="Number of keywords to generate for the search.")
    max_results_per_keyword: int = Field(50, ge=1, le=200, description="Maximum results to attempt to fetch per API per keyword.")
    # campaign_id_prefix: Optional[str] = "standalone_topic" # Can be handled by agent internally

class StandaloneRelatedSearchRequest(BaseModel):
    seed_rss_url: str = Field(..., description="The RSS URL of the seed podcast for finding related ones.")
    max_depth: int = Field(2, ge=1, le=3, description="Depth of related search (e.g., 1 for direct, 2 for related of related).")
    max_total_results: int = Field(50, ge=1, le=200, description="Approximate maximum number of unique podcasts to return.")
    # campaign_id_prefix: Optional[str] = "standalone_related"

class StandaloneSearchResponse(BaseModel):
    message: str
    search_type: Literal["topic", "related"]
    count: int
    leads: List[Dict[str, Any]] # Or List[PodcastLead] if we convert
    csv_file_path: Optional[str] = None
    error: Optional[str] = None

class StandaloneEnrichmentRequest(BaseModel):
    leads: List[Dict[str, Any]] = Field(..., description="List of podcast lead dictionaries to enrich.")
    # campaign_id_prefix: Optional[str] = "standalone_enrich" # Handled by agent internally
    # NEW: Add campaign_id to link to a specific search run
    source_campaign_id: Optional[str] = Field(None, description="The campaign ID from the initial search, to link outputs.")

class StandaloneEnrichmentResponse(BaseModel):
    message: str
    count: int
    enriched_profiles: List[EnrichedPodcastProfile]
    csv_file_path: Optional[str] = None
    error: Optional[str] = None

# --- NEW: Pydantic model for User Preferences --- #
class UserPreferences(BaseModel):
    user_id: str # Or integrate with a proper auth system user ID
    default_sort_by: Literal['date_added', 'score', 'name'] = 'date_added'
    default_sort_order: Literal['asc', 'desc'] = 'desc'
    default_page_size: int = Field(10, ge=1, le=100)
    # Example saved filter structure
    saved_filters: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Saved filter presets") 
    # Add other preferences as needed

    class Config:
        validate_assignment = True

# --- Placeholder Data/Logic --- #
# Replace with actual database interaction later
# TODO: Connect to MongoDB persistence layer
DUMMY_LEADS = [
    LeadForReview(
        lead_info=PodcastLead(podcast_id="ln_1", name="Tech Unfiltered", description="Raw tech insights."),
        vetting_info=VettingResult(
            podcast_id="ln_1", 
            composite_score=88, 
            quality_tier="A", 
            # Add missing required fields with plausible defaults
            programmatic_consistency_passed=True,
            programmatic_consistency_reason="Recent and frequent (dummy data).",
            final_explanation="Overall Tier: A (Score: 88/100). | Programmatic Consistency: Passed - Recent and frequent (dummy data). | LLM Content Match: Score N/A - Not performed on dummy data.", 
            metric_scores={}
            # Removed invalid 'explanation' field
        )
    ),
    LeadForReview(
        lead_info=PodcastLead(podcast_id="ps_2", name="Startup Hustle", description="Founder interviews.", email="hustle@startup.com"),
        vetting_info=VettingResult(
            podcast_id="ps_2", 
            composite_score=65, 
            quality_tier="B", 
            programmatic_consistency_passed=True,
            programmatic_consistency_reason="Okay frequency (dummy data).",
            final_explanation="Overall Tier: B (Score: 65/100). | Programmatic Consistency: Passed - Okay frequency (dummy data). | LLM Content Match: Score N/A - Not performed on dummy data.", 
            metric_scores={}
        ),
        review_status="approved" # Example approved lead
    ),
    LeadForReview(
        lead_info=PodcastLead(podcast_id="ln_3", name="AI Today", description="Latest in AI."),
        vetting_info=VettingResult(
            podcast_id="ln_3", 
            composite_score=42, 
            quality_tier="C", 
            programmatic_consistency_passed=False,
            programmatic_consistency_reason="Infrequent (dummy data).",
            final_explanation="Overall Tier: C (Score: 42/100). | Programmatic Consistency: Failed - Infrequent (dummy data). | LLM Content Match: Score N/A - Not performed on dummy data.",
            metric_scores={}
        )
    ),
    LeadForReview(
        lead_info=PodcastLead(podcast_id="ps_4", name="Marketing Mavericks", description="Marketing tips."),
        vetting_info=VettingResult(
            podcast_id="ps_4", 
            composite_score=78, 
            quality_tier="B", 
            programmatic_consistency_passed=True,
            programmatic_consistency_reason="Recent, good count (dummy data).",
            final_explanation="Overall Tier: B (Score: 78/100). | Programmatic Consistency: Passed - Recent, good count (dummy data). | LLM Content Match: Score N/A - Not performed on dummy data.",
            metric_scores={}
        )
    )
]

# Placeholder storage for preferences (replace with DB)
DUMMY_USER_PREFS: Dict[str, UserPreferences] = {}

# --- NEW: Pydantic model for Login Request --- #
class LoginRequest(BaseModel):
    password: str

# --- NEW: Dependency function to provide settings --- #
def get_app_settings() -> Settings:
    return get_settings() # Correctly return the result of get_settings()

# --- JWT Utility Functions ---
def create_access_token(data: dict, settings: Settings, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=settings.JWT_ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)
    return encoded_jwt

async def get_current_user_from_token(
    settings: Settings = Depends(get_settings), 
    session_token: Optional[str] = Cookie(None) # Read the cookie
):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"}, # Though we use cookies, this is a common header
    )
    if session_token is None:
        logger.warning("No session token cookie found.")
        raise credentials_exception
    try:
        payload = jwt.decode(session_token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
        # For now, we don't have a specific "username" or user ID in the token,
        # so we'll just confirm the token is valid.
        # You could add a "sub" (subject) to your token data if needed.
        # username: str = payload.get("sub")
        # if username is None:
        #     raise credentials_exception
        # For this basic setup, if decode is successful, we consider the user authenticated.
        logger.info(f"Token decoded successfully for payload: {payload}")
        return {"user_payload": payload} # Return payload or a placeholder indicating auth
    except JWTError as e:
        logger.error(f"JWTError decoding token: {e}")
        raise credentials_exception
    except Exception as e:
        logger.error(f"Unexpected error decoding token: {e}")
        raise credentials_exception

# --- API Endpoints --- #

@app.get("/", tags=["Status"])
def read_root():
    """Root endpoint for basic API status check."""
    logger.info("Root endpoint accessed.")
    return {"message": "Podcast Vetting Review API is running."}

@app.get("/leads", response_model=PaginatedLeadsResponse, tags=["Leads"])
def get_leads_for_review(
    # Optional user_id to load preferences
    user_id: Optional[str] = Query(None, description="User ID to load saved preferences as defaults"),
    # --- Query Params with Defaults potentially overridden by prefs --- #
    page: int = Query(1, ge=1, description="Page number to retrieve"),
    page_size: Optional[int] = Query(None, ge=1, le=100, description="Number of leads per page (uses user default if available)"),
    filter_tier: Optional[Literal['A', 'B', 'C', 'D']] = Query(None, description="Filter by quality tier (A, B, C, D)"),
    filter_status: Optional[Literal['pending', 'approved', 'rejected']] = Query(None, description="Filter by review status"),
    min_score: Optional[int] = Query(None, ge=0, le=100, description="Minimum composite score"),
    max_score: Optional[int] = Query(None, ge=0, le=100, description="Maximum composite score"),
    search_term: Optional[str] = Query(None, description="Search term for podcast name or description"),
    sort_by: Optional[Literal['date_added', 'score', 'name']] = Query(None, description="Field to sort by ('date_added', 'score', 'name') (uses user default if available)"),
    sort_order: Optional[Literal['asc', 'desc']] = Query(None, description="Sort order ('asc', 'desc') (uses user default if available)"),
    current_user: dict = Depends(get_current_user_from_token) # Protected
):
    """Retrieves a paginated list of podcast leads, optionally using user preferences for defaults."""
    logger.info(f"User {current_user} accessing /leads endpoint.")
    # --- Load User Preferences (if user_id provided) --- #
    # Initialize preferences with a safe user_id (defaults) to avoid validation errors
    initial_user_id = user_id or "_anonymous_"
    prefs = UserPreferences(user_id=initial_user_id)  # Start with defaults
    if user_id and user_id in DUMMY_USER_PREFS: # TODO: Replace with DB lookup
        prefs = DUMMY_USER_PREFS[user_id]
        logger.info(f"Loaded preferences for user: {user_id}")
    elif user_id:
         logger.info(f"No preferences found for user {user_id}, using defaults.")
         
    # --- Determine effective parameters (Query Params override Prefs) --- #
    effective_page_size = page_size if page_size is not None else prefs.default_page_size
    effective_sort_by = sort_by if sort_by is not None else prefs.default_sort_by
    effective_sort_order = sort_order if sort_order is not None else prefs.default_sort_order
    # TODO: Implement logic for applying saved_filters from prefs if no specific filters are provided

    logger.info(f"Effective params for leads request: page={page}, page_size={effective_page_size}, filter_tier={filter_tier}, filter_status={filter_status}, min_score={min_score}, max_score={max_score}, search={search_term}, sort_by={effective_sort_by}, sort_order={effective_sort_order}")
    
    # --- Database Query / Data Fetching (using effective params) --- #
    # TODO: Replace DUMMY_LEADS with actual database query logic using effective parameters
    processed_leads = list(DUMMY_LEADS) # Make a copy to modify

    # --- Filtering Logic (using query params directly for now) --- #
    if filter_tier:
        logger.info(f"Applying filter: tier={filter_tier}")
        processed_leads = [lead for lead in processed_leads if lead.vetting_info and lead.vetting_info.quality_tier == filter_tier]
    
    if filter_status:
        logger.info(f"Applying filter: status={filter_status}")
        processed_leads = [lead for lead in processed_leads if lead.review_status == filter_status]
        
    if min_score is not None:
         logger.info(f"Applying filter: min_score={min_score}")
         processed_leads = [lead for lead in processed_leads if lead.vetting_info and lead.vetting_info.composite_score is not None and lead.vetting_info.composite_score >= min_score]
         
    if max_score is not None:
         logger.info(f"Applying filter: max_score={max_score}")
         processed_leads = [lead for lead in processed_leads if lead.vetting_info and lead.vetting_info.composite_score is not None and lead.vetting_info.composite_score <= max_score]
         
    if search_term:
        logger.info(f"Applying filter: search_term='{search_term}'")
        term_lower = search_term.lower()
        processed_leads = [lead for lead in processed_leads 
                           if (term_lower in lead.lead_info.name.lower() or 
                               (lead.lead_info.description and term_lower in lead.lead_info.description.lower()))]

    # --- Sorting Logic (using effective sort params) --- #
    reverse_sort = (effective_sort_order == 'desc')
    
    sort_key_func = None
    if effective_sort_by == 'score':
        logger.info(f"Sorting by score, order={effective_sort_order}")
        sort_key_func = lambda lead: lead.vetting_info.composite_score if lead.vetting_info and lead.vetting_info.composite_score is not None else (-math.inf if reverse_sort else math.inf)
    elif effective_sort_by == 'name':
        logger.info(f"Sorting by name, order={effective_sort_order}")
        sort_key_func = lambda lead: lead.lead_info.name.lower()
    elif effective_sort_by == 'date_added':
        logger.info(f"Sorting by date_added (default order), order={effective_sort_order}")
        if reverse_sort:
            processed_leads.reverse()
        sort_key_func = None
    
    if sort_key_func:
         processed_leads.sort(key=sort_key_func, reverse=reverse_sort)

    # --- Pagination (using effective page size) --- #
    total_leads = len(processed_leads)
    start_index = (page - 1) * effective_page_size
    end_index = start_index + effective_page_size
    paginated_leads = processed_leads[start_index:end_index]

    if not paginated_leads and page > 1:
        logger.warning(f"Page {page} requested but only {total_leads} leads available after filtering/sorting.")
        raise HTTPException(status_code=404, detail="Page not found for the given filters/sort order")

    logger.info(f"Returning {len(paginated_leads)} leads for page {page} after processing.")
    return PaginatedLeadsResponse(
        total_leads=total_leads, 
        leads=paginated_leads,
        page=page,
        page_size=effective_page_size # Return the effective page size used
    )

@app.post("/leads/{podcast_id}/review", status_code=204, tags=["Leads"])
def submit_lead_review(
    podcast_id: str,
    decision: LeadReviewDecision,
    current_user: dict = Depends(get_current_user_from_token) # Protected
):
    """Submits a human review decision (approve/reject) for a specific lead."""
    logger.info(f"User {current_user} - Review decision received for podcast_id='{podcast_id}': Approved={decision.approved}, Feedback='{decision.feedback or ''}'")
    # TODO: Implement logic to find the lead by podcast_id
    # TODO: Update the lead's status in the database (e.g., 'approved', 'rejected')
    # TODO: Store the feedback if provided

    # Placeholder logic
    lead_exists = any(lead.lead_info.podcast_id == podcast_id for lead in DUMMY_LEADS)
    if not lead_exists:
        logger.warning(f"Attempted to review non-existent lead: {podcast_id}")
        raise HTTPException(status_code=404, detail=f"Lead with podcast_id '{podcast_id}' not found.")

    # Simulate storing the decision
    logger.info(f"Successfully processed review for {podcast_id}.")
    # No content response for status_code=204
    return

# --- NEW: Bulk Review Endpoint --- #
@app.post("/leads/bulk-review", response_model=BulkReviewResponse, tags=["Leads"])
async def submit_bulk_lead_review(
    decision: BulkReviewDecision,
    current_user: dict = Depends(get_current_user_from_token) # Protected
):
    """Submits a review decision (approve/reject) for multiple leads at once."""
    logger.info(f"User {current_user} - Bulk review decision received for {len(decision.podcast_ids)} leads: Approved={decision.approved}")
    
    processed = 0
    success = 0
    failed_items = []

    # TODO: Implement actual database update logic
    # This requires leads to have a field like 'review_status' 
    # Connect to DB
    # leads_collection = _get_collection(LEADS_COLLECTION)
    
    new_status = "approved" if decision.approved else "rejected"
    
    for podcast_id in decision.podcast_ids:
        processed += 1
        try:
            # Placeholder: Simulate finding and updating the lead
            # find_result = leads_collection.find_one({"podcast_id": podcast_id})
            # if not find_result:
            #    raise ValueError("Lead not found")
            # update_result = leads_collection.update_one(
            #     {"podcast_id": podcast_id},
            #     {"$set": {"review_status": new_status, "review_feedback": decision.feedback, "reviewed_at": datetime.utcnow()}}
            # )
            # if update_result.matched_count == 0:
            #     raise ValueError("Lead found but failed to update")
            # elif update_result.modified_count == 0 and update_result.matched_count == 1:
            #     logger.warning(f"Lead {podcast_id} status was already {new_status}.")
            #     # Count as success even if status didn't change
            
            # --- Simulate success for now ---
            logger.debug(f"Simulating update for {podcast_id} to status: {new_status}")
            lead_exists = any(lead.lead_info.podcast_id == podcast_id for lead in DUMMY_LEADS)
            if not lead_exists:
                 raise ValueError("Lead not found in dummy data")
            # --- End Simulate --- 
                 
            success += 1
            
        except Exception as e:
            logger.error(f"Failed to process bulk review for podcast_id '{podcast_id}': {e}")
            failed_items.append({"podcast_id": podcast_id, "error": str(e)})

    response = BulkReviewResponse(
        processed_count=processed,
        success_count=success,
        failed_count=len(failed_items),
        failures=failed_items if failed_items else None
    )
    
    logger.info(f"Bulk review processing complete. Success: {success}/{processed}")
    return response

# --- NEW: User Preference Endpoints --- #

@app.get("/users/{user_id}/preferences", response_model=UserPreferences, tags=["Users", "Preferences"])
async def get_user_preferences(
    user_id: str,
    current_user: dict = Depends(get_current_user_from_token) # Protected
):
    """Retrieves saved preferences for a given user."""
    # Add check: ensure current_user can access preferences for user_id
    # For now, assuming any authenticated user can fetch any user's prefs for simplicity,
    # but in a real app, you'd restrict this.
    logger.info(f"User {current_user} - Request received for preferences of user: {user_id}")
    # TODO: Replace with DB lookup
    if user_id in DUMMY_USER_PREFS:
        return DUMMY_USER_PREFS[user_id]
    else:
        # Return default preferences if none saved?
        logger.info(f"No preferences found for user {user_id}, returning defaults.")
        # Create default prefs object - user_id must be passed
        default_prefs = UserPreferences(user_id=user_id) 
        return default_prefs
        # Or raise 404?
        # raise HTTPException(status_code=404, detail=f"Preferences not found for user '{user_id}'")

@app.put("/users/{user_id}/preferences", response_model=UserPreferences, tags=["Users", "Preferences"])
async def update_user_preferences(
    user_id: str, 
    preferences: UserPreferences,
    current_user: dict = Depends(get_current_user_from_token) # Protected
):
    """Saves/updates preferences for a given user."""
    # Add check: ensure current_user can update preferences for user_id
    logger.info(f"User {current_user} - Request received to update preferences for user: {user_id}")
    # Basic validation
    if user_id != preferences.user_id:
        raise HTTPException(status_code=400, detail="User ID in path does not match user ID in preferences payload.")
        
    # TODO: Replace with DB update/insert
    DUMMY_USER_PREFS[user_id] = preferences
    logger.info(f"Successfully updated preferences for user {user_id}")
    return preferences

# --- NEW: Attio Webhook Endpoint --- #
@app.post("/webhooks/attio", status_code=200, tags=["Webhooks"])
async def handle_attio_webhook(
    payload: AttioWebhookPayload, 
    request: Request, # Inject Request object to access body and headers
    settings: Settings = Depends(get_settings), # Inject settings
    # Placeholder: Replace 'X-Attio-Signature-256' with the actual header name
    x_attio_signature: Optional[str] = Header(None, alias="X-Attio-Signature-256") 
    # This endpoint is typically not protected by user session tokens, but by a webhook secret
):
    """Receives and processes webhook events from Attio."""
    logger.info(f"Received Attio webhook. Event Type: {payload.event_type}")
    
    # --- Webhook Signature Verification --- # 
    # TODO: Replace placeholders with actual values/logic
    webhook_secret = settings.ATTIO_WEBHOOK_SECRET # Get from injected settings
    
    if webhook_secret and x_attio_signature:
        logger.debug("Attempting webhook signature verification.")
        try:
            raw_body = await request.body()
            # Placeholder: Assumes HMAC-SHA256. Adjust if different.
            computed_hash = hmac.new(
                webhook_secret.encode('utf-8'), 
                raw_body,
                hashlib.sha256
            ).hexdigest()
            
            # Prefix might be needed depending on Attio's format (e.g., 'sha256=')
            expected_signature = f"sha256={computed_hash}" # Adjust prefix if needed
            
            logger.debug(f"Received Signature: {x_attio_signature}")
            logger.debug(f"Computed Signature: {expected_signature}")

            if not hmac.compare_digest(expected_signature, x_attio_signature):
                logger.error("Webhook signature mismatch!")
                raise HTTPException(status_code=403, detail="Invalid webhook signature")
            
            logger.info("Webhook signature verified successfully.")
            
        except Exception as e:
            logger.exception(f"Error during webhook signature verification: {e}")
            raise HTTPException(status_code=403, detail="Webhook signature verification failed")
            
    elif webhook_secret and not x_attio_signature:
        # Secret is configured, but no signature received - treat as error
        logger.error("Webhook secret configured, but no signature header received.")
        raise HTTPException(status_code=403, detail="Missing webhook signature")
    else:
        # No secret configured, skip verification (log a warning)
        logger.warning("ATTIO_WEBHOOK_SECRET not set. Skipping signature verification. THIS IS INSECURE.")
        
    # --- Process Payload --- #
    # TODO: Implement logic to parse payload and update internal state
    
    event_type = payload.event_type
    event_data = payload.payload
    
    logger.debug(f"Webhook payload data: {event_data}")
    
    if event_type == "record.updated" or event_type == "record.tag_added" or event_type == "record.tag_removed":
        # Example: Process record updates (likely status changes via tags)
        record_id = event_data.get("record_id")
        object_type = event_data.get("object")
        
        if object_type == "company" and record_id:
            logger.info(f"Processing update for Attio company ID: {record_id}")
            # --- Placeholder for update logic --- #
            # 1. Fetch the full record details from Attio using record_id? (Maybe not needed if payload is rich)
            # 2. Convert Attio data to internal format (attio_company_to_podcast)
            # 3. Identify the change (e.g., new status tag)
            # 4. Update the corresponding lead in the database/state
            # Example (needs actual implementation):
            # from ..services.webhook_processor import process_attio_update
            await process_attio_update(record_id, event_data)
            # pass # Replace with actual processing call
        else:
            logger.warning("Webhook payload missing record_id or object_type is not 'company'.")
            
    else:
        logger.info(f"Received unhandled Attio event type: {event_type}")

    # Acknowledge receipt to Attio
    return {"status": "received"}

# --- NEW: Analytics Endpoints --- #

@app.get("/analytics/step-durations", tags=["Analytics"])
async def get_analytics_step_durations(
    campaign_id: Optional[str] = Query(None, description="Optional Campaign ID to filter results"),
    current_user: dict = Depends(get_current_user_from_token) # Protected
):
    """Retrieves average and median durations for each workflow step."""
    logger.info(f"User {current_user} - Request received for analytics: step durations. Campaign ID: {campaign_id}")
    try:
        analytics_service = AnalyticsService()
        durations = analytics_service.get_step_durations(campaign_id=campaign_id)
        if not durations:
             # Return 404 if no metrics found for the filter, or just empty dict?
             # Let's return empty dict for now.
             logger.info(f"No step duration metrics found for campaign ID: {campaign_id}")
             # raise HTTPException(status_code=404, detail="No step duration metrics found for the specified campaign.")
        return durations
    except Exception as e:
        logger.exception(f"Error fetching step duration analytics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error fetching analytics.")

@app.get("/analytics/vetting-distribution", tags=["Analytics"])
async def get_analytics_vetting_distribution(
    campaign_id: Optional[str] = Query(None, description="Optional Campaign ID to filter results"),
    current_user: dict = Depends(get_current_user_from_token) # Protected
):
    """Retrieves the distribution of vetting quality tiers."""
    logger.info(f"User {current_user} - Request received for analytics: vetting distribution. Campaign ID: {campaign_id}")
    try:
        analytics_service = AnalyticsService()
        distribution = analytics_service.get_vetting_tier_distribution(campaign_id=campaign_id)
        return distribution
    except Exception as e:
        logger.exception(f"Error fetching vetting distribution analytics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error fetching analytics.")

@app.get("/analytics/search-performance", tags=["Analytics"])
async def get_analytics_search_performance(
    campaign_id: Optional[str] = Query(None, description="Optional Campaign ID to filter results"),
    current_user: dict = Depends(get_current_user_from_token) # Protected
):
    """Retrieves search performance metrics grouped by source."""
    logger.info(f"User {current_user} - Request received for analytics: search performance. Campaign ID: {campaign_id}")
    try:
        analytics_service = AnalyticsService()
        performance = analytics_service.get_search_source_performance(campaign_id=campaign_id)
        return performance
    except Exception as e:
        logger.exception(f"Error fetching search performance analytics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error fetching analytics.")

@app.get("/analytics/crm-sync-summary", tags=["Analytics"])
async def get_analytics_crm_sync_summary(
    campaign_id: Optional[str] = Query(None, description="Optional Campaign ID to filter results"),
    current_user: dict = Depends(get_current_user_from_token) # Protected
):
    """Retrieves a summary of CRM synchronization attempts, successes, and errors."""
    logger.info(f"User {current_user} - Request received for analytics: CRM sync summary. Campaign ID: {campaign_id}")
    try:
        analytics_service = AnalyticsService()
        summary = analytics_service.get_crm_sync_summary(campaign_id=campaign_id)
        return summary
    except Exception as e:
        logger.exception(f"Error fetching CRM sync summary analytics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error fetching analytics.")

@app.get("/analytics/suggestions", tags=["Analytics"])
async def get_analytics_suggestions(
    campaign_id: Optional[str] = Query(None, description="Optional Campaign ID to filter results"),
    current_user: dict = Depends(get_current_user_from_token) # Protected
):
    """Generates performance suggestions based on collected metrics."""
    logger.info(f"User {current_user} - Request received for analytics: suggestions. Campaign ID: {campaign_id}")
    try:
        analytics_service = AnalyticsService()
        suggestions = analytics_service.generate_performance_suggestions(campaign_id=campaign_id)
        return {"suggestions": suggestions}
    except Exception as e:
        logger.exception(f"Error generating performance suggestions: {e}")
        raise HTTPException(status_code=500, detail="Internal server error generating suggestions.")

@app.get("/analytics/compare-campaigns", tags=["Analytics"])
async def compare_campaign_analytics(
    campaign_ids: List[str] = Query(..., description="List of Campaign IDs to compare"),
    current_user: dict = Depends(get_current_user_from_token) # Protected
):
    """Retrieves and compares analytics across multiple specified campaigns."""
    # Basic validation
    if not campaign_ids or len(campaign_ids) < 2:
         raise HTTPException(status_code=400, detail="Please provide at least two campaign IDs to compare.")
         
    logger.info(f"User {current_user} - Request received for analytics: compare campaigns. IDs: {campaign_ids}")
    try:
        analytics_service = AnalyticsService()
        comparison_data = analytics_service.compare_campaigns(campaign_ids=campaign_ids)
        if not comparison_data:
             # This might happen if none of the provided IDs had metrics
             logger.warning(f"No analytics data found for any of the provided campaign IDs: {campaign_ids}")
             # Return 404 or empty object?
             raise HTTPException(status_code=404, detail="No analytics data found for the specified campaign IDs.")
        return comparison_data
    except HTTPException as http_exc:
         # Re-raise HTTPExceptions directly
         raise http_exc
    except Exception as e:
        logger.exception(f"Error comparing campaign analytics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error comparing campaign analytics.")

# --- NEW: Pydantic model for Workflow Response --- #
class WorkflowResponse(BaseModel):
    message: str
    final_state: Optional[AgentState] = None # Include the final state
    error: Optional[str] = None

# --- NEW: Workflow Trigger Endpoint (MODIFIED FOR SYNCHRONOUS RUN) --- #
@app.post("/campaigns/run", response_model=WorkflowResponse, tags=["Workflow"])
async def run_campaign_workflow_sync(
    campaign_config: CampaignConfiguration,
    current_user: dict = Depends(get_current_user_from_token) # Protected
    # background_tasks: BackgroundTasks # Removed BackgroundTasks
) -> WorkflowResponse:
    """
    Runs the backend lead generation workflow (Discovery, Vetting, Enrichment)
    synchronously for the given campaign configuration and returns the final state.
    WARNING: This can take a significant amount of time.
    """
    logger.info(f"User {current_user} - Received synchronous request to run workflow for campaign: {campaign_config.campaign_id} ({campaign_config.target_audience})")
    final_agent_state = None
    try:
        # 1. Create Initial State
        initial_agent_state = AgentState(
            current_step="search", # Start at the beginning
            campaign_config=campaign_config,
            execution_status="pending"
        )

        # 2. Get Checkpointer (will be None as it's disabled)
        checkpointer = get_checkpoint_saver()
        if not checkpointer:
            logger.warning("Checkpointer is None. Workflow will run WITHOUT persistence.")

        # 3. Run workflow directly and wait for completion
        # Ensure run_workflow is awaitable if it uses async components
        logger.info(f"Starting synchronous workflow run for {campaign_config.campaign_id}...")
        final_agent_state = run_workflow(initial_agent_state, checkpointer) # Assume run_workflow is compatible
        logger.info(f"Synchronous workflow for campaign {campaign_config.campaign_id} completed.")

        if final_agent_state:
            return WorkflowResponse(
                message="Workflow completed successfully.",
                final_state=final_agent_state
            )
        else:
             logger.error(f"Workflow for {campaign_config.campaign_id} finished but returned no final state.")
             return WorkflowResponse(
                 message="Workflow finished, but no final state could be determined.",
                 final_state=None,
                 error="Could not retrieve final state."
             )

    except Exception as e:
        logger.exception(f"Error running workflow synchronously for campaign {campaign_config.campaign_id}")
        # Return error details in the response
        return WorkflowResponse(
            message="Workflow failed to execute.",
            final_state=None,
            error=str(e)
        )

# --- NEW: Standalone Agent Action Endpoints ---

# Instantiate agents (consider if they should be global or per-request)
# For simplicity, global for now, but for production, consider FastAPI dependencies for stateful services.
search_agent_instance = SearchAgent()
enrichment_agent_instance = EnrichmentAgent()
vetting_agent_instance = VettingAgent() # NEW: Instantiate VettingAgent

@app.post("/actions/search/topic", response_model=StandaloneSearchResponse, tags=["Actions", "Search"])
async def trigger_standalone_topic_search(
    request_data: StandaloneTopicSearchRequest,
    current_user: dict = Depends(get_current_user_from_token) # Protected
):
    """Triggers a standalone topic-based podcast search."""
    logger.info(f"User {current_user} - Received standalone topic search request: {request_data.target_audience}")
    try:
        leads, csv_path = search_agent_instance.perform_standalone_topic_search(
            target_audience=request_data.target_audience,
            key_messages=request_data.key_messages,
            num_keywords_to_generate=request_data.num_keywords_to_generate,
            max_results_per_keyword=request_data.max_results_per_keyword
        )
        return StandaloneSearchResponse(
            message=f"Standalone topic search completed. Found {len(leads)} leads.",
            search_type="topic",
            count=len(leads),
            leads=leads,
            csv_file_path=csv_path
        )
    except Exception as e:
        logger.exception(f"Error during standalone topic search: {e}")
        return StandaloneSearchResponse(
            message="Standalone topic search failed.",
            search_type="topic",
            count=0,
            leads=[],
            error=str(e)
        )

@app.post("/actions/search/related", response_model=StandaloneSearchResponse, tags=["Actions", "Search"])
async def trigger_standalone_related_search(
    request_data: StandaloneRelatedSearchRequest,
    current_user: dict = Depends(get_current_user_from_token) # Protected
):
    """Triggers a standalone related podcast search based on a seed RSS URL."""
    logger.info(f"User {current_user} - Received standalone related search request for RSS: {request_data.seed_rss_url}")
    try:
        leads, csv_path = search_agent_instance.perform_standalone_related_search(
            seed_rss_url=request_data.seed_rss_url,
            max_depth=request_data.max_depth,
            max_total_results=request_data.max_total_results
        )
        return StandaloneSearchResponse(
            message=f"Standalone related search completed. Found {len(leads)} leads.",
            search_type="related",
            count=len(leads),
            leads=leads,
            csv_file_path=csv_path
        )
    except Exception as e:
        logger.exception(f"Error during standalone related search: {e}")
        return StandaloneSearchResponse(
            message="Standalone related search failed.",
            search_type="related",
            count=0,
            leads=[],
            error=str(e)
        )

# --- NEW: Standalone Enrichment Endpoint ---
@app.post("/actions/enrich", response_model=StandaloneEnrichmentResponse, tags=["Actions", "Enrichment"])
async def trigger_standalone_enrichment(
    request_data: StandaloneEnrichmentRequest,
    current_user: dict = Depends(get_current_user_from_token) # Protected
):
    """Triggers standalone enrichment for a provided list of podcast leads."""
    logger.info(f"User {current_user} - Received standalone enrichment request for {len(request_data.leads)} leads. Source Campaign ID: {request_data.source_campaign_id}")
    if not request_data.leads:
        return StandaloneEnrichmentResponse(
            message="No leads provided for enrichment.",
            count=0,
            enriched_profiles=[],
            error="Input list of leads was empty."
        )
    try:
        enriched_profiles, csv_path = await enrichment_agent_instance.perform_standalone_enrichment(
            leads_to_enrich=request_data.leads,
            # Pass the source_campaign_id to the agent method
            existing_campaign_id=request_data.source_campaign_id 
        )
        return StandaloneEnrichmentResponse(
            message=f"Standalone enrichment completed. Enriched {len(enriched_profiles)} profiles.",
            count=len(enriched_profiles),
            enriched_profiles=enriched_profiles,
            csv_file_path=csv_path
        )
    except Exception as e:
        logger.exception(f"Error during standalone enrichment: {e}")
        return StandaloneEnrichmentResponse(
            message="Standalone enrichment failed.",
            count=0,
            enriched_profiles=[],
            error=str(e)
        )
# --- END NEW STANDALONE ENRICHMENT --- 

# --- NEW: Standalone Vetting Endpoint --- #
@app.post("/actions/vet", response_model=StandaloneVettingResponse, tags=["Actions", "Vetting"])
async def trigger_standalone_vetting(
    request_data: StandaloneVettingRequest,
    current_user: dict = Depends(get_current_user_from_token) # Protected
):
    """Triggers standalone vetting for a list of enriched podcast profiles based on specified criteria."""
    logger.info(f"User {current_user} - Received standalone vetting request for {len(request_data.enriched_profiles)} profiles. Source Campaign ID: {request_data.source_campaign_id}")
    if not request_data.enriched_profiles:
        return StandaloneVettingResponse(
            message="No enriched profiles provided for vetting.",
            count=0,
            vetting_results=[],
            error="Input list of enriched_profiles was empty."
        )
    try:
        vetting_results, csv_path = await vetting_agent_instance.perform_standalone_vetting(
            enriched_profiles=request_data.enriched_profiles,
            ideal_podcast_description=request_data.ideal_podcast_description,
            guest_bio=request_data.guest_bio,
            guest_talking_points=request_data.guest_talking_points,
            source_campaign_id=request_data.source_campaign_id
        )
        return StandaloneVettingResponse(
            message=f"Standalone vetting completed. Processed {len(request_data.enriched_profiles)} profiles.",
            count=len(request_data.enriched_profiles),
            vetting_results=vetting_results,
            csv_file_path=csv_path
        )
    except Exception as e:
        logger.exception(f"Error during standalone vetting: {e}")
        return StandaloneVettingResponse(
            message="Standalone vetting failed.",
            count=len(request_data.enriched_profiles), # Count of profiles attempted
            vetting_results=[], # Or potentially return partial results if any were processed before error
            error=str(e)
        )
# --- END NEW STANDALONE VETTING --- 

# --- NEW: Helper function to parse CSV --- #
async def parse_csv_to_dicts(file: UploadFile) -> List[Dict[str, Any]]:
    contents = await file.read()
    # Use io.StringIO to treat the byte string as a file for csv.reader
    # Decode bytes to string assuming UTF-8 encoding
    try:
        text_content = contents.decode('utf-8')
    except UnicodeDecodeError:
        # Fallback or raise specific error for encoding issues
        logger.warning("CSV file is not UTF-8 encoded, trying latin-1")
        try:
            text_content = contents.decode('latin-1') # Common alternative
        except UnicodeDecodeError as e:
            logger.error(f"Failed to decode CSV with UTF-8 and latin-1: {e}")
            raise HTTPException(status_code=400, detail=f"Invalid CSV encoding. Please use UTF-8 or Latin-1. Error: {e}")

    # Check for Byte Order Mark (BOM) for UTF-8, which can cause issues with DictReader
    if text_content.startswith('\ufeff'):
        logger.info("UTF-8 BOM detected and removed from CSV content.")
        text_content = text_content[1:]

    file_like_object = io.StringIO(text_content)
    
    # Sniff dialect to handle various CSV formats (e.g. comma or semicolon separated)
    try:
        # Read more data for sniffing, ensure it's enough for varied CSVs
        sample = file_like_object.read(min(2048, len(text_content))) # Read up to 2KB or whole content if smaller
        dialect = csv.Sniffer().sniff(sample)
        file_like_object.seek(0) # Reset read pointer
        reader = csv.DictReader(file_like_object, dialect=dialect)
        logger.info(f"CSV dialect sniffed: delimiter='{dialect.delimiter}', quotechar='{dialect.quotechar}'")
    except csv.Error as e:
        logger.warning(f"Could not sniff CSV dialect: {e}. Falling back to default comma delimiter.")
        file_like_object.seek(0) # Reset read pointer
        reader = csv.DictReader(file_like_object) # Default assumes comma delimiter
        
    data = [row for row in reader]
    await file.close() # Ensure the uploaded file is closed
    return data

# --- NEW: Helper function to check required headers ---
def check_required_headers(csv_headers: List[str], required_headers: List[str]):
    missing_headers = [header for header in required_headers if header not in csv_headers]
    if missing_headers:
        raise HTTPException(
            status_code=400, 
            detail=f"Missing required CSV columns: {', '.join(missing_headers)}"
        )

# --- NEW: Standalone Enrichment from CSV Endpoint ---
@app.post("/actions/enrich/csv", response_model=StandaloneEnrichmentResponse, tags=["Actions", "Enrichment", "CSV Upload"])
async def trigger_standalone_enrichment_from_csv(
    file: UploadFile = File(..., description="CSV file containing podcast leads to enrich."),
    source_campaign_id: Optional[str] = Form(None, description="Optional campaign ID to link this enrichment run."),
    current_user: dict = Depends(get_current_user_from_token) # Protected
):
    logger.info(f"User {current_user} - Received standalone enrichment request from CSV. Source Campaign ID: {source_campaign_id}")
    if not file.filename or not file.filename.endswith('.csv'): # Added check for file.filename existence
        raise HTTPException(status_code=400, detail="Invalid file type or no filename. Please upload a CSV file.")

    try:
        leads_to_enrich = await parse_csv_to_dicts(file)
        logger.info(f"Successfully parsed {len(leads_to_enrich)} leads from uploaded CSV.")
    except HTTPException as e: # Re-raise HTTPExceptions from parser
        raise e
    except Exception as e:
        logger.exception(f"Error parsing CSV for enrichment: {e}")
        return StandaloneEnrichmentResponse(
            message="Failed to parse uploaded CSV file.",
            count=0,
            enriched_profiles=[],
            error=f"CSV Parsing Error: {str(e)}"
        )

    if not leads_to_enrich:
        return StandaloneEnrichmentResponse(
            message="No leads found in the uploaded CSV or CSV was empty.",
            count=0,
            enriched_profiles=[],
            error="CSV contained no data."
        )
    
    try:
        enriched_profiles, csv_path = await enrichment_agent_instance.perform_standalone_enrichment(
            leads_to_enrich=leads_to_enrich,
            existing_campaign_id=source_campaign_id
        )
        return StandaloneEnrichmentResponse(
            message=f"Standalone enrichment from CSV completed. Enriched {len(enriched_profiles)} profiles.",
            count=len(enriched_profiles),
            enriched_profiles=enriched_profiles,
            csv_file_path=csv_path
        )
    except Exception as e:
        logger.exception(f"Error during standalone enrichment from CSV: {e}")
        return StandaloneEnrichmentResponse(
            message="Standalone enrichment from CSV failed.",
            count=0, # Or count of leads_to_enrich if you want to show attempted count
            enriched_profiles=[],
            error=str(e)
        )

# --- NEW: Standalone Vetting from CSV Endpoint --- #
@app.post("/actions/vet/csv", response_model=StandaloneVettingResponse, tags=["Actions", "Vetting", "CSV Upload"])
async def trigger_standalone_vetting_from_csv(
    file: UploadFile = File(..., description="CSV file containing enriched podcast profiles to vet."),
    ideal_podcast_description: str = Form(..., description="Description of the ideal podcast for the guest/client."),
    guest_bio: str = Form(..., description="Biography or background of the guest/client."),
    guest_talking_points_str: str = Form(..., description="Key talking points (newline or comma separated)."), # Changed name to avoid conflict if model used directly
    source_campaign_id: Optional[str] = Form(None, description="Optional campaign ID to link this vetting run."),
    current_user: dict = Depends(get_current_user_from_token) # Protected
):
    logger.info(f"User {current_user} - Received standalone vetting request from CSV. Source Campaign ID: {source_campaign_id}")
    if not file.filename or not file.filename.endswith('.csv'): # Added check for file.filename existence
        raise HTTPException(status_code=400, detail="Invalid file type or no filename. Please upload a CSV file.")

    vetting_criteria = VettingCriteriaFormData(
        ideal_podcast_description=ideal_podcast_description,
        guest_bio=guest_bio,
        guest_talking_points_str=guest_talking_points_str
    )

    try:
        parsed_csv_data = await parse_csv_to_dicts(file)
        logger.info(f"Successfully parsed {len(parsed_csv_data)} profiles from uploaded CSV for vetting.")
        
        # --- NEW: Header Validation --- #
        if parsed_csv_data:
            # Define the essential headers needed to even attempt creating an EnrichedPodcastProfile
            # Adjust this list based on the *truly required* fields in your model or logic
            required_cols = ['title', 'api_id'] # Example: Add more critical fields as needed
            csv_headers = list(parsed_csv_data[0].keys()) # Get headers from the first row
            check_required_headers(csv_headers, required_cols)
        # --- END Header Validation --- #

    except HTTPException as e: # Re-raise HTTPExceptions from parser or header check
        raise e
    except Exception as e:
        logger.exception(f"Error parsing CSV or checking headers for vetting: {e}")
        return StandaloneVettingResponse(
            message="Failed to parse uploaded CSV file or validate headers.",
            count=0,
            vetting_results=[],
            error=f"CSV Parsing/Validation Error: {str(e)}"
        )

    if not parsed_csv_data:
        return StandaloneVettingResponse(
            message="No profiles found in the uploaded CSV or CSV was empty.",
            count=0,
            vetting_results=[],
            error="CSV for vetting contained no data."
        )

    enriched_profiles_to_vet: List[EnrichedPodcastProfile] = []
    parsing_errors: List[str] = []
    error_threshold = 50 # Example: Stop if more than 50 rows fail
    
    # --- Define boolean fields and their expected True values ---
    boolean_fields = {'rss_explicit': ['true', '1', 'yes']} # Add other boolean fields if any

    for i, row_dict in enumerate(parsed_csv_data):
        # --- NEW: Error Threshold Check ---
        if len(parsing_errors) >= error_threshold:
            logger.error(f"Exceeded error threshold ({error_threshold}) while parsing CSV for vetting. Aborting.")
            parsing_errors.append(f"Processing aborted after {error_threshold} parsing errors.")
            break # Stop processing more rows
            
        try:
            processed_row = row_dict.copy() # Work on a copy
            
            # Convert empty strings to None and handle specific non-numeric strings
            for key, value in processed_row.items():
                if isinstance(value, str):
                    cleaned_value = value.strip()
                    if cleaned_value == "":
                        processed_row[key] = None
                    elif cleaned_value.lower() in ["n/a", "na", "unknown", "-", "none"]:
                         processed_row[key] = None
                         
            # Basic type coercion for common numeric fields (with enhanced cleanup)
            numeric_fields = ['total_episodes', 'average_duration_seconds', 'publishing_frequency_days',
                           'listen_score', 'listen_score_global_rank', 'audience_size',
                           'itunes_rating_average', 'itunes_rating_count',
                           'spotify_rating_average', 'spotify_rating_count',
                           'twitter_followers', 'linkedin_connections']
            for key in numeric_fields:
                value = processed_row.get(key)
                if isinstance(value, str):
                    cleaned_value = value.strip().replace('$', '').replace(',', '') # Remove $ and commas
                    if cleaned_value:
                        try:
                            if '.' in cleaned_value: 
                                processed_row[key] = float(cleaned_value)
                            else: 
                                processed_row[key] = int(cleaned_value)
                        except ValueError:
                            logger.warning(f"Could not convert cleaned value '{cleaned_value}' for field '{key}' to number in CSV row {i+1}. Setting to None.")
                            processed_row[key] = None # Set to None if conversion fails after cleanup
                    else:
                         processed_row[key] = None # If value was just whitespace, $, or ,

            # --- NEW: Boolean Field Conversion ---
            for field, true_values in boolean_fields.items():
                value = processed_row.get(field)
                if isinstance(value, str):
                    cleaned_value = value.strip().lower()
                    if cleaned_value in true_values:
                        processed_row[field] = True
                    elif cleaned_value in ['false', '0', 'no', '']: # Handle false and empty
                         processed_row[field] = False
                    else:
                         processed_row[field] = None # Set to None if not clearly True/False and field allows None
                         logger.warning(f"Unrecognized boolean value '{value}' for field '{field}' in CSV row {i+1}. Setting to None.")
                elif value is None and EnrichedPodcastProfile.model_fields[field].is_required():
                     # Handle case where a required boolean field is missing/None
                     logger.warning(f"Required boolean field '{field}' is missing or None in CSV row {i+1}. Defaulting to False (or raise error).")
                     processed_row[field] = False # Or raise error/skip row

            # Attempt to parse stringified lists (refined)
            list_fields = ['host_names', 'data_sources']
            for field in list_fields:
                value = processed_row.get(field)
                if isinstance(value, str):
                    cleaned_value = value.strip()
                    if cleaned_value.startswith('[') and cleaned_value.endswith(']'):
                        try:
                            parsed_list = json.loads(cleaned_value)
                            if isinstance(parsed_list, list): # Ensure it parsed to a list
                                processed_row[field] = parsed_list
                            else:
                                logger.warning(f"Parsed stringified list for field '{field}' in CSV row {i+1}, but result was not a list: {type(parsed_list)}. Setting to None.")
                                processed_row[field] = None
                        except json.JSONDecodeError:
                            logger.warning(f"Could not JSON decode stringified list for field '{field}' in CSV row {i+1}: '{cleaned_value[:50]}...'. Setting to None.")
                            processed_row[field] = None
                    elif cleaned_value == "": # Handle empty string for list field
                         processed_row[field] = None # Or [] if appropriate default
                    # else: keep original string if it doesn't look like a list

            # Instantiate the Pydantic model using the processed row
            profile = EnrichedPodcastProfile(**processed_row)
            enriched_profiles_to_vet.append(profile)
        except HTTPException: # Should not happen here, but good practice
             raise
        except Exception as validation_error: # Catch Pydantic ValidationError or others
            # Log original data for better debugging
            err_msg = f"Row {i+1}: Could not parse into EnrichedPodcastProfile. Error: {validation_error}. Original Data: {row_dict}"
            logger.warning(err_msg)
            parsing_errors.append(err_msg)
            # Decide: skip this row or fail the whole request? Skip for now.

    if not enriched_profiles_to_vet and parsed_csv_data: # All rows failed parsing
         final_error_msg = "All rows in CSV failed validation. Check column names, data types, and required fields. "
         if parsing_errors:
              final_error_msg += "First few errors: " + "; ".join(parsing_errors[:3])
         return StandaloneVettingResponse(
            message="CSV parsing completed, but no valid enriched profiles could be created.",
            count=len(parsed_csv_data),
            vetting_results=[],
            error=final_error_msg
        )

    try:
        vetting_results, csv_path = await vetting_agent_instance.perform_standalone_vetting(
            enriched_profiles=enriched_profiles_to_vet,
            ideal_podcast_description=vetting_criteria.ideal_podcast_description,
            guest_bio=vetting_criteria.guest_bio,
            guest_talking_points=vetting_criteria.guest_talking_points,
            source_campaign_id=source_campaign_id
        )
        
        final_message = f"Standalone vetting from CSV completed. Processed {len(enriched_profiles_to_vet)} valid profiles."
        if parsing_errors:
            final_message += f" ({len(parsing_errors)} rows from CSV had parsing issues and were skipped.)"

        return StandaloneVettingResponse(
            message=final_message,
            count=len(enriched_profiles_to_vet), # Count of successfully parsed and processed profiles
            vetting_results=vetting_results,
            csv_file_path=csv_path,
            error= "; ".join(parsing_errors[:3]) if parsing_errors else None # Include some parsing errors in the main error field if any
        )
    except Exception as e:
        logger.exception(f"Error during standalone vetting from CSV: {e}")
        return StandaloneVettingResponse(
            message="Standalone vetting from CSV failed during agent execution.",
            count=len(enriched_profiles_to_vet),
            vetting_results=[],
            error=str(e)
        )

# --- Old Background Workflow Trigger Endpoint (COMMENTED OUT) --- #
# @app.post("/campaigns/run_background", status_code=202, tags=["Workflow"])
# async def start_campaign_workflow(
#     campaign_config: CampaignConfiguration,
#     background_tasks: BackgroundTasks
# ):
#     # ... original background implementation ...
#     pass

# --- NEW: Authentication Endpoint --- #
@app.post("/auth/validate-login", status_code=status.HTTP_200_OK, tags=["Authentication"])
async def validate_login(login_request: LoginRequest, settings: Settings = Depends(get_settings)):
    # IMPORTANT: Ensure PGL_FRONTEND_PASSWORD is set in your Replit Secrets (or .env for local dev)
    # And that your Settings model in config.py loads it.
    # Example in Settings (src/config.py):
    # PGL_FRONTEND_PASSWORD: str
    
    expected_password = settings.PGL_FRONTEND_PASSWORD 
    # Add a check to ensure the setting was loaded
    if not expected_password or expected_password == "DEFAULT_PGL_PASSWORD_SHOULD_BE_OVERRIDDEN_BY_ENV": # Added check for default
        logger.error("CRITICAL: PGL_FRONTEND_PASSWORD is not configured in server settings or is using the default insecure value.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server authentication configuration error."
        )

    if login_request.password == expected_password:
        logger.info("Frontend login validation successful.")
        # Create a token
        access_token_expires = timedelta(minutes=settings.JWT_ACCESS_TOKEN_EXPIRE_MINUTES)
        # For now, the token subject can be simple. In a multi-user system, this would be user_id.
        access_token = create_access_token(
            data={"sub": "user_authenticated"}, # You can put more user info here if needed
            settings=settings, 
            expires_delta=access_token_expires
        )
        
        response = JSONResponse(content={"message": "Login successful", "access_token_type": "cookie"}) # Updated message
        response.set_cookie(
            key="session_token",
            value=access_token,
            httponly=True, # Client-side JS cannot access this cookie
            secure=True,   # Cookie will only be sent over HTTPS in production
            samesite="lax", # Mitigates CSRF
            max_age=int(access_token_expires.total_seconds()) # In seconds
            # domain= settings.COOKIE_DOMAIN (optional, for cross-subdomain cookies if needed)
            # path= "/" (optional, defaults to /)
        )
        return response
    else:
        logger.warning("Frontend login validation failed: Incorrect password.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials"
        )
# --- END NEW Authentication Endpoint --- #

# --- SPA Catch-all (MUST BE PLACED AFTER ALL OTHER API ROUTES BUT BEFORE STARTUP/SHUTDOWN EVENTS) --- #
# This serves the main index.html for any GET request that isn't an API route, crucial for SPAs.
FRONTEND_INDEX_HTML = os.path.join(FRONTEND_BUILD_DIR, "index.html") # Uses the updated FRONTEND_BUILD_DIR

if os.path.exists(FRONTEND_INDEX_HTML):
    @app.get("/{full_path:path}", include_in_schema=False) # THIS IS THE CATCH-ALL
    async def serve_react_app(request: Request, full_path: str): 
        path_parts = full_path.split('/')
        api_prefixes = ["webhooks", "actions", "campaigns", "users", "leads", "analytics", "static", "auth"]
        if path_parts and path_parts[0] in api_prefixes:
            logger.debug(f"Path {full_path} looks like API call, but not matched by other routes. Returning 404.")
            raise HTTPException(status_code=404, detail="Resource not found")

        potential_file_in_build_root = os.path.join(FRONTEND_BUILD_DIR, full_path)
        if "." in full_path.split("/")[-1] and os.path.exists(potential_file_in_build_root):
            logger.debug(f"Serving specific file from build root: {potential_file_in_build_root} for path: {full_path}")
            return FileResponse(potential_file_in_build_root)

        logger.debug(f"Serving SPA index.html ({FRONTEND_INDEX_HTML}) for path: {full_path}")
        if not os.path.exists(FRONTEND_INDEX_HTML):
             logger.error(f"SPA index.html not found at: {FRONTEND_INDEX_HTML} during request for {full_path}")
             raise HTTPException(status_code=500, detail="Frontend not available")
        return FileResponse(FRONTEND_INDEX_HTML)
    logger.info(f"SPA catch-all route configured to serve index.html from: {FRONTEND_INDEX_HTML}")
else:
    logger.warning(f"Frontend index.html not found at: {FRONTEND_INDEX_HTML}. SPA will not be served by catch-all. Ensure the frontend is built and path is correct.")

# --- Optional: Add startup/shutdown events --- #
@app.on_event("startup")
async def startup_event():
    """Connect to MongoDB and initialize Beanie on startup."""
    logger.info("FastAPI application starting up...")
    # Connect to MongoDB (uses settings implicitly loaded via Pydantic)
    # await connect_to_mongo()
    # await initialize_collections() # Ensure collections/indexes exist
    # Initialize Beanie (crucial for the checkpointer)
    # Pass the explicitly loaded settings object
    settings = get_settings() # <--- Get settings instance here
    await initialize_beanie_for_checkpointer(settings) # <--- Use the local settings instance
    logger.info("Database connections and Beanie initialized.")

@app.on_event("shutdown")
async def shutdown_event():
    """Disconnect from MongoDB on shutdown."""
    logger.info("Closing MongoDB connection.")
    # close_mongo_connection() 