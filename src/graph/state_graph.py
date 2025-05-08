import logging
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.base import BaseCheckpointSaver
from typing import TypedDict, List, Dict, Optional
import uuid
import time # Import time
import asyncio # Add asyncio import at the top

# Import our Pydantic state model
from ..models.state import AgentState
# Import the checkpointer
from ..persistence.state_manager import get_checkpoint_saver
# Import models for example usage
from ..models.campaign import CampaignConfiguration
# Import the actual agent
from ..agents.search_agent import SearchAgent
from ..agents.vetting_agent import VettingAgent
from ..agents.enrichment_agent import EnrichmentAgent
# Import CRMAgent
from ..agents.crm_agent import CRMAgent
# Import MetricsService
from ..services.metrics_service import MetricsService
# Import MongoDB access
# from ..persistence.mongodb import _get_collection, LEADS_COLLECTION 
# Import GraphState from the new file
from .graph_types import GraphState

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Define the State (MOVED to graph_types.py) --- #
# class GraphState(TypedDict):
#     agent_state: AgentState
#     # We can add other graph-specific state elements here if needed
#     error_message: Optional[str] = None

# --- Agent Node Functions --- #

def search_agent_node(state: GraphState) -> GraphState:
    """Node that executes the search agent, choosing the method based on search_type."""
    logger.info("--- Calling Search Agent Node --- ")
    try:
        agent = SearchAgent()
        agent_state: AgentState = state['agent_state']
        campaign_config: CampaignConfiguration = agent_state.campaign_config
        
        if not campaign_config:
             # Handle error: No campaign config found
            error_msg = "CampaignConfiguration not found in agent state."
            logger.error(error_msg)
            agent_state.current_step = "error"
            agent_state.execution_status = "search_failed_no_config"
            state['error_message'] = error_msg
            state['agent_state'] = agent_state
            return state

        search_type = campaign_config.search_type

        if search_type == "related":
            seed_rss = campaign_config.seed_rss_url
            if not seed_rss:
                error_msg = "Search type is 'related' but no seed_rss_url provided in CampaignConfiguration."
                logger.error(error_msg)
                agent_state.current_step = "error"
                agent_state.execution_status = "search_failed_no_seed_rss"
                state['error_message'] = error_msg
                state['agent_state'] = agent_state
                return state
            
            logger.info(f"--- Search Node: Routing to RELATED search for seed: {seed_rss} ---")
            # --- PASS PARAMS TO STANDALONE METHOD --- #
            try:
                related_leads, csv_path = agent.perform_standalone_related_search(
                    seed_rss_url=seed_rss,
                    max_depth=campaign_config.max_depth or 2, # Use config value or default
                    max_total_results=campaign_config.max_total_results or 50, # Use config value or default
                    campaign_id_prefix=f"graph_related_{campaign_config.campaign_id}" # Use graph campaign ID
                )
                agent_state.leads = related_leads
                agent_state.search_results_csv_path = csv_path
                agent_state.execution_status = "related_search_complete" if related_leads else "related_search_complete_no_results"
                agent_state.current_step = "enrichment" # Set next step here on success
                state['error_message'] = None
            except Exception as related_err:
                 logger.exception(f"Error calling perform_standalone_related_search: {related_err}")
                 agent_state.current_step = "error"
                 agent_state.execution_status = "search_failed_agent_execution"
                 state['error_message'] = f"Related search agent execution failed: {related_err}"
            # --- END PASS PARAMS --- #
            state['agent_state'] = agent_state # Update state regardless of inner try/except
            updated_state = state # Return the modified state
            # updated_state = agent.run_related_search(state) # Call the new method # OLD CALL
        
        elif search_type == "topic":
             if not campaign_config.target_audience:
                  # Handle error: topic search requires description
                  error_msg = "Search type is 'topic' but no target_audience provided in CampaignConfiguration."
                  logger.error(error_msg)
                  agent_state.current_step = "error"
                  agent_state.execution_status = "search_failed_no_description"
                  state['error_message'] = error_msg
                  state['agent_state'] = agent_state
                  return state
             
             logger.info("--- Search Node: Routing to TOPIC search based on description ---")
             # --- PASS PARAMS TO STANDALONE METHOD --- #
             try:
                 topic_leads, csv_path = agent.perform_standalone_topic_search(
                     target_audience=campaign_config.target_audience,
                     key_messages=campaign_config.key_messages or [], # Use config value or default
                     num_keywords_to_generate=campaign_config.num_keywords_to_generate or 10,
                     max_results_per_keyword=campaign_config.max_results_per_keyword or 50,
                     campaign_id_prefix=f"graph_topic_{campaign_config.campaign_id}" # Use graph campaign ID
                 )
                 agent_state.leads = topic_leads
                 agent_state.search_results_csv_path = csv_path
                 agent_state.execution_status = "search_complete" if topic_leads else "search_complete_no_results"
                 agent_state.current_step = "enrichment" # Set next step here on success
                 state['error_message'] = None
             except Exception as topic_err:
                  logger.exception(f"Error calling perform_standalone_topic_search: {topic_err}")
                  agent_state.current_step = "error"
                  agent_state.execution_status = "search_failed_agent_execution"
                  state['error_message'] = f"Topic search agent execution failed: {topic_err}"
             # --- END PASS PARAMS --- #
             state['agent_state'] = agent_state # Update state regardless of inner try/except
             updated_state = state # Return the modified state
             # updated_state = agent.run_search(state) # Call the existing method # OLD CALL
        
        else:
             # Handle error: Invalid search_type
             error_msg = f"Invalid search_type '{search_type}' found in CampaignConfiguration."
             logger.error(error_msg)
             agent_state.current_step = "error"
             agent_state.execution_status = "search_failed_invalid_type"
             state['error_message'] = error_msg
             state['agent_state'] = agent_state
             return state
             
        return updated_state
        
    except Exception as e:
        logger.exception("Critical error in search_agent_node execution.")
        # Update state to reflect critical node failure
        # Ensure agent_state exists before trying to modify it
        if 'agent_state' in state:
             state['agent_state'].current_step = "error"
             state['agent_state'].execution_status = "node_failed"
        state['error_message'] = f"Search node failed critically: {e}"
        # Ensure agent_state is returned even if it couldn't be modified fully
        if 'agent_state' not in state:
             state['agent_state'] = AgentState(current_step="error", execution_status="node_failed") # Create minimal error state
        return state

def vetting_agent_node(state: GraphState) -> GraphState:
    """Node that executes the vetting agent in graph mode."""
    logger.info("--- Calling Vetting Agent Node --- ")
    try:
        # Instantiate the agent
        agent = VettingAgent() 
        
        # The agent method is async, run it using asyncio.run()
        # This will block the node until the async function completes.
        # It returns the modified GraphState dictionary.
        updated_state = asyncio.run(agent.run_vetting_graph_mode(state)) 
        return updated_state
        
    except Exception as e:
        logger.exception("Critical error in vetting_agent_node execution (before or during agent call).")
        # Ensure agent_state exists before trying to modify it
        if 'agent_state' in state:
             state['agent_state'].current_step = "error"
             state['agent_state'].execution_status = "vetting_node_failed_critically"
        else:
             # Create a minimal error state if agent_state doesn't exist
             state['agent_state'] = AgentState(current_step="error", execution_status="vetting_node_failed_critically")
        state['error_message'] = f"Vetting node failed critically: {e}"
        # Log error metric if possible (MetricsService might not be available here)
        # metrics_service = MetricsService()
        # if metrics_service: ...
        return state

def enrichment_agent_node(state: GraphState) -> GraphState:
    """Node that executes the enrichment agent."""
    logger.info("--- Calling Enrichment Agent Node --- ")
    try:
        if not state['agent_state'].leads:
            logger.warning("No leads found in state for enrichment. Skipping enrichment step.")
            # Decide next step: Go to END if enrichment is skipped
            # This depends on the desired workflow. Let's assume END for now.
            state['agent_state'].current_step = "completed" # Mark as completed or skipped
            state['agent_state'].execution_status = "enrichment_skipped_no_leads"
            # Return the updated state directly
            return state # Important: Return the state dict here

        agent = EnrichmentAgent()
        # Run the async function synchronously and get the result dict
        updated_state = asyncio.run(agent.run_enrichment(state))
        return updated_state
    except Exception as e:
        logger.exception("Critical error in enrichment_agent_node execution.")
        # Ensure agent_state exists before trying to modify it
        if 'agent_state' in state:
            state['agent_state'].current_step = "error"
            state['agent_state'].execution_status = "node_failed"
        state['error_message'] = f"Enrichment node failed critically: {e}"
        # Ensure agent_state is returned even if it couldn't be modified fully
        if 'agent_state' not in state:
             state['agent_state'] = AgentState(current_step="error", execution_status="node_failed") # Create minimal error state
        return state

def human_review_node(state: GraphState) -> GraphState:
    logger.info("--- Executing Human Review Node (Placeholder) --- ")
    current_state = state['agent_state']
    # Log enriched data before moving on (example)
    logger.info(f"Enriched profiles count: {len(current_state.enriched_profiles)}")
    if current_state.enriched_profiles:
        logger.info(f"Example enriched profile[0] sources: {current_state.enriched_profiles[0].data_sources}")
    
    current_state.current_step = "crm_export"
    state['agent_state'] = current_state
    logger.info("Human Review Node completed (Placeholder: Logged enriched data). ")
    return state

# --- Define CRM Export Node Placeholder --- #
def crm_export_node(state: GraphState) -> GraphState:
    """Placeholder node for CRM export. Not currently functional."""
    logger.info("--- Skipping CRM Export Node (Placeholder) --- ")
    # In a real scenario, this would interact with the CRM
    agent_state = state['agent_state']
    agent_state.current_step = "completed"
    agent_state.execution_status = "crm_export_skipped_placeholder"
    state['agent_state'] = agent_state
    return state

# --- Define Conditional Edge Functions --- #
def decide_after_search(state: GraphState) -> str:
    """Decides the next step after the search agent node.

    Returns:
        str: The name of the next node ('enrichment' or '__end__').
    """
    agent_state: AgentState = state['agent_state']
    status = agent_state.execution_status
    error_message = state.get('error_message')

    # --- Failure / No-result conditions --- #
    if (
        error_message
        or "failed" in status
        or status.endswith("_no_results")  # Handles both topic and related variants
    ):
        logger.warning(
            f"Search step failed or yielded no results (Status: {status}). Ending workflow."
        )
        return "__end__"  # Go directly to END if search failed or found nothing

    # --- Success conditions that should continue to enrichment --- #
    if (
        status.startswith("search_complete")
        or status.startswith("related_search_complete")
    ):
        logger.info(
            f"Search step successful (Status: {status}). Proceeding to enrichment."
        )
        return "enrichment"  # Proceed to enrichment if search was successful

    # --- Fallback --- #
    logger.error(
        f"Unknown status after search node: {status}. Ending workflow as a precaution."
    )
    return "__end__"  # Fallback to end on unexpected status

# --- NEW: Define decide_after_vetting function ---
def decide_after_vetting(state: GraphState) -> str:
    """Decides the next step after the vetting agent node.

    Returns:
        str: The name of the next node ('crm_export' or '__end__').
    """
    agent_state: AgentState = state['agent_state']
    status = agent_state.execution_status
    error_message = state.get('error_message')
    vetting_results = agent_state.vetting_results

    logger.info(f"Deciding after vetting. Status: {status}, Error: {error_message}, Vetting Results Count: {len(vetting_results) if vetting_results else 0}")

    # --- Failure / No-result conditions from vetting step itself ---
    if (
        error_message
        or "failed" in status # Catches various failure statuses from the vetting node
        or not vetting_results # No vetting results produced
    ):
        logger.warning(
            f"Vetting step failed or yielded no results (Status: {status}). Ending workflow."
        )
        return "__end__"

    # --- Success conditions: Check if there are any usable vetting results ---
    # Example: Check if at least one podcast was successfully vetted (not errored within its result)
    # And perhaps meets a minimum quality (e.g., not all are 'D' tier or unvetted due to errors)
    # For simplicity now, if we have any results and no node error, proceed.
    # A more nuanced check could be:
    # successful_vets = [vr for vr in vetting_results if not vr.error and vr.quality_tier != "Unvetted"]
    # if not successful_vets:
    #     logger.warning("Vetting completed, but no successfully vetted profiles found. Ending.")
    #     return "__end__"
    
    if status == "vetting_complete" and vetting_results: # Assuming "vetting_complete" is set on success
        logger.info(
            f"Vetting step successful (Status: {status}). Proceeding to CRM export."
        )
        return "crm_export"

    # --- Fallback --- #
    logger.error(
        f"Unknown status or condition after vetting node: {status}. Vetting results count: {len(vetting_results) if vetting_results else 0}. Ending workflow as a precaution."
    )
    return "__end__"
# --- END NEW decide_after_vetting function ---

# --- Build the Graph --- #
def build_graph(checkpointer: Optional[BaseCheckpointSaver] = None):
    """Builds the state machine graph."""
    workflow = StateGraph(GraphState)

    # Add nodes
    workflow.add_node("search", search_agent_node)
    workflow.add_node("enrichment", enrichment_agent_node)
    workflow.add_node("vetting", vetting_agent_node) # Vetting node added
    workflow.add_node("crm_export", crm_export_node) # CRM node added
    # workflow.add_node("human_review", human_review_node) # Human review node (kept for reference)

    # Define edges
    workflow.set_entry_point("search")

    # Conditional Edge from Search (Simplified)
    workflow.add_conditional_edges(
        "search",
        decide_after_search, 
        {
            "enrichment": "enrichment", 
            "__end__": END          
        }
    )

    # Updated Edge from Enrichment to Vetting
    workflow.add_edge("enrichment", "vetting")

    # Conditional Edge from Vetting
    workflow.add_conditional_edges(
        "vetting",
        decide_after_vetting, # Use the new decision function
        {
            "crm_export": "crm_export", # If approved/good quality
            "__end__": END # If rejected or error
        }
    )

    # Normal Edge from CRM Export to End
    workflow.add_edge("crm_export", END)

    # Normal Edge from Human Review to End (Kept commented for reference)
    # workflow.add_edge("human_review", END)

    # Compile the graph
    app = workflow.compile(
        checkpointer=checkpointer,
        # interrupt_before=["human_review"], # Example: Interrupt before human review
        )
    return app

# --- Graph Execution --- #

def run_workflow(initial_state: AgentState, checkpointer: Optional[BaseCheckpointSaver]):
    """Runs the workflow. Persistence is optional based on checkpointer."""
    app = build_graph(checkpointer=checkpointer)

    # Use a unique identifier for the thread, e.g., campaign_id
    thread_id = initial_state.campaign_config.campaign_id
    config = {"configurable": {"thread_id": thread_id}}

    graph_input = GraphState(agent_state=initial_state)
    logger.info(f"Running workflow for thread_id: {thread_id}")

    # Check if there's existing state to resume from (only if checkpointer exists)
    if checkpointer:
        existing_state_tuple = checkpointer.get_tuple(config)
        if existing_state_tuple:
            logger.info(f"Resuming workflow from existing checkpoint for thread_id: {thread_id}")
            # The graph will automatically pick up from the last saved state
        else:
            logger.info(f"Starting new workflow run for thread_id: {thread_id} (with persistence)")
    else:
        logger.info(f"Starting new workflow run for thread_id: {thread_id} (no persistence)")

    # Use stream to see intermediate steps and allow for interruption/resumption
    last_state = None # Initialize last_state
    for output in app.stream(graph_input, config=config, stream_mode="values"):
        # output is the state after each step has executed
        last_state = output # Keep track of the most recent state
        step_name = list(output.keys())[-1] # Get the name of the node that just ran
        logger.info(f"<- Step '{step_name}' completed. Current state: {last_state['agent_state'].current_step}, Status: {last_state['agent_state'].execution_status} ->")
        # We could add logic here to inspect the state and potentially pause/interrupt

    # After the stream finishes, get the final state directly if needed
    # Note: streaming already updates the checkpointer if it exists
    final_agent_state = None
    if checkpointer:
        final_state_tuple = checkpointer.get_tuple(config)
        if final_state_tuple:
            final_graph_state = final_state_tuple.checkpoint
            logger.info(f"Workflow finished for thread_id: {thread_id}. Final status: {final_graph_state['agent_state'].execution_status}")
            final_agent_state = final_graph_state['agent_state']
        else:
            logger.error(f"Could not retrieve final state via checkpointer for thread_id: {thread_id}")
    
    # Fallback to the last observed state from the stream if checkpointer failed or doesn't exist
    if final_agent_state is None and last_state:
         logger.info(f"Workflow finished for thread_id: {thread_id}. Final status (from stream): {last_state['agent_state'].execution_status}")
         final_agent_state = last_state['agent_state']
    elif final_agent_state is None:
         logger.error(f"Workflow finished for thread_id: {thread_id}, but could not determine final state.")

    return final_agent_state

if __name__ == '__main__':
    # Example standalone execution for testing persistence
    # Set up basic logging for the example
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger.info("Testing StateGraph build and execution...")

    checkpointer = None
    # --- Uncomment below to test with persistence --- 
    # from ..persistence.mongodb import connect_to_mongo, close_mongo_connection, initialize_collections
    # try:
    #     # Setup DB connection and checkpointer
    #     connect_to_mongo()
    #     initialize_collections() # Ensure MongoDB collections exist
    #     checkpointer = get_checkpoint_saver()
    #     logger.info("Persistence checkpointer obtained.")
    # except Exception as e:
    #     logger.error(f"Failed to set up persistence: {e}. Running without checkpointer.")
    #     checkpointer = None
    # --- End Persistence Setup Block ---
    
    # Create a dummy initial state with a descriptive target audience
    test_campaign_id = f"test_campaign_{uuid.uuid4()}"
    test_description = "Podcasts focused on early-stage SaaS startups, particularly those discussing product-market fit, bootstrapping, and interviews with founders who have recently raised seed funding in the fintech or healthtech sectors."
    
    initial_campaign = CampaignConfiguration(
        campaign_id=test_campaign_id,
        target_audience=test_description, # Use descriptive audience
        key_messages=["Test key message"],
        tone_preferences="Neutral"
    )
    initial_agent_state = AgentState(
        current_step="search", # Start at the beginning
        campaign_config=initial_campaign,
        execution_status="pending"
    )

    print(f"\n--- Running workflow for campaign: {initial_campaign.campaign_id} ---")
    # Pass checkpointer (which might be None) to run_workflow
    final_state = run_workflow(initial_agent_state, checkpointer)
    if final_state:
         print(f"--- Workflow completed. Final step: {final_state.current_step}, Status: {final_state.execution_status} ---")
         # Check for the CSV path if search completed successfully
         if hasattr(final_state, 'search_results_csv_path') and final_state.search_results_csv_path:
              print(f"Search results saved to: {final_state.search_results_csv_path}")
         else:
              print("Search results CSV path not found in final state.")
    else:
         print("--- Workflow execution finished with errors or no final state retrieved. ---")

    # --- Uncomment below if testing with persistence ---
    # finally:
    #     # Close the MongoDB connection if it was used
    #     if 'close_mongo_connection' in locals():
    #          close_mongo_connection()
    #          logger.info("MongoDB connection closed.")
    # --- End Persistence Teardown --- 
    print("\nTest finished.") 