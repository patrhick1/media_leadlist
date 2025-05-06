from typing import TypedDict, Optional

# Import the Pydantic AgentState model used by the graph
from ..models.state import AgentState

# --- Define the Graph State --- #
# LangGraph requires the state to be a TypedDict or derive from BaseMessage.
# We use a TypedDict that wraps our Pydantic AgentState model.
class GraphState(TypedDict):
    agent_state: AgentState
    # Add other graph-specific state elements here if needed
    error_message: Optional[str] = None 