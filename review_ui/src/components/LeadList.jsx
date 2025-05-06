import React, { useState, useEffect, useCallback, useRef } from 'react';
// Removed PropTypes as we'll rely on fetched data structure or define interfaces if using TS later
import LeadCard from './LeadCard';

// Basic styling for the list container
const listStyle = {
  maxWidth: '900px',
  margin: '20px auto',
  padding: '0 16px',
};

const controlsStyle = {
    display: 'flex',
    flexWrap: 'wrap', // Allow controls to wrap on smaller screens
    gap: '15px',       // Spacing between controls
    marginBottom: '20px',
    padding: '15px',
    border: '1px solid #ccc',
    borderRadius: '8px',
    backgroundColor: '#f9f9f9',
};

const controlGroupStyle = {
    display: 'flex',
    flexDirection: 'column',
    gap: '5px',
};

const labelStyle = {
    fontWeight: 'bold',
    fontSize: '0.9em',
    color: '#555',
};

const paginationStyle = {
    display: 'flex',
    justifyContent: 'space-between',
    marginTop: '20px',
};

const bulkActionsStyle = {
    display: 'flex',
    alignItems: 'center',
    gap: '15px',
    padding: '10px 15px',
    border: '1px solid #ccc',
    borderRadius: '8px',
    backgroundColor: '#f0f0f0', 
    marginBottom: '20px',
};

const selectAllLabelStyle = {
    display: 'flex',
    alignItems: 'center',
    gap: '5px',
    cursor: 'pointer',
    fontWeight: 'bold',
};

// --- NEW: Preferences Model (Placeholder - ideally import from shared types) ---
// Matches the Pydantic model in the backend
const defaultUserPreferences = {
    user_id: '',
    default_sort_by: 'date_added',
    default_sort_order: 'desc',
    default_page_size: 10,
    saved_filters: {},
};

// Removed onApprove/onReject props from LeadList signature, defined handlers inside
function LeadList() {
  const [leads, setLeads] = useState([]);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(defaultUserPreferences.default_page_size);
  const [totalLeads, setTotalLeads] = useState(0);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);
  const [filterTier, setFilterTier] = useState('');
  const [filterStatus, setFilterStatus] = useState('');
  const [minScore, setMinScore] = useState('');
  const [maxScore, setMaxScore] = useState('');
  const [searchTerm, setSearchTerm] = useState('');
  const [sortBy, setSortBy] = useState(defaultUserPreferences.default_sort_by);
  const [sortOrder, setSortOrder] = useState(defaultUserPreferences.default_sort_order);
  // --- NEW: State for Bulk Selection ---
  const [selectedLeadIds, setSelectedLeadIds] = useState(new Set());
  // --- NEW: State for bulk action processing --- 
  const [isBulkProcessing, setIsBulkProcessing] = useState(false);
  // --- NEW: State for active lead (keyboard nav) ---
  const [activeLeadId, setActiveLeadId] = useState(null);
  const listContainerRef = useRef(null); // Ref for scrolling

  // --- NEW: User ID and Preferences State ---
  const [userId, setUserId] = useState('test-user'); // Placeholder User ID
  const [userPreferences, setUserPreferences] = useState(defaultUserPreferences);
  const [prefsLoading, setPrefsLoading] = useState(false);
  const [prefsError, setPrefsError] = useState(null);

  const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

  // --- Effect to Load Preferences --- 
  useEffect(() => {
    if (!userId) { // Don't fetch if no user ID
        // Reset to defaults if user ID is cleared
        setSortBy(defaultUserPreferences.default_sort_by);
        setSortOrder(defaultUserPreferences.default_sort_order);
        setPageSize(defaultUserPreferences.default_page_size);
        setUserPreferences(defaultUserPreferences);
        return; 
    }
    
    const fetchPreferences = async () => {
        setPrefsLoading(true);
        setPrefsError(null);
        const prefsUrl = `${API_BASE_URL}/users/${userId}/preferences`;
        console.log("Fetching preferences from:", prefsUrl);
        try {
            const response = await fetch(prefsUrl);
            if (!response.ok) {
                 const errorData = await response.json().catch(() => ({ detail: 'Failed to load preferences' }));
                 // Handle 404 specifically? Backend currently returns defaults for new users.
                 throw new Error(`HTTP error ${response.status}: ${errorData?.detail || response.statusText}`);
            }
            const prefsData = await response.json();
            console.log("Loaded preferences:", prefsData);
            setUserPreferences(prefsData);
            // --- Set initial state based on loaded preferences --- 
            // Note: Query params in the URL should still override these if present on initial load,
            // but this sets the defaults for subsequent interactions.
            setSortBy(prefsData.default_sort_by || defaultUserPreferences.default_sort_by);
            setSortOrder(prefsData.default_sort_order || defaultUserPreferences.default_sort_order);
            setPageSize(prefsData.default_page_size || defaultUserPreferences.default_page_size);
            // TODO: Apply saved_filters if needed
            
        } catch (err) {
            console.error('Failed to fetch user preferences:', err);
            setPrefsError(err.message);
             // Revert to defaults on error
            setUserPreferences(defaultUserPreferences);
            setSortBy(defaultUserPreferences.default_sort_by);
            setSortOrder(defaultUserPreferences.default_sort_order);
            setPageSize(defaultUserPreferences.default_page_size);
        } finally {
            setPrefsLoading(false);
        }
    };

    fetchPreferences();
    
  }, [userId, API_BASE_URL]); // Re-fetch prefs if userId changes

  // --- Fetching Logic --- 
  const fetchLeads = useCallback(async () => {
    setIsLoading(true);
    setError(null);
    // Clear selection when fetching new leads
    setSelectedLeadIds(new Set());
    const params = new URLSearchParams();
    params.append('page', page.toString());
    params.append('page_size', pageSize.toString());
    if (filterTier) params.append('filter_tier', filterTier);
    if (filterStatus) params.append('filter_status', filterStatus);
    if (minScore) params.append('min_score', minScore);
    if (maxScore) params.append('max_score', maxScore);
    if (searchTerm) params.append('search_term', searchTerm);
    if (sortBy) params.append('sort_by', sortBy);
    if (sortOrder) params.append('sort_order', sortOrder);
    if (userId) params.append('user_id', userId);
    const apiUrl = `${API_BASE_URL}/leads?${params.toString()}`;
    console.log("Fetching from API:", apiUrl);
    try {
      const response = await fetch(apiUrl);
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ detail: 'Unknown error' }));
        throw new Error(`HTTP error ${response.status}: ${errorData?.detail || response.statusText}`);
      }
      const data = await response.json();
      setLeads(data.leads || []);
      setTotalLeads(data.total_leads || 0);
      if (data.page_size !== pageSize) {
        setPageSize(data.page_size);
      }
    } catch (err) {
      console.error('Failed to fetch leads:', err);
      setError(err.message);
      setLeads([]);
      setTotalLeads(0);
    } finally {
      setIsLoading(false);
    }
  }, [page, pageSize, filterTier, filterStatus, minScore, maxScore, searchTerm, sortBy, sortOrder, API_BASE_URL, userId]);

  useEffect(() => {
    fetchLeads();
  }, [fetchLeads]);

  // --- Event Handlers --- 
  const handleFilterOrSortChange = (setterFunc, value) => {
    setterFunc(value);
    setPage(1);
  };
  const handleNextPage = () => {
    if (page * pageSize < totalLeads) {
      setPage(prevPage => prevPage + 1);
    }
  };
  const handlePrevPage = () => {
    setPage(prevPage => Math.max(1, prevPage - 1));
  };

  // --- NEW: Selection Handler --- 
  const handleLeadSelectChange = (podcastId, isSelected) => {
    setSelectedLeadIds(prevSelectedIds => {
        const newSelectedIds = new Set(prevSelectedIds);
        if (isSelected) {
            newSelectedIds.add(podcastId);
        } else {
            newSelectedIds.delete(podcastId);
        }
        console.log("Selected IDs:", Array.from(newSelectedIds)); // Debug log
        return newSelectedIds;
    });
  };

  // --- NEW: Select All Handler --- 
  const handleSelectAllChange = (event) => {
      const isChecked = event.target.checked;
      if (isChecked) {
          // Select all IDs currently displayed
          const allCurrentIds = leads.map(lead => lead?.lead_info?.podcast_id).filter(id => !!id);
          setSelectedLeadIds(new Set(allCurrentIds));
      } else {
          // Deselect all
          setSelectedLeadIds(new Set());
      }
  };

  // --- NEW: Bulk Review Handler --- 
  const handleBulkReviewAction = async (approved) => {
      const idsToProcess = Array.from(selectedLeadIds);
      if (idsToProcess.length === 0) {
          alert("Please select at least one lead.");
          return;
      }

      const action = approved ? "approve" : "reject";
      if (!confirm(`Are you sure you want to ${action} ${idsToProcess.length} selected lead(s)?`)) {
          return;
      }

      setIsBulkProcessing(true);
      const apiUrl = `${API_BASE_URL}/leads/bulk-review`;
      console.log(`Performing bulk ${action} for IDs:`, idsToProcess);

      try {
          const response = await fetch(apiUrl, {
              method: 'POST',
              headers: {
                  'Content-Type': 'application/json',
              },
              body: JSON.stringify({
                  podcast_ids: idsToProcess,
                  approved: approved,
                  // Note: Bulk endpoint doesn't currently take feedback
              }),
          });

          const result = await response.json(); // Expect BulkReviewResponse

          if (!response.ok) {
              throw new Error(`Bulk review failed: ${result?.detail || response.statusText} (Processed: ${result?.processed_count}, Failed: ${result?.failed_count})`);
          }

          alert(`Bulk ${action} successful! Processed: ${result.processed_count}, Success: ${result.success_count}, Failed: ${result.failed_count}`);
          
          if (result.failures && result.failures.length > 0) {
              console.error("Bulk review failures:", result.failures);
              // Optionally display failures more prominently
          }
          
          // Refresh leads list after successful bulk action
          fetchLeads(); 
          // Selection is cleared automatically by fetchLeads

      } catch (err) {
          console.error(`Failed to submit bulk ${action}:`, err);
          alert(`Error submitting bulk ${action}: ${err.message}`);
      } finally {
          setIsBulkProcessing(false);
      }
  };

  // --- Review Action Handlers --- 
  const handleReviewAction = async (podcastId, approved, feedback) => {
    console.log(`Review action: podcastId=${podcastId}, approved=${approved}, feedback=${feedback}`);
    const apiUrl = `${API_BASE_URL}/leads/${podcastId}/review`;
    
    try {
      const response = await fetch(apiUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          approved: approved,
          feedback: feedback, // Backend expects optional string or null
        }),
      });

      if (!response.ok) {
          // Handle HTTP errors from the review endpoint
          // The 204 response won't have a body, handle others
          let errorDetail = `HTTP error ${response.status}: ${response.statusText}`;
          if (response.status !== 204) {
              try {
                 const errorData = await response.json();
                 errorDetail = errorData?.detail || errorDetail;
              } catch (parseError) {
                  // Ignore if body can't be parsed
              }
          } 
          throw new Error(errorDetail);
      }

      // Review submitted successfully (204 No Content)
      console.log(`Review for ${podcastId} submitted successfully.`);
      
      // Option 1: Re-fetch the current page to reflect the change
      // fetchLeads(); 
      
      // Option 2: Update local state (more complex, avoids extra fetch)
      setLeads(prevLeads => prevLeads.map(lead => 
          lead.lead_info.podcast_id === podcastId 
            ? { ...lead, review_status: approved ? 'approved' : 'rejected' } 
            : lead
      ));
      // Note: This assumes the backend sets the status. If we just want to remove 
      // the item from the list after review, we could filter instead:
      // setLeads(prevLeads => prevLeads.filter(lead => lead.lead_info.podcast_id !== podcastId));
      
      // --- NEW: Clear active lead after action ---
      if (podcastId === activeLeadId) {
          setActiveLeadId(null); // Clear focus after action
      }
      
    } catch (err) {
      console.error(`Failed to submit review for ${podcastId}:`, err);
      // TODO: Show error message to the user in the UI
      alert(`Error submitting review: ${err.message}`); // Simple alert for now
    }
  };

  const handleApprove = (podcastId, feedback) => {
    handleReviewAction(podcastId, true, feedback);
  };

  const handleReject = (podcastId, feedback) => {
    handleReviewAction(podcastId, false, feedback);
  };

  // --- NEW: Keyboard Navigation/Action Handler --- 
  useEffect(() => {
      const handleKeyDown = (event) => {
          // Ignore if modifier keys are pressed (e.g., Cmd+R)
          if (event.metaKey || event.ctrlKey || event.altKey || event.shiftKey) {
              return;
          }
          // Ignore if typing in an input/textarea
          if (event.target.tagName === 'INPUT' || event.target.tagName === 'TEXTAREA' || event.target.tagName === 'SELECT') {
              return;
          }

          let currentActiveIndex = -1;
          if (activeLeadId) {
              currentActiveIndex = leads.findIndex(lead => lead?.lead_info?.podcast_id === activeLeadId);
          }
          
          let nextActiveId = null;

          switch (event.key) {
              case 'ArrowDown':
              case 'j': // Vim-style down
                  event.preventDefault(); // Prevent page scroll
                  if (currentActiveIndex === -1 && leads.length > 0) {
                      nextActiveId = leads[0]?.lead_info?.podcast_id; // Start at top if nothing active
                  } else if (currentActiveIndex < leads.length - 1) {
                      nextActiveId = leads[currentActiveIndex + 1]?.lead_info?.podcast_id;
                  }
                  break;
              case 'ArrowUp':
              case 'k': // Vim-style up
                  event.preventDefault(); // Prevent page scroll
                  if (currentActiveIndex > 0) {
                      nextActiveId = leads[currentActiveIndex - 1]?.lead_info?.podcast_id;
                  }
                  break;
              case 'a': // Approve
                  if (activeLeadId) {
                      console.log(`Keyboard: Approving ${activeLeadId}`);
                      handleApprove(activeLeadId, null); // Approve without feedback
                  }
                  break;
              case 'r': // Reject
                  if (activeLeadId) {
                      console.log(`Keyboard: Rejecting ${activeLeadId}`);
                      handleReject(activeLeadId, null); // Reject without feedback
                  }
                  break;
              // Add other shortcuts here if needed (e.g., for pagination)
              default:
                  break;
          }

          if (nextActiveId && nextActiveId !== activeLeadId) {
              setActiveLeadId(nextActiveId);
              // Scroll the newly active card into view
              // Simple implementation - might need refinement for exact positioning
              const nextCardElement = listContainerRef.current?.querySelector(`[data-podcast-id="${nextActiveId}"]`);
              nextCardElement?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
          }
      };

      document.addEventListener('keydown', handleKeyDown);
      // Cleanup listener on unmount
      return () => {
          document.removeEventListener('keydown', handleKeyDown);
      };
  // Depend on leads array to recalculate indices, and action handlers
  }, [leads, activeLeadId, handleApprove, handleReject]); 

  // --- Render Logic --- 
  const totalPages = Math.ceil(totalLeads / pageSize);
  // Determine if "Select All" checkbox should be checked
  const allSelected = leads.length > 0 && selectedLeadIds.size === leads.length && 
                      leads.every(lead => selectedLeadIds.has(lead?.lead_info?.podcast_id));
  const noneSelected = selectedLeadIds.size === 0;

  return (
    <div style={listStyle} ref={listContainerRef}>
      {/* --- NEW: User ID Input (Placeholder) --- */} 
      <div style={{ marginBottom: '10px', padding: '5px', border: '1px dashed grey' }}>
          <label htmlFor="user-id-input" style={{ marginRight: '5px', fontWeight: 'bold' }}>User ID:</label>
          <input 
              id="user-id-input"
              type="text" 
              value={userId}
              onChange={(e) => setUserId(e.target.value)} 
              placeholder="Enter User ID"
          />
           {prefsLoading && <span style={{ marginLeft: '10px' }}> Loading preferences...</span>}
           {prefsError && <span style={{ marginLeft: '10px', color: 'red' }}> Error loading preferences!</span>}
          {/* TODO: Add Save Preferences Button Here */} 
      </div>

      <h2>Leads for Review ({totalLeads} total)</h2>
      
      {/* Filter and Sort Controls */}
      <div style={controlsStyle}>
          {/* Search Term */}
          <div style={controlGroupStyle}>
              <label htmlFor="search-term" style={labelStyle}>Search:</label>
              <input 
                  id="search-term"
                  type="text" 
                  value={searchTerm} 
                  onChange={(e) => handleFilterOrSortChange(setSearchTerm, e.target.value)} 
                  placeholder="Name or description..."
              />
          </div>
          {/* Filter Tier */}
          <div style={controlGroupStyle}>
              <label htmlFor="filter-tier" style={labelStyle}>Tier:</label>
              <select id="filter-tier" value={filterTier} onChange={(e) => handleFilterOrSortChange(setFilterTier, e.target.value)}>
                  <option value="">All</option>
                  <option value="A">A</option>
                  <option value="B">B</option>
                  <option value="C">C</option>
                  <option value="D">D</option>
              </select>
          </div>
          {/* Filter Status */}
           <div style={controlGroupStyle}>
              <label htmlFor="filter-status" style={labelStyle}>Status:</label>
              <select id="filter-status" value={filterStatus} onChange={(e) => handleFilterOrSortChange(setFilterStatus, e.target.value)}>
                  <option value="">All</option>
                  <option value="pending">Pending</option>
                  <option value="approved">Approved</option>
                  <option value="rejected">Rejected</option>
              </select>
          </div>
          {/* Score Range */}
          <div style={controlGroupStyle}>
              <label style={labelStyle}>Score:</label>
              <div style={{display: 'flex', gap: '5px', alignItems: 'center'}}>
                  <input 
                      type="number" 
                      value={minScore} 
                      onChange={(e) => handleFilterOrSortChange(setMinScore, e.target.value)} 
                      placeholder="Min" 
                      min="0" max="100" 
                      style={{width: '60px'}}
                  />
                  <span>-</span>
                  <input 
                      type="number" 
                      value={maxScore} 
                      onChange={(e) => handleFilterOrSortChange(setMaxScore, e.target.value)} 
                      placeholder="Max" 
                      min="0" max="100" 
                      style={{width: '60px'}}
                  />
              </div>
          </div>
           {/* Sort By */}
          <div style={controlGroupStyle}>
              <label htmlFor="sort-by" style={labelStyle}>Sort By:</label>
              <select id="sort-by" value={sortBy} onChange={(e) => handleFilterOrSortChange(setSortBy, e.target.value)}>
                  <option value="date_added">Date Added</option>
                  <option value="score">Score</option>
                  <option value="name">Name</option>
              </select>
          </div>
          {/* Sort Order */}
          <div style={controlGroupStyle}>
              <label htmlFor="sort-order" style={labelStyle}>Order:</label>
              <select id="sort-order" value={sortOrder} onChange={(e) => handleFilterOrSortChange(setSortOrder, e.target.value)}>
                  <option value="desc">Descending</option>
                  <option value="asc">Ascending</option>
              </select>
          </div>
      </div>

      {/* --- NEW: Bulk Action Controls --- */} 
      <div style={bulkActionsStyle}>
          <label style={selectAllLabelStyle} htmlFor="select-all">
              <input 
                  type="checkbox" 
                  id="select-all"
                  checked={allSelected}
                  onChange={handleSelectAllChange}
                  disabled={isLoading || leads.length === 0} // Disable while loading or if no leads
              />
              Select All Visible
          </label>
          <button 
              onClick={() => handleBulkReviewAction(true)} 
              disabled={noneSelected || isBulkProcessing}> 
              {isBulkProcessing ? 'Processing...' : 'Approve Selected'}
          </button>
          <button 
              onClick={() => handleBulkReviewAction(false)} 
              disabled={noneSelected || isBulkProcessing}>
               {isBulkProcessing ? 'Processing...' : 'Reject Selected'}
          </button>
          <span>({selectedLeadIds.size} selected)</span>
      </div>

      {isLoading && <p>Loading leads...</p>}
      {error && <p style={{ color: 'red' }}>Error fetching leads: {error}</p>}
      {!isLoading && !error && leads.length === 0 && <p>No leads match the current filters.</p>}

      {!isLoading && !error && leads.length > 0 && leads.map((lead, index) => (
          // --- NEW: Add data attribute for scrolling --- 
          <div 
              key={lead?.lead_info?.podcast_id} 
              onClick={() => setActiveLeadId(lead?.lead_info?.podcast_id)}
              data-podcast-id={lead?.lead_info?.podcast_id} // Add ID for querySelector
          >
        <LeadCard
          lead={lead}
                onApprove={handleApprove}
                onReject={handleReject}
                isSelected={selectedLeadIds.has(lead?.lead_info?.podcast_id)}
                onSelectChange={handleLeadSelectChange}
                isActive={activeLeadId === lead?.lead_info?.podcast_id}
              />
          </div>
      ))}
      
      {!isLoading && !error && totalPages > 1 && (
          <div style={paginationStyle}>
            <button onClick={handlePrevPage} disabled={page <= 1}>
              Previous
            </button>
            <span>Page {page} of {totalPages}</span>
            <button onClick={handleNextPage} disabled={page >= totalPages}>
              Next
            </button>
          </div>
      )}
    </div>
  );
}

// Removed propTypes as state is now internal

export default LeadList; 