import { useState, useEffect, useCallback } from 'react'; // Import hooks and useCallback

// import type { Route } from "./+types/home"; // Comment out for now
// Remove Welcome component import
// import { Welcome } from "../welcome/welcome";
// @ts-ignore - Suppress TS error for JS component
import LeadList from "../../src/components/LeadList"; // Adjust path
// @ts-ignore - Suppress TS error for JS service
import { getLeads, submitReview } from '../../src/services/api'; // Import API function and submitReview

// --- Remove Dummy Data --- //
/*
const DUMMY_LEADS = [
  // ... dummy data removed ...
];
*/
// ------------------------- //

// @ts-ignore - Adjust function signature if Route type isn't available
export function meta({}: /* Route.MetaArgs */ any) {
  return [
    // Update title/description for our app
    { title: "Podcast Lead Review" },
    { name: "description", content: "Review vetted podcast leads." },
  ];
}

export default function Home() {
  // State variables
  const [leads, setLeads] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null); // Add type for error state
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [totalLeads, setTotalLeads] = useState(0);
  const pageSize = 10; // Or make this configurable

  // Filter and Sort State
  const [filterTier, setFilterTier] = useState<string>('all'); // e.g., 'all', 'A', 'B', 'C'
  // TODO: Add filterStatus state (e.g., 'all', 'pending', 'approved', 'rejected')
  const [sortBy, setSortBy] = useState<string>('date_added'); // e.g., 'date_added', 'score'
  const [sortOrder, setSortOrder] = useState<string>('desc'); // 'asc' or 'desc'

  // --- Fetch Leads Logic (now depends on filters/sort) ---
  const fetchLeads = useCallback(async () => {
    setIsLoading(true);
    setError(null);
    console.log(`Fetching page ${currentPage} with filters: tier=${filterTier}, sort=${sortBy} ${sortOrder}...`);
    try {
      // Pass filter/sort state to getLeads (API function needs update)
      const data = await getLeads(currentPage, pageSize, filterTier, sortBy, sortOrder);
      setLeads(data.leads || []);
      setTotalLeads(data.total_leads || 0);
      setTotalPages(Math.ceil((data.total_leads || 0) / pageSize));
      console.log("Fetch successful");
    } catch (err: any) {
      console.error("Caught error fetching leads:", err);
      setError(err.message || 'Failed to fetch leads.');
      setLeads([]);
      setTotalPages(1);
      setTotalLeads(0);
    } finally {
      setIsLoading(false);
    }
    // Dependencies now include filter and sort states
  }, [currentPage, filterTier, sortBy, sortOrder]);

  useEffect(() => {
    // Reset to page 1 when filters or sort change
    setCurrentPage(1);
    fetchLeads();
  }, [fetchLeads]); // fetchLeads dependency implicitly covers filter/sort changes
  // ---------------------------------------------------------

  // --- Review Submission Logic (Updated) --- //
  // Now accepts optional feedback text
  const handleReviewSubmit = useCallback(async (podcastId: string, approved: boolean, feedback: string | null) => {
    console.log(`Submitting review for ${podcastId}: Approved=${approved}, Feedback='${feedback || ''}'`);
    // TODO: Add visual indicator that submission is in progress
    try {
      await submitReview(podcastId, approved, feedback); // Pass feedback to API call
      console.log(`Review submission successful for ${podcastId}`);
      alert(`Review submitted successfully for ${podcastId}`);
      // TODO: Optimistic update - remove/update item locally before refetch
      fetchLeads(); // Refresh list
    } catch (err: any) {
      console.error(`Caught error submitting review for ${podcastId}:`, err);
      setError(err.message || `Failed to submit review for ${podcastId}.`);
      alert(`Error submitting review: ${err.message}`);
      // TODO: Remove visual indicator for submission progress
    }
  }, [fetchLeads]); // Dependency: fetchLeads (to refresh)

  // Updated handlers to receive feedback from LeadCard
  const handleApprove = useCallback((podcastId: string, feedback: string | null) => {
    handleReviewSubmit(podcastId, true, feedback);
  }, [handleReviewSubmit]);

  const handleReject = useCallback((podcastId: string, feedback: string | null) => {
    handleReviewSubmit(podcastId, false, feedback);
  }, [handleReviewSubmit]);
  // ------------------------------- //

  // Pagination handlers
  const handleNextPage = () => {
    if (currentPage < totalPages) {
      setCurrentPage(currentPage + 1);
    }
  };

  const handlePreviousPage = () => {
    if (currentPage > 1) {
      setCurrentPage(currentPage - 1);
    }
  };

  // Filter/Sort Change Handlers
  const handleTierChange = (event: React.ChangeEvent<HTMLSelectElement>) => {
    setFilterTier(event.target.value);
  };

  const handleSortByChange = (event: React.ChangeEvent<HTMLSelectElement>) => {
    setSortBy(event.target.value);
    // Optional: Reset sort order when sort field changes, or keep current?
    // setSortOrder('desc');
  };

  const handleSortOrderChange = (event: React.ChangeEvent<HTMLSelectElement>) => {
    setSortOrder(event.target.value);
  };

  return (
    <div style={{ padding: '20px', maxWidth: '900px', margin: 'auto' }}>
      <h1>Podcast Lead Review Dashboard</h1>

      {/* Filter and Sort Controls */}
      <div style={{ marginBottom: '20px', display: 'flex', gap: '15px', alignItems: 'center', flexWrap: 'wrap' }}>
        <div>
          <label htmlFor="filter-tier" style={{ marginRight: '5px' }}>Filter Tier:</label>
          <select id="filter-tier" value={filterTier} onChange={handleTierChange}>
            <option value="all">All Tiers</option>
            <option value="A">Tier A</option>
            <option value="B">Tier B</option>
            <option value="C">Tier C</option>
            {/* Add other tiers if applicable */}
          </select>
        </div>
        {/* TODO: Add Filter by Status Dropdown */}
        <div>
          <label htmlFor="sort-by" style={{ marginRight: '5px' }}>Sort By:</label>
          <select id="sort-by" value={sortBy} onChange={handleSortByChange}>
            <option value="date_added">Date Added</option>
            <option value="score">Vetting Score</option>
            {/* Add other sort options like name */}
          </select>
        </div>
        <div>
          <label htmlFor="sort-order" style={{ marginRight: '5px' }}>Order:</label>
          <select id="sort-order" value={sortOrder} onChange={handleSortOrderChange}>
            <option value="desc">Descending</option>
            <option value="asc">Ascending</option>
          </select>
        </div>
        {/* TODO: Add button to clear filters/sort */}
      </div>

      {isLoading && <p>Loading leads...</p>}

      {error && <p style={{ color: 'red' }}>Error: {error}</p>}

      {!isLoading && !error && (
        <>
          <LeadList
            leads={leads}
            onApprove={handleApprove}
            onReject={handleReject}
          />

          {/* Pagination Controls */}
          <div style={{ marginTop: '20px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <button onClick={handlePreviousPage} disabled={currentPage <= 1}>
              Previous
            </button>
            <span>
              Page {currentPage} of {totalPages} (Total Leads: {totalLeads})
            </span>
            <button onClick={handleNextPage} disabled={currentPage >= totalPages}>
              Next
            </button>
          </div>
        </>
      )}
    </div>
  );
}
