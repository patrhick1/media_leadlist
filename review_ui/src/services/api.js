import axios from 'axios';

// Base URL for the FastAPI backend
// TODO: Make this configurable via environment variables
const API_BASE_URL = 'http://localhost:8000';

const apiClient = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

/**
 * Fetches a paginated list of leads from the backend.
 * @param {number} [page=1] - The page number to fetch.
 * @param {number} [pageSize=10] - The number of leads per page.
 * @param {string} [filterTier='all'] - The quality tier to filter by ('all', 'A', 'B', 'C').
 * @param {string} [sortBy='date_added'] - The field to sort by.
 * @param {string} [sortOrder='desc'] - The sort order ('asc' or 'desc').
 * @returns {Promise<object>} - A promise that resolves to the API response data.
 */
export const getLeads = async (
  page = 1,
  pageSize = 10,
  filterTier = 'all',
  sortBy = 'date_added',
  sortOrder = 'desc'
) => {
  try {
    // Construct the params object
    const params = {
      page: page,
      page_size: pageSize,
      sort_by: sortBy,
      sort_order: sortOrder,
    };

    // Conditionally add filters
    if (filterTier && filterTier.toLowerCase() !== 'all') {
      params.filter_tier = filterTier;
    }
    // TODO: Add other filters here

    console.log('Fetching leads with params:', params);
    const response = await apiClient.get('/leads', { params });
    console.log('API Response:', response.data);
    return response.data;
  } catch (error) {
    console.error('Error fetching leads:', error);
    if (error.response) {
      console.error('Error data:', error.response.data);
      console.error('Error status:', error.response.status);
      throw new Error(`Failed to fetch leads: ${error.response.data.detail || error.response.statusText}`);
    } else if (error.request) {
      console.error('Error request:', error.request);
      throw new Error('Failed to fetch leads: No response from server.');
    } else {
      console.error('Error message:', error.message);
      throw new Error(`Failed to fetch leads: ${error.message}`);
    }
  }
};

/**
 * Submits a review decision for a specific lead.
 * @param {string} podcastId - The ID of the podcast lead being reviewed.
 * @param {boolean} approved - Whether the lead is approved.
 * @param {string|null} [feedback] - Optional feedback text.
 * @returns {Promise<void>} - A promise that resolves when the request is complete.
 */
export const submitReview = async (podcastId, approved, feedback = null) => {
  try {
    const payload = { approved, feedback };
    await apiClient.post(`/leads/${podcastId}/review`, payload);
    console.log(`Review submitted for ${podcastId}: Approved=${approved}`);
  } catch (error) {
    console.error(`Error submitting review for ${podcastId}:`, error);
    if (error.response) {
      console.error('Error data:', error.response.data);
      console.error('Error status:', error.response.status);
      throw new Error(`Failed to submit review: ${error.response.data.detail || error.response.statusText}`);
    } else if (error.request) {
      console.error('Error request:', error.request);
      throw new Error('Failed to submit review: No response from server.');
    } else {
      console.error('Error message:', error.message);
      throw new Error(`Failed to submit review: ${error.message}`);
    }
  }
};

// Add other API functions as needed (e.g., filtering, sorting) 