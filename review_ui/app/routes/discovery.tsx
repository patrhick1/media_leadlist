import type { MetaFunction } from "@remix-run/node";
import React, { useState, useEffect, useMemo } from "react";
import { useNavigate } from 'react-router';

export const meta: MetaFunction = () => {
  return [
    { title: "Podcast Discovery & Enrichment" },
    { name: "description", content: "Search for podcasts and enrich their data." },
  ];
};

// Define interfaces for our data structures based on backend models
interface PodcastLead {
  source_api?: string;
  api_id?: string;
  title?: string;
  description?: string;
  rss_url?: string;
  website?: string;
  email?: string;
  itunes_id?: number | string; // Can be number or string from APIs
  latest_episode_id?: string;
  latest_pub_date_ms?: number;
  earliest_pub_date_ms?: number;
  total_episodes?: number;
  update_frequency_hours?: number;
  listen_score?: number;
  listen_score_global_rank?: number;
  podcast_spotify_id?: string;
  audience_size?: number;
  itunes_rating_average?: number;
  itunes_rating_count?: number;
  spotify_rating_average?: number;
  spotify_rating_count?: number;
  last_posted_at?: string; // Or Date
  image_url?: string;
  instagram_url?: string;
  twitter_url?: string;
  linkedin_url?: string;
  tiktok_url?: string;
  youtube_url?: string;
  facebook_url?: string;
  other_social_url?: string;
  // Fields from older interface that might still come from some API responses
  // before full standardization to CSV_HEADERS structure in search agent output.
  // For robustness, keep them optional or ensure backend sends them if these are used by accessors like "url" or "rssUrl"
  url?: string; // specific to podcastindex, map to website if possible
  rssUrl?: string; // specific to some, map to rss_url
  language?: string;
  episodeCount?: number; // map to total_episodes
  author?: string;
  ownerName?: string;
  categories?: Record<string, string> | string[]; // Can be object or array
  explicit?: boolean;
  image?: string; // map to image_url
  [key: string]: any; // Allow other properties
}

interface EnrichedPodcastProfile {
  unified_profile_id?: string;
  source_api?: string;
  api_id?: string;
  title?: string;
  description?: string;
  image_url?: string;
  website?: string;
  language?: string;
  rss_feed_url?: string;
  total_episodes?: number;
  first_episode_date?: string; // Or Date
  latest_episode_date?: string; // Or Date
  average_duration_seconds?: number;
  publishing_frequency_days?: number;
  host_names?: string[];
  rss_owner_name?: string;
  rss_owner_email?: string;
  primary_email?: string;
  podcast_twitter_url?: string;
  podcast_linkedin_url?: string;
  podcast_instagram_url?: string;
  podcast_facebook_url?: string;
  podcast_youtube_url?: string;
  podcast_tiktok_url?: string;
  podcast_other_social_url?: string;
  host_twitter_url?: string;
  host_linkedin_url?: string;
  listen_score?: number;
  listen_score_global_rank?: number;
  audience_size?: number;
  itunes_rating_average?: number;
  itunes_rating_count?: number;
  spotify_rating_average?: number;
  spotify_rating_count?: number;
  twitter_followers?: number;
  linkedin_connections?: number;
  data_sources?: string[]; // Assuming it's a list of strings
  last_enriched_timestamp?: string; // Or Date
  social_links?: Record<string, string>; // For the custom renderer
  keywords?: string[] | string; // Can be array or string
  [key: string]: any; // Allow other properties
}

// --- NEW: Interface for VettingResult (mirroring backend) ---
interface VettingResult {
  podcast_id: string;
  programmatic_consistency_passed: boolean;
  programmatic_consistency_reason: string;
  last_episode_date?: string | null; // Assuming ISO string from backend JSON
  days_since_last_episode?: number | null;
  average_frequency_days?: number | null;
  llm_match_score?: number | null;
  llm_match_explanation?: string | null;
  composite_score: number;
  quality_tier: "A" | "B" | "C" | "D" | "Unvetted";
  final_explanation: string;
  metric_scores?: Record<string, number>;
  error?: string | null;
  [key: string]: any; // Allow other properties if needed
}

export default function DiscoveryPage() {
  const [searchType, setSearchType] = useState<"topic" | "related">("topic");
  const navigate = useNavigate();
  
  // Topic Search Form State
  const [targetAudience, setTargetAudience] = useState("");
  const [keyMessages, setKeyMessages] = useState("");
  const [numKeywords, setNumKeywords] = useState<number>(10);
  const [maxResultsPerKeyword, setMaxResultsPerKeyword] = useState<number>(50);

  // Related Search Form State
  const [seedRssUrl, setSeedRssUrl] = useState("");
  const [maxDepth, setMaxDepth] = useState<number>(2);
  const [maxTotalResults, setMaxTotalResults] = useState<number>(50);

  // Results and Loading/Error State
  const [leads, setLeads] = useState<PodcastLead[]>([]);
  const [enrichedProfiles, setEnrichedProfiles] = useState<EnrichedPodcastProfile[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lastOperation, setLastOperation] = useState<"search" | "enrich" | "vet" | null>(null);
  const [downloadCsvPath, setDownloadCsvPath] = useState<string | null>(null);

  // --- NEW State for Vetting --- //
  const [showVettingForm, setShowVettingForm] = useState(false);
  const [idealPodcastDescription, setIdealPodcastDescription] = useState("");
  const [guestBio, setGuestBio] = useState("");
  const [guestTalkingPoints, setGuestTalkingPoints] = useState("");
  const [vettingResults, setVettingResults] = useState<VettingResult[]>([]); // To store vetting results
  // --- END NEW Vetting State --- //

  // --- Moved Memoization Here (Fix for Hook Error) ---
  const itemsToDisplay = useMemo(() => 
    lastOperation === 'vet' && vettingResults.length > 0 ? vettingResults :
    lastOperation === 'enrich' && enrichedProfiles.length > 0 ? enrichedProfiles :
    leads
  , [lastOperation, vettingResults, enrichedProfiles, leads]);

  const isEnrichedView = useMemo(() => lastOperation === 'enrich' && enrichedProfiles.length > 0, [lastOperation, enrichedProfiles]);
  const isVettingView = useMemo(() => lastOperation === 'vet' && vettingResults.length > 0, [lastOperation, vettingResults]);

  const columns = useMemo(() => {
    let definedColumns: { Header: string; accessor: string; Cell?: (cell: any) => React.ReactNode }[] = [];
    if (isVettingView) {
       // Define columns based on VettingResult interface
       // Explicitly type as string array to satisfy linter for accessor
       const vettingHeaders: string[] = [
         'podcast_id', 'quality_tier', 'composite_score', 
         'programmatic_consistency_passed', 'programmatic_consistency_reason',
         'llm_match_score', 'llm_match_explanation',
         'final_explanation', 'days_since_last_episode', 'average_frequency_days',
         'last_episode_date', 'error' 
         // Add 'metric_scores' if needed, requires JSON stringify cell
       ];
       definedColumns = vettingHeaders.map(header => ({
         // Explicitly type word as string in map callback
         Header: header.split('_').map((word: string) => word.charAt(0).toUpperCase() + word.slice(1)).join(' '),
         accessor: header,
         Cell: ({ value }: { value: any }) => {
            if (header === 'programmatic_consistency_passed') {
              return value ? <span className="text-green-600 font-semibold">Passed</span> : <span className="text-red-600 font-semibold">Failed</span>;
            }
            if (header === 'composite_score' || header === 'llm_match_score' || header === 'average_frequency_days') {
               return typeof value === 'number' ? value.toFixed(1) : 'N/A';
            }
            if (header === 'last_episode_date' && typeof value === 'string') {
                try { return new Date(value).toLocaleDateString(); } catch { return 'Invalid Date'; }
            }
            // Handle long text fields with wrapping
            // Use header directly here as it's guaranteed to be string now
            if (['programmatic_consistency_reason', 'llm_match_explanation', 'final_explanation'].includes(header)) {
              return (
                <div className="max-w-md whitespace-normal break-words">
                  {value !== null && value !== undefined ? String(value) : 'N/A'}
                </div>
              );
            }
           return value !== null && value !== undefined ? String(value) : 'N/A';
         }
       }));
    } else if (isEnrichedView) {
      // Use ENRICHED_CSV_HEADERS as a guide
      // Ensure EnrichedPodcastProfile interface matches these fields
      const enrichedHeaders = [
        'unified_profile_id', 'source_api', 'api_id',
        'title', 'description', 'image_url', 'website', 'language',
        'rss_feed_url', 'total_episodes', 'first_episode_date', 'latest_episode_date',
        'average_duration_seconds', 'publishing_frequency_days',
        'host_names', 'rss_owner_name', 'rss_owner_email', 'primary_email',
        'podcast_twitter_url', 'podcast_linkedin_url', 'podcast_instagram_url',
        'podcast_facebook_url', 'podcast_youtube_url', 'podcast_tiktok_url',
        'podcast_other_social_url', 'host_twitter_url', 'host_linkedin_url',
        'listen_score', 'listen_score_global_rank', 'audience_size',
        'itunes_rating_average', 'itunes_rating_count', 'spotify_rating_average',
        'spotify_rating_count', 'twitter_followers', 'linkedin_connections',
        'data_sources', 'last_enriched_timestamp', 'social_links', 'keywords'
      ];
      definedColumns = enrichedHeaders.map(header => ({
        Header: header.split('_').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' '), // Auto-capitalize header
        accessor: header,
        Cell: ({ value }: { value: any }) => {
          if (header === 'image_url' && typeof value === 'string' && value) {
            return <img src={value} alt={header} className="h-10 w-10 object-cover rounded" />;
          } if (header === 'description') { // Specific handling for description
            return (
              <div className="max-w-md whitespace-normal break-words">
                {value !== null && value !== undefined ? String(value) : 'N/A'}
              </div>
            );
          } if (header === 'social_links' && typeof value === 'object' && value && Object.keys(value).length > 0) {
            return (
              <ul className="list-disc list-inside text-xs">
                {Object.entries(value).map(([platform, url]) => (
                  <li key={platform}><a href={url as string} target="_blank" rel="noopener noreferrer" className="text-blue-500 hover:underline">{platform}</a></li>
                ))}
              </ul>
            );
          } if (header === 'host_names' && Array.isArray(value)) {
            return value.join(', ');
          } if (header === 'keywords' && Array.isArray(value)) {
            return value.join(', ');
          } if (header === 'data_sources' && Array.isArray(value)) {
            return (
                 <ul className="list-disc list-inside text-xs">
                    {value.map((source, idx) => <li key={idx}>{source}</li>)}
                 </ul>
             );
          } if (typeof value === 'object' && value !== null) {
            return JSON.stringify(value); // For other objects, show JSON string
          }
          return value !== null && value !== undefined ? String(value) : 'N/A';
        }
      }));
    } else { // Search Leads View - map to CSV_HEADERS
      const searchHeaders = [
        "source_api", "api_id", "title", "description", "rss_url", "website", "email",
        "itunes_id", "latest_episode_id", "latest_pub_date_ms", "earliest_pub_date_ms",
        "total_episodes", "update_frequency_hours", "listen_score", "listen_score_global_rank",
        "podcast_spotify_id", "audience_size", "itunes_rating_average", "itunes_rating_count",
        "spotify_rating_average", "spotify_rating_count", "last_posted_at", "image_url",
        "instagram_url", "twitter_url", "linkedin_url", "tiktok_url",
        "youtube_url", "facebook_url", "other_social_url",
        // Keep old accessors for compatibility if backend doesn't perfectly match CSV_HEADERS yet for search
        "language", "author", "ownerName", "categories", "explicit", "image" 
      ];
      definedColumns = searchHeaders.map(header => {
        let accessor = header;
        // Handle mapping for older field names if necessary - check the first item to see if props exist
        const firstItem = itemsToDisplay.length > 0 ? itemsToDisplay[0] : {};
        if (header === "website" && !firstItem?.hasOwnProperty('website') && firstItem?.hasOwnProperty('url')) accessor = 'url';
        if (header === "rss_url" && !firstItem?.hasOwnProperty('rss_url') && firstItem?.hasOwnProperty('rssUrl')) accessor = 'rssUrl';
        if (header === "total_episodes" && !firstItem?.hasOwnProperty('total_episodes') && firstItem?.hasOwnProperty('episodeCount')) accessor = 'episodeCount';
        if (header === "image_url" && !firstItem?.hasOwnProperty('image_url') && firstItem?.hasOwnProperty('image')) accessor = 'image';
        
        return {
          Header: header.split('_').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' '),
          accessor: accessor,
          Cell: ({ value }: { value: any }) => {
            if ((header === 'image_url' || header === 'image') && typeof value === 'string' && value) {
              return <img src={value} alt={header} className="h-10 w-10 object-cover rounded" />;
            }
            if (header === 'description') { // Specific handling for description
              return (
                <div className="max-w-md whitespace-normal break-words">
                  {value !== null && value !== undefined ? String(value) : 'N/A'}
                </div>
              );
            }
            if (header === 'categories' && typeof value === 'object' && value && !Array.isArray(value)) {
              return Object.entries(value).map(([id, name]) => name).join(', ');
            }
            if (Array.isArray(value)) {
              return value.join(', ');
            }
            return value !== null && value !== undefined ? String(value) : 'N/A';
          }
        }
      });
    }
    return definedColumns;
  }, [isVettingView, isEnrichedView, itemsToDisplay]); // Added itemsToDisplay dependency

  const data = useMemo(() => itemsToDisplay, [itemsToDisplay]);
  // --- End Moved Memoization ---

  const handleSearchTypeChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setSearchType(event.target.value as "topic" | "related");
    setLeads([]);
    setEnrichedProfiles([]);
    setVettingResults([]); // Clear vetting results
    setError(null);
    setDownloadCsvPath(null);
    setShowVettingForm(false); // Hide vetting form
    setLastOperation(null);
  };

  const handleSubmitSearch = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setIsLoading(true);
    setError(null);
    setLeads([]);
    setEnrichedProfiles([]);
    setVettingResults([]); // Clear vetting results
    setLastOperation("search");
    setDownloadCsvPath(null);
    setShowVettingForm(false); // Hide vetting form

    let endpoint = "";
    let payload: any = {};

    if (searchType === "topic") {
      endpoint = `/actions/search/topic`;
      payload = {
        target_audience: targetAudience,
        key_messages: keyMessages.split('\n').filter(km => km.trim() !== ""), // Split by newline
        num_keywords_to_generate: numKeywords,
        max_results_per_keyword: maxResultsPerKeyword,
      };
    } else { // related search
      endpoint = `/actions/search/related`;
      payload = {
        seed_rss_url: seedRssUrl,
        max_depth: maxDepth,
        max_total_results: maxTotalResults,
      };
    }

    try {
      const response = await fetch(endpoint, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      const data = await response.json();

      if (!response.ok) {
        if (response.status === 401) {
          localStorage.removeItem('isLoggedInPGL');
          navigate('/login', { replace: true });
        }
        throw new Error(data.detail || data.error || "Search failed");
      }
      
      setLeads(data.leads || []);
      if (data.leads && data.leads.length === 0) {
        setError("Search completed but found no leads.");
      }
      if (data.csv_file_path) {
        console.log("Search CSV available at:", data.csv_file_path);
        setDownloadCsvPath(data.csv_file_path); // Set CSV path for download
      }

    } catch (err: any) {
      if (err.message.includes("Session invalid")) {
        localStorage.removeItem('isLoggedInPGL');
        navigate('/login', { replace: true });
      } else {
        setError(err.message || "An unknown error occurred during search.");
        setLeads([]);
      }
    } finally {
      setIsLoading(false);
    }
  };

  const handleEnrichResults = async () => {
    if (leads.length === 0) {
      setError("No leads to enrich. Please perform a search first.");
      return;
    }
    setIsLoading(true);
    setError(null);
    setEnrichedProfiles([]); // Clear previous enrichment results
    setVettingResults([]); // Clear vetting results
    setLastOperation("enrich");
    setDownloadCsvPath(null);
    setShowVettingForm(false); // Hide vetting form initially

    const endpoint = `/actions/enrich`;
    // Link enrichment using the *first* source lead's api_id for potential tracking
    // Or generate a unique ID if that makes more sense for your tracking.
    const sourceCampaignId = leads.length > 0 ? leads[0]?.api_id || `enrich_${Date.now()}` : `enrich_${Date.now()}`;

    const payload = {
      leads: leads, 
      source_campaign_id: sourceCampaignId
    };

    try {
      const response = await fetch(endpoint, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const data = await response.json();

      if (!response.ok) {
        if (response.status === 401) {
          localStorage.removeItem('isLoggedInPGL');
          navigate('/login', { replace: true });
        }
        throw new Error(data.detail || data.error || "Enrichment failed");
      }
      
      setEnrichedProfiles(data.enriched_profiles || []);
       if (data.enriched_profiles && data.enriched_profiles.length === 0 && leads.length > 0) {
        setError("Enrichment completed but no profiles were returned. Check logs or try again.");
      } else if (data.enriched_profiles && data.enriched_profiles.length > 0) {
        // Success: Show vetting form trigger
        setShowVettingForm(false); // Keep form hidden until button click
      }
      if (data.csv_file_path) {
        console.log("Enrichment CSV available at:", data.csv_file_path);
        setDownloadCsvPath(data.csv_file_path); // Set CSV path for download
      }
    } catch (err: any) {
      if (err.message.includes("Session invalid")) {
        localStorage.removeItem('isLoggedInPGL');
        navigate('/login', { replace: true });
      } else {
        setError(err.message || "An unknown error occurred during enrichment.");
        setEnrichedProfiles([]);
      }
    } finally {
      setIsLoading(false);
    }
  };

  // --- NEW: Function to handle Vetting Submission --- //
  const handleVetSubmit = async (event?: React.FormEvent<HTMLFormElement>) => {
    if (event) event.preventDefault(); // Prevent default form submission if used
    if (enrichedProfiles.length === 0) {
      setError("No enriched profiles available to vet.");
      return;
    }
    setIsLoading(true);
    setError(null);
    setVettingResults([]); // Clear previous vetting results
    setLastOperation("vet");
    setDownloadCsvPath(null); // Clear previous CSV path

    const endpoint = `/actions/vet`;
    // Attempt to link to the source enrichment/search run using the first profile's ID if available
    const sourceCampaignId = enrichedProfiles.length > 0 ? enrichedProfiles[0]?.api_id || `vet_${Date.now()}` : `vet_${Date.now()}`;

    const payload = {
      enriched_profiles: enrichedProfiles,
      ideal_podcast_description: idealPodcastDescription,
      guest_bio: guestBio,
      guest_talking_points: guestTalkingPoints.split('\n').filter(tp => tp.trim() !== ""), // Split by newline
      source_campaign_id: sourceCampaignId
    };

    try {
      const response = await fetch(endpoint, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const data = await response.json();

      if (!response.ok) {
        if (response.status === 401) {
          localStorage.removeItem('isLoggedInPGL');
          navigate('/login', { replace: true });
        }
        throw new Error(data.detail || data.error || "Vetting failed");
      }
      
      setVettingResults(data.vetting_results || []);
      if (data.vetting_results && data.vetting_results.length === 0 && enrichedProfiles.length > 0) {
        setError("Vetting completed but no results were returned. Check logs or try again.");
      }
      if (data.csv_file_path) {
        console.log("Vetting CSV available at:", data.csv_file_path);
        setDownloadCsvPath(data.csv_file_path); // Set CSV path for download
      }
      setShowVettingForm(false); // Hide form after successful submission
    } catch (err: any) {
       if (err.message.includes("Session invalid")) {
        localStorage.removeItem('isLoggedInPGL');
        navigate('/login', { replace: true });
      } else {
        setError(err.message || "An unknown error occurred during vetting.");
        setVettingResults([]);
      }
    } finally {
      setIsLoading(false);
    }
  };
  // --- END NEW Vetting Submit --- //

  const handleClearResults = () => {
    setLeads([]);
    setEnrichedProfiles([]);
    setVettingResults([]); // Clear vetting results
    setError(null);
    setLastOperation(null);
    setDownloadCsvPath(null);
    setShowVettingForm(false); // Hide vetting form
    // Optionally reset form fields
    setTargetAudience("");
    setKeyMessages("");
    setSeedRssUrl("");
    setIdealPodcastDescription("");
    setGuestBio("");
    setGuestTalkingPoints("");
    console.log("Results and forms cleared.");
  };

  // Updated table to display more columns and handle social links
  const renderTable = () => {
    if (data.length === 0 && !isLoading) return <p className="mt-4 text-gray-600">No data to display. Perform a search or clear results.</p>;
    if (data.length === 0 && isLoading) return null;

    return (
      <div className="mt-6 overflow-x-auto">
        <h2 className="text-xl font-semibold mb-3">
          {isVettingView ? "Vetting Results" : isEnrichedView ? "Enriched Profiles" : "Search Results"}
        </h2>
        <div className="shadow border-b border-gray-200 sm:rounded-lg">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                {columns.map((col) => (
                  <th
                    key={col.Header}
                    scope="col"
                    className={`px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider ${ // Reduced padding
                      col.accessor === 'description' || col.accessor === 'final_explanation' || col.accessor === 'llm_match_explanation' ? 'w-1/3' : 
                      col.accessor === 'title' ? 'w-1/6' : ''
                    }`}
                  >
                    {col.Header}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {data.map((item, index) => (
                <tr key={(item as any).api_id || (item as any).unified_profile_id || (item as any).podcast_id || index} className={index % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                  {columns.map((col) => (
                    <td
                      key={col.Header + ((item as any).api_id || (item as any).unified_profile_id || (item as any).podcast_id || index)}
                      className={`px-4 py-3 whitespace-nowrap text-sm text-gray-700 align-top ${ // Reduced padding, align top
                        col.accessor === 'description' || col.accessor === 'final_explanation' || col.accessor === 'llm_match_explanation' ? 'whitespace-normal break-words w-1/3' : 
                        col.accessor === 'title' ? 'w-1/6' : ''
                      }`}
                    >
                      {col.Cell ? col.Cell({ value: (item as any)[col.accessor] }) : 
                       ((item as any)[col.accessor] !== null && (item as any)[col.accessor] !== undefined ? String((item as any)[col.accessor]) : 'N/A')}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  // --- NEW: Explicit boolean flags for loading states to potentially help linter ---
  const isSearching = isLoading && lastOperation === 'search';
  const isEnriching = isLoading && lastOperation === 'enrich';
  const isVetting = isLoading && lastOperation === 'vet';

  return (
    <div className="container mx-auto p-4 md:p-8 font-sans">
      <header className="mb-8">
        <h1 className="text-3xl font-bold text-gray-800">Podcast Discovery & Enrichment</h1>
        <div className="mt-2 flex space-x-2">
          <button
            onClick={handleClearResults}
            className="py-2 px-4 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500"
          >
            Clear Results / New Search
          </button>
          {downloadCsvPath && (
            <a
              href={downloadCsvPath}
              download // Suggests download to browser
              target="_blank" // Opens in new tab, good for direct downloads
              rel="noopener noreferrer"
              className="py-2 px-4 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-green-600 hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-green-500"
            >
              Download {lastOperation?.toUpperCase() || ''} CSV
            </a>
          )}
        </div>
      </header>

      <section className="bg-white shadow-md rounded-lg p-6 mb-8">
        <h2 className="text-xl font-semibold text-gray-700 mb-4">Search Type</h2>
        <div className="flex items-center space-x-6 mb-6">
          <label className="flex items-center space-x-2 cursor-pointer">
            <input
              type="radio"
              name="searchType"
              value="topic"
              checked={searchType === "topic"}
              onChange={handleSearchTypeChange}
              className="form-radio h-4 w-4 text-blue-600 transition duration-150 ease-in-out"
            />
            <span className="text-gray-700">Topic Search</span>
          </label>
          <label className="flex items-center space-x-2 cursor-pointer">
            <input
              type="radio"
              name="searchType"
              value="related"
              checked={searchType === "related"}
              onChange={handleSearchTypeChange}
              className="form-radio h-4 w-4 text-blue-600 transition duration-150 ease-in-out"
            />
            <span className="text-gray-700">Related Search</span>
          </label>
        </div>

        <form onSubmit={handleSubmitSearch}>
          {searchType === "topic" && (
            <div className="space-y-4">
              <div>
                <label htmlFor="targetAudience" className="block text-sm font-medium text-gray-700 mb-1">Target Audience</label>
                <input
                  type="text"
                  id="targetAudience"
                  value={targetAudience}
                  onChange={(e) => setTargetAudience(e.target.value)}
                  required
                  className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                  placeholder="e.g., Software developers interested in AI"
                />
              </div>
              <div>
                <label htmlFor="keyMessages" className="block text-sm font-medium text-gray-700 mb-1">Describe the topic you would like to speak on</label>
                <textarea
                  id="keyMessages"
                  value={keyMessages}
                  onChange={(e) => setKeyMessages(e.target.value)}
                  rows={3}
                  required
                  className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                  placeholder="e.g., The impact of generative AI on modern web development and user experience."
                />
              </div>
              <div>
                <label htmlFor="numKeywords" className="block text-sm font-medium text-gray-700 mb-1">Number of Keywords to Generate (1-30)</label>
                <input
                  type="number"
                  id="numKeywords"
                  value={numKeywords}
                  onChange={(e) => setNumKeywords(parseInt(e.target.value, 10))}
                  min="1"
                  max="30"
                  required
                  className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                />
              </div>
              <div>
                <label htmlFor="maxResultsPerKeyword" className="block text-sm font-medium text-gray-700 mb-1">Max Results Per Keyword (1-200)</label>
                <input
                  type="number"
                  id="maxResultsPerKeyword"
                  value={maxResultsPerKeyword}
                  onChange={(e) => setMaxResultsPerKeyword(parseInt(e.target.value, 10))}
                  min="1"
                  max="200"
                  required
                  className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                />
              </div>
            </div>
          )}

          {searchType === "related" && (
            <div className="space-y-4">
              <div>
                <label htmlFor="seedRssUrl" className="block text-sm font-medium text-gray-700 mb-1">Seed RSS URL</label>
                <input
                  type="url"
                  id="seedRssUrl"
                  value={seedRssUrl}
                  onChange={(e) => setSeedRssUrl(e.target.value)}
                  required
                  className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                  placeholder="https://example.com/podcast/rss"
                />
              </div>
              <div>
                <label htmlFor="maxDepth" className="block text-sm font-medium text-gray-700 mb-1">Max Depth (1-3)</label>
                <input
                  type="number"
                  id="maxDepth"
                  value={maxDepth}
                  onChange={(e) => setMaxDepth(parseInt(e.target.value, 10))}
                  min="1"
                  max="3"
                  required
                  className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                />
              </div>
              <div>
                <label htmlFor="maxTotalResults" className="block text-sm font-medium text-gray-700 mb-1">Max Total Results (1-200)</label>
                <input
                  type="number"
                  id="maxTotalResults"
                  value={maxTotalResults}
                  onChange={(e) => setMaxTotalResults(parseInt(e.target.value, 10))}
                  min="1"
                  max="200"
                  required
                  className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                />
              </div>
            </div>
          )}
          <div className="mt-6">
            <button
              type="submit"
              disabled={isLoading}
              className="w-full md:w-auto py-2 px-6 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:bg-gray-400"
            >
              {isSearching ? "Searching..." : "Search Podcasts"}
            </button>
          </div>
        </form>
      </section>

      {error && (
        <div className="my-4 p-4 bg-red-100 border border-red-400 text-red-700 rounded-md shadow-sm">
          <p><span className="font-semibold">Error:</span> {error}</p>
        </div>
      )}
      
      {/* --- Enrichment Button --- */} 
      {/* Show if leads exist, not loading, and last op was search or null (cleared) */}
      {leads.length > 0 && !isLoading && (lastOperation === 'search' || lastOperation === null) && (
         <section className="my-6">
           <button
             onClick={handleEnrichResults}
             disabled={isLoading}
             className="w-full md:w-auto py-2 px-6 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-teal-600 hover:bg-teal-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-teal-500 disabled:bg-gray-400"
           >
             {isEnriching ? "Enriching..." : "Enrich Displayed Search Results"}
           </button>
         </section>
      )}
      
      {/* --- Vetting Trigger Button & Form --- */} 
      {/* Show if enriched profiles exist, not loading, and last op was enrich */}
      {enrichedProfiles.length > 0 && !isLoading && lastOperation === 'enrich' && (
          <section className="my-6 p-6 bg-gray-50 shadow rounded-lg border border-gray-200">
              <h2 className="text-xl font-semibold text-gray-700 mb-4">2. Vet Enriched Podcasts</h2>
              {!showVettingForm && (
                  <button
                      onClick={() => setShowVettingForm(true)}
                      className="w-full md:w-auto py-2 px-6 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-purple-600 hover:bg-purple-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-purple-500"
                  >
                      Prepare Vetting Criteria
                  </button>
              )}

              {showVettingForm && (
                  <form onSubmit={handleVetSubmit} className="space-y-4">
                       <div>
                           <label htmlFor="idealPodcastDesc" className="block text-sm font-medium text-gray-700 mb-1">Ideal Podcast Description</label>
                           <textarea
                               id="idealPodcastDesc"
                               value={idealPodcastDescription}
                               onChange={(e) => setIdealPodcastDescription(e.target.value)}
                               rows={4}
                               required
                               className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                               placeholder="Describe the perfect podcast characteristics for this outreach (e.g., audience demographics, common topics, interview style)."
                           />
                       </div>
                       <div>
                           <label htmlFor="guestBio" className="block text-sm font-medium text-gray-700 mb-1">Guest Bio</label>
                           <textarea
                               id="guestBio"
                               value={guestBio}
                               onChange={(e) => setGuestBio(e.target.value)}
                               rows={4}
                               required
                               className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                               placeholder="Provide the bio or relevant background of the person you want to pitch as a guest."
                           />
                       </div>
                       <div>
                           <label htmlFor="guestTalkingPoints" className="block text-sm font-medium text-gray-700 mb-1">Guest Talking Points (One per line)</label>
                           <textarea
                               id="guestTalkingPoints"
                               value={guestTalkingPoints}
                               onChange={(e) => setGuestTalkingPoints(e.target.value)}
                               rows={5}
                               required
                               className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                               placeholder="List the key topics, angles, or stories the guest wants to discuss."
                           />
                       </div>
                       <div className="flex space-x-3">
                          <button
                              type="submit"
                              disabled={isLoading}
                              className="py-2 px-6 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-green-600 hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-green-500 disabled:bg-gray-400"
                          >
                              {isVetting ? "Vetting..." : "Vet Podcasts Now"}
                          </button>
                          <button
                              type="button"
                              onClick={() => setShowVettingForm(false)}
                              disabled={isLoading}
                              className="py-2 px-4 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 disabled:opacity-50"
                          >
                              Cancel
                          </button>
                       </div>
                  </form>
              )}
          </section>
      )}

      {(leads.length > 0 || enrichedProfiles.length > 0 || vettingResults.length > 0) && renderTable()}
      
      {isLoading && (
        <div className="mt-6 flex justify-center items-center py-10">
          <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-blue-500"></div>
          <p className="ml-4 text-lg text-gray-700 font-semibold">
            {isSearching ? "Fetching search results..." : 
             isEnriching ? "Enriching data..." : 
             isVetting ? "Vetting podcasts with LLM..." : 
             "Loading..."}
            </p>
        </div>
      )}
    </div>
  );
} 