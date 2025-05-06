import type { MetaFunction } from "@remix-run/node";
import React, { useState, useEffect } from "react";

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


export default function DiscoveryPage() {
  const [searchType, setSearchType] = useState<"topic" | "related">("topic");
  
  // Topic Search Form State
  const [targetAudience, setTargetAudience] = useState("");
  const [keyMessages, setKeyMessages] = useState("");
  const [numKeywords, setNumKeywords] = useState<number>(10);

  // Related Search Form State
  const [seedRssUrl, setSeedRssUrl] = useState("");
  const [maxDepth, setMaxDepth] = useState<number>(2);
  const [maxTotalResults, setMaxTotalResults] = useState<number>(50);

  // Results and Loading/Error State
  const [leads, setLeads] = useState<PodcastLead[]>([]);
  const [enrichedProfiles, setEnrichedProfiles] = useState<EnrichedPodcastProfile[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lastOperation, setLastOperation] = useState<"search" | "enrich" | null>(null);
  const [downloadCsvPath, setDownloadCsvPath] = useState<string | null>(null); // For CSV download link

  const handleSearchTypeChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setSearchType(event.target.value as "topic" | "related");
    setLeads([]); // Clear previous results when changing search type
    setEnrichedProfiles([]);
    setError(null);
    setDownloadCsvPath(null); // Clear CSV path on type change
  };

  const handleSubmitSearch = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setIsLoading(true);
    setError(null);
    setLeads([]);
    setEnrichedProfiles([]);
    setLastOperation("search");
    setDownloadCsvPath(null); // Clear previous CSV path

    let endpoint = "";
    let payload: any = {};

    if (searchType === "topic") {
      endpoint = `/actions/search/topic`;
      payload = {
        target_audience: targetAudience,
        key_messages: keyMessages.split('\n').filter(km => km.trim() !== ""), // Split by newline
        num_keywords_to_generate: numKeywords,
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
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      const data = await response.json();

      if (!response.ok) {
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
      setError(err.message || "An unknown error occurred during search.");
      setLeads([]);
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
    setLastOperation("enrich");
    setDownloadCsvPath(null); // Clear previous CSV path (enrichment will have its own)

    const endpoint = `/actions/enrich`;
    // Assuming the first lead might have a campaign_id if it came from a previous search run
    // For truly standalone, source_campaign_id might be null or a new one generated.
    // The backend /actions/enrich endpoint uses request_data.source_campaign_id
    // For now, let's pass the api_id of the first lead as a placeholder for source_campaign_id
    // This needs to align with how your backend expects to link enrichments if not part of a full campaign.
    // If enrichment is truly standalone from a specific previous search run, source_campaign_id can be omitted.
    const firstLeadApiId = leads[0]?.api_id; 

    const payload = {
      leads: leads, // Send the raw lead dictionaries
      source_campaign_id: firstLeadApiId || null // Or generate a unique ID, or leave null
    };

    try {
      const response = await fetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.detail || data.error || "Enrichment failed");
      }
      
      setEnrichedProfiles(data.enriched_profiles || []);
       if (data.enriched_profiles && data.enriched_profiles.length === 0 && leads.length > 0) {
        setError("Enrichment completed but no profiles were returned. The input leads might not have been enrichable or an issue occurred.");
      } else if (data.enriched_profiles && data.enriched_profiles.length > 0) {
        // Successfully enriched, potentially clear leads or show enrichedProfiles preferentially
      }
      if (data.csv_file_path) {
        console.log("Enrichment CSV available at:", data.csv_file_path);
        setDownloadCsvPath(data.csv_file_path); // Set CSV path for download
      }
    } catch (err: any) {
      setError(err.message || "An unknown error occurred during enrichment.");
      setEnrichedProfiles([]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleClearResults = () => {
    setLeads([]);
    setEnrichedProfiles([]);
    setError(null);
    setLastOperation(null);
    setDownloadCsvPath(null);
    // Optionally reset form fields
    // setTargetAudience("");
    // setKeyMessages("");
    // setSeedRssUrl("");
    console.log("Results cleared.");
  };

  // Updated table to display more columns and handle social links
  const renderTable = () => {
    const itemsToDisplay = lastOperation === 'enrich' && enrichedProfiles.length > 0 ? enrichedProfiles : leads;
    const isEnrichedView = lastOperation === 'enrich' && enrichedProfiles.length > 0;

    if (itemsToDisplay.length === 0 && !isLoading) return <p className="mt-4 text-gray-600">No data to display. Perform a search or clear results.</p>;
    if (itemsToDisplay.length === 0 && isLoading) return null;

    let definedColumns: { Header: string; accessor: string; Cell?: (cell: any) => React.ReactNode }[] = [];

    if (isEnrichedView) {
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
            return <img src={value} alt={header} className="h-10 w-10 object-cover" />;
          } if (header === 'social_links' && typeof value === 'object' && value && Object.keys(value).length > 0) {
            return (
              <ul className="list-disc list-inside text-xs">
                {Object.entries(value).map(([platform, url]) => (
                  <li key={platform}><a href={url as string} target="_blank" rel="noopener noreferrer" className="text-blue-500 hover:underline">{platform}</a></li>
                ))}
              </ul>
            );
          } if (Array.isArray(value)) {
            return value.join(', ');
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
        // Handle mapping for older field names if necessary
        if (header === "website" && !itemsToDisplay[0]?.hasOwnProperty('website') && itemsToDisplay[0]?.hasOwnProperty('url')) accessor = 'url';
        if (header === "rss_url" && !itemsToDisplay[0]?.hasOwnProperty('rss_url') && itemsToDisplay[0]?.hasOwnProperty('rssUrl')) accessor = 'rssUrl';
        if (header === "total_episodes" && !itemsToDisplay[0]?.hasOwnProperty('total_episodes') && itemsToDisplay[0]?.hasOwnProperty('episodeCount')) accessor = 'episodeCount';
        if (header === "image_url" && !itemsToDisplay[0]?.hasOwnProperty('image_url') && itemsToDisplay[0]?.hasOwnProperty('image')) accessor = 'image';
        
        return {
          Header: header.split('_').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' '),
          accessor: accessor,
          Cell: ({ value }: { value: any }) => {
            if ((header === 'image_url' || header === 'image') && typeof value === 'string' && value) {
              return <img src={value} alt={header} className="h-10 w-10 object-cover" />;
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

    return (
      <div className="mt-6 overflow-x-auto">
        <h2 className="text-xl font-semibold mb-3">{isEnrichedView ? "Enriched Profiles" : "Search Results"}</h2>
        <table className="min-w-full divide-y divide-gray-200 shadow">
          <thead className="bg-gray-50">
            <tr>
              {definedColumns.map((col) => (
                <th key={col.Header} scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  {col.Header}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {itemsToDisplay.map((item, index) => (
              <tr key={item.api_id || item.unified_profile_id || item.id || index} className={index % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                {definedColumns.map((col) => (
                  <td key={col.Header + (item.api_id || item.unified_profile_id || item.id || index)} className="px-6 py-4 whitespace-nowrap text-sm text-gray-700">
                    {col.Cell ? col.Cell({ value: (item as any)[col.accessor] }) : 
                     ((item as any)[col.accessor] !== null && (item as any)[col.accessor] !== undefined ? String((item as any)[col.accessor]) : 'N/A')}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  };


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
              Download CSV
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
                <label htmlFor="keyMessages" className="block text-sm font-medium text-gray-700 mb-1">Key Messages (one per line, optional)</label>
                <textarea
                  id="keyMessages"
                  value={keyMessages}
                  onChange={(e) => setKeyMessages(e.target.value)}
                  rows={3}
                  className="mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 sm:text-sm"
                  placeholder="e.g., AI is transforming software development.\nOur tool simplifies AI integration."
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
              className="w-full flex justify-center py-2 px-4 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:bg-gray-400"
            >
              {isLoading && lastOperation === 'search' ? "Searching..." : "Search Podcasts"}
            </button>
          </div>
        </form>
      </section>

      {error && (
        <div className="my-4 p-4 bg-red-100 border border-red-400 text-red-700 rounded-md">
          <p>Error: {error}</p>
        </div>
      )}
      
      {leads.length > 0 && !isLoading && (
         <section className="my-8">
           <button
             onClick={handleEnrichResults}
             disabled={isLoading}
             className="w-full md:w-auto flex justify-center py-2 px-6 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-green-600 hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-green-500 disabled:bg-gray-400"
           >
             {isLoading && lastOperation === 'enrich' ? "Enriching..." : "Enrich Displayed Results"}
           </button>
         </section>
      )}

      {(leads.length > 0 || enrichedProfiles.length > 0) && renderTable()}
      
      {isLoading && (
        <div className="mt-6 flex justify-center items-center">
          <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-blue-500"></div>
          <p className="ml-3 text-gray-700">
            {lastOperation === 'search' ? "Fetching search results..." : (lastOperation === 'enrich' ? "Enriching data..." : "Loading...")}
            </p>
        </div>
      )}
    </div>
  );
} 