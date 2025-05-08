import React, { useState, useMemo } from 'react';
import { useNavigate } from 'react-router';
import { v4 as uuidv4 } from 'uuid';

// Define interfaces for the expected API response structure
// --- REMOVE OLD/SIMPLIFIED INTERFACES TO AVOID CONFLICTS ---
/*
interface PodcastProfile { // This will be replaced by PodcastLead
  podcast_id: string;
  name: string;
  description: string;
  episode_count?: number;
  url?: string;
  image_url?: string;
  rss_url?: string;
  language?: string;
  contact_details?: string | null;
  listennotes_url?: string;
  podscan_url?: string;
  keywords?: string[];
  host_information?: string | null;
  audience_demographics?: any | null;
  relevance_score?: number | null;
  categories?: any[] | null;
  network?: string | null;
  tags?: any[] | null;
}

interface VettingResult { // This will be replaced by the detailed VettingResult below
  podcast_id: string;
  quality_tier: string; // This is the conflicting part for error 1
  composite_score: number;
  decision?: string;
  reasoning?: string;
  explanation?: string;
  metric_scores?: any; // This is the conflicting part for error 2
  error?: string | null;
}

interface EnrichmentResult { // This will be replaced by EnrichedPodcastProfile
  podcast_id: string;
  enriched_data?: any; 
}
*/
// --- END REMOVAL OF OLD INTERFACES ---

// --- UPDATED INTERFACES (similar to discovery.tsx and backend models) ---
interface PodcastLead {
  source_api?: string;
  api_id?: string;
  title?: string;
  description?: string;
  rss_url?: string;
  website?: string;
  email?: string;
  itunes_id?: number | string;
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
  language?: string;
  author?: string;
  ownerName?: string;
  categories?: Record<string, string> | string[];
  explicit?: boolean;
  // Mapped for compatibility if old names appear
  url?: string; 
  rssUrl?: string; 
  episodeCount?: number; 
  image?: string; 
  [key: string]: any;
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
  twitter_following?: number;
  is_twitter_verified?: boolean;
  linkedin_connections?: number;
  instagram_followers?: number;
  tiktok_followers?: number;
  facebook_likes?: number;
  youtube_subscribers?: number;
  data_sources?: string[];
  last_enriched_timestamp?: string; // Or Date
  social_links?: Record<string, string>;
  keywords?: string[] | string;
  [key: string]: any;
}

// This is the definitive VettingResult interface
interface VettingResult {
  podcast_id: string; 
  programmatic_consistency_passed: boolean;
  programmatic_consistency_reason: string;
  last_episode_date?: string | null;
  days_since_last_episode?: number | null;
  average_frequency_days?: number | null;
  llm_match_score?: number | null;
  llm_match_explanation?: string | null;
  composite_score: number;
  quality_tier: "A" | "B" | "C" | "D" | "Unvetted";
  final_explanation: string;
  metric_scores?: Record<string, number | string>;
  error?: string | null;
  title?: string; 
  image_url?: string; 
  [key: string]: any;
}

// --- REVISED: Structure matching AgentState from the debug output ---
interface AgentState {
  current_step?: string;
  campaign_config: any; // Use a more specific type if known
  leads?: PodcastLead[]; // Updated type
  vetting_results?: VettingResult[]; 
  enriched_profiles?: EnrichedPodcastProfile[]; // Updated type
  messages_history?: any[];
  execution_status?: string;
  neighborhood_results?: any;
  [key: string]: any; // Allow other arbitrary keys
}
// ------------------------------------------------------------------

// --- Define the expected response from /campaigns/run --- //
interface WorkflowResponse {
  message: string;
  final_state?: AgentState; // Contains the final state of the workflow
  error?: string;
}
// --- Removed Old ApiResponse --- //
/*
interface ApiResponse {
  message: string;
  processed_podcasts?: Array<{
    profile: PodcastProfile;
    vetting?: VettingResult | null;
    enrichment?: EnrichmentResult | null;
  }>;
  error?: string;
}
*/

const CampaignRunner: React.FC = () => {
  const navigate = useNavigate();

  // --- Combined Form State --- //
  const [searchType, setSearchType] = useState<'topic' | 'related'>('topic');
  // Topic Search
  const [targetAudience, setTargetAudience] = useState("");
  const [keyMessages, setKeyMessages] = useState("");
  const [numKeywords, setNumKeywords] = useState<number>(10);
  const [maxResultsPerKeyword, setMaxResultsPerKeyword] = useState<number>(50);
  // Related Search
  const [seedRssUrl, setSeedRssUrl] = useState("");
  const [maxDepth, setMaxDepth] = useState<number>(2);
  const [maxTotalResults, setMaxTotalResults] = useState<number>(50);
  // Vetting Criteria
  const [idealPodcastDescription, setIdealPodcastDescription] = useState("");
  const [guestBio, setGuestBio] = useState("");
  const [guestTalkingPoints, setGuestTalkingPoints] = useState("");

  // --- Results State --- //
  const [finalState, setFinalState] = useState<AgentState | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [workflowMessage, setWorkflowMessage] = useState<string | null>(null);

  // --- Memoized Display Data --- //
  const itemsToDisplay = useMemo(() => 
    // If vetting results exist and have content, use them.
    // Vetting results might be an array of VettingResult, which might need to be mapped to include enriched data for display.
    // For now, assume finalState.vetting_results directly contains all necessary display fields or they are merged by backend.
    finalState?.vetting_results && finalState.vetting_results.length > 0 ? finalState.vetting_results :
    finalState?.enriched_profiles && finalState.enriched_profiles.length > 0 ? finalState.enriched_profiles :
    finalState?.leads
  , [finalState]);

  const displayDataType = useMemo(() => 
    finalState?.vetting_results && finalState.vetting_results.length > 0 ? 'vetting' :
    finalState?.enriched_profiles && finalState.enriched_profiles.length > 0 ? 'enrichment' :
    finalState?.leads && finalState.leads.length > 0 ? 'search' :
    null
  , [finalState]);

  // --- Memoized Columns (EXPANDED) --- //
  const columns = useMemo(() => {
    let definedColumns: { Header: string; accessor: string; Cell?: (cell: any) => React.ReactNode }[] = [];
    
    const formatDateCell = (value: any) => {
      if (!value) return 'N/A';
      try {
        const date = new Date(value);
        return isNaN(date.getTime()) ? (typeof value === 'string' ? value : 'Invalid Date') : date.toLocaleDateString();
      } catch {
        return typeof value === 'string' ? value : 'Invalid Date';
      }
    };

    const formatNumberCell = (value: any, toFixed: number = 1) => {
        return typeof value === 'number' ? value.toFixed(toFixed) : (value || 'N/A');
    };
    
    const renderLongTextCell = (value: any) => (
        <div className="max-w-md whitespace-normal break-words">
            {value !== null && value !== undefined ? String(value) : 'N/A'}
        </div>
    );

    const renderImageCell = (value: any, altText: string = "Image") => {
        if (typeof value === 'string' && value) {
            return <img src={value} alt={altText} className="h-10 w-10 object-cover rounded" />;
        }
        return 'N/A';
    };

    const renderArrayCell = (value: any) => {
        if (Array.isArray(value)) {
            return value.join(', ');
        }
        return value !== null && value !== undefined ? String(value) : 'N/A';
    };
     const renderBooleanCell = (value: any, trueText = "Yes", falseText = "No") => {
      if (typeof value === 'boolean') {
        return value ? <span className="text-green-600 font-semibold">{trueText}</span> : <span className="text-red-600 font-semibold">{falseText}</span>;
      }
      return 'N/A';
    };
    const renderSocialLinksCell = (value: any) => {
        if (typeof value === 'object' && value && Object.keys(value).length > 0) {
            return (
              <ul className="list-disc list-inside text-xs">
                {Object.entries(value).map(([platform, url]) => (
                  <li key={platform}><a href={url as string} target="_blank" rel="noopener noreferrer" className="text-blue-500 hover:underline">{platform}</a></li>
                ))}
              </ul>
            );
        }
        return 'N/A';
    };


    if (displayDataType === 'vetting') {
       // Based on VettingResult interface from discovery.tsx
       const vettingHeaders: string[] = [
         'podcast_id', 'title', 'image_url', // Added title and image for context
         'quality_tier', 'composite_score', 
         'programmatic_consistency_passed', 'programmatic_consistency_reason',
         'llm_match_score', 'llm_match_explanation',
         'final_explanation', 'days_since_last_episode', 'average_frequency_days',
         'last_episode_date', 'metric_scores', 'error'
       ];
       // It's common for vetting results to be associated with a podcast,
       // so if finalState.vetting_results items don't have title/image, we might need to merge them
       // with finalState.enriched_profiles based on podcast_id for display.
       // For now, assuming they might be directly available or added to VettingResult structure by backend.
       definedColumns = vettingHeaders.map(header => ({
         Header: header.split('_').map((word: string) => word.charAt(0).toUpperCase() + word.slice(1)).join(' '),
         accessor: header,
         Cell: ({ value, row }: { value: any, row: any }) => {
            // Try to get title from enriched_profiles if not in vetting_results directly (example of merging)
            // This logic would be more complex if needing full merge, but for simplicity:
            if (header === 'title' && !value && finalState?.enriched_profiles) {
                const profile = finalState.enriched_profiles.find(p => p.api_id === row.original.podcast_id);
                value = profile?.title;
            }
            if (header === 'image_url' && !value && finalState?.enriched_profiles) {
                const profile = finalState.enriched_profiles.find(p => p.api_id === row.original.podcast_id);
                value = profile?.image_url;
                return renderImageCell(value, row.original.title || 'Podcast Art');
            }

            if (header === 'programmatic_consistency_passed') return renderBooleanCell(value, "Passed", "Failed");
            if (header === 'composite_score' || header === 'llm_match_score' || header === 'average_frequency_days' || header === 'days_since_last_episode') return formatNumberCell(value);
            if (header === 'last_episode_date') return formatDateCell(value);
            if (['programmatic_consistency_reason', 'llm_match_explanation', 'final_explanation', 'error'].includes(header)) return renderLongTextCell(value);
            if (header === 'metric_scores' && typeof value === 'object') return <pre className="text-xs whitespace-pre-wrap">{JSON.stringify(value, null, 2)}</pre>;
            if (header === 'image_url') return renderImageCell(value, row.original.title || "Podcast Art");
            return value !== null && value !== undefined ? String(value) : 'N/A';
         }
       }));

    } else if (displayDataType === 'enrichment') {
      // Based on EnrichedPodcastProfile interface from discovery.tsx
      const enrichedHeaders = [
        'api_id', 'title', 'description', 'image_url', 'website', 'language',
        'rss_feed_url', 'total_episodes', 'first_episode_date', 'latest_episode_date',
        'average_duration_seconds', 'publishing_frequency_days',
        'host_names', 'rss_owner_name', 'rss_owner_email', 'primary_email',
        'podcast_twitter_url', 'twitter_followers', 'is_twitter_verified', 'twitter_following',
        'podcast_linkedin_url', 'linkedin_connections',
        'podcast_instagram_url', 'instagram_followers',
        'podcast_facebook_url', 'facebook_likes',
        'podcast_youtube_url', 'youtube_subscribers',
        'podcast_tiktok_url', 'tiktok_followers',
        'podcast_other_social_url', 'host_twitter_url', 'host_linkedin_url',
        'listen_score', 'listen_score_global_rank', 'audience_size',
        'itunes_rating_average', 'itunes_rating_count', 'spotify_rating_average', 'spotify_rating_count',
        'data_sources', 'last_enriched_timestamp', 'social_links', 'keywords', 'unified_profile_id'
      ];
      definedColumns = enrichedHeaders.map(header => ({
        Header: header.split('_').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' '),
        accessor: header,
        Cell: ({ value, row }: { value: any, row: any }) => {
          if (header === 'image_url') return renderImageCell(value, row.original.title || "Podcast Art");
          if (header === 'description') return renderLongTextCell(value);
          if (header === 'social_links') return renderSocialLinksCell(value);
          if (['host_names', 'keywords', 'data_sources'].includes(header)) return renderArrayCell(value);
          if (['first_episode_date', 'latest_episode_date', 'last_enriched_timestamp'].includes(header)) return formatDateCell(value);
          if (['is_twitter_verified'].includes(header)) return renderBooleanCell(value);
          if (typeof value === 'number' && ![
              'total_episodes', 'itunes_rating_count', 'spotify_rating_count', 
              'twitter_followers', 'twitter_following', 'linkedin_connections', 
              'instagram_followers', 'tiktok_followers', 'facebook_likes', 'youtube_subscribers',
              'listen_score_global_rank', 'average_duration_seconds' // these can be whole numbers
            ].includes(header)
          ) return formatNumberCell(value);
          if (typeof value === 'object' && value !== null) return <pre className="text-xs whitespace-pre-wrap">{JSON.stringify(value, null, 2)}</pre>;
          return value !== null && value !== undefined ? String(value) : 'N/A';
        }
      }));

    } else if (displayDataType === 'search') { 
      // Based on PodcastLead interface from discovery.tsx
      const searchHeaders = [
        "api_id", "title", "description", "image_url", "rss_url", "website", "email", "language",
        "total_episodes", "latest_pub_date_ms", "earliest_pub_date_ms", "update_frequency_hours",
        "listen_score", "listen_score_global_rank", "audience_size",
        "itunes_id", "itunes_rating_average", "itunes_rating_count",
        "podcast_spotify_id", "spotify_rating_average", "spotify_rating_count",
        "last_posted_at", "author", "ownerName", "categories", "explicit",
        "source_api", "instagram_url", "twitter_url", "linkedin_url", "tiktok_url",
        "youtube_url", "facebook_url", "other_social_url"
      ];
      definedColumns = searchHeaders.map(header => {
        // Handle potential mapping for old field names if the backend data isn't perfectly standardized yet.
        // This is mostly illustrative; the PodcastLead interface is designed to catch common variations.
        let accessor = header;
        // Example: if (header === "website" && dataToRender[0] && !dataToRender[0].hasOwnProperty('website') && dataToRender[0].hasOwnProperty('url')) accessor = 'url';

        return {
          Header: header.split('_').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' '),
          accessor: accessor,
          Cell: ({ value, row }: { value: any, row: any }) => {
            if (header === 'image_url' || (accessor === 'image' && header === 'image_url')) return renderImageCell(value, row.original.title || "Podcast Art");
            if (header === 'description') return renderLongTextCell(value);
            if (header === 'categories') {
                if (typeof value === 'object' && value && !Array.isArray(value)) {
                    return Object.values(value).join(', ');
                }
                return renderArrayCell(value);
            }
            if (header === 'explicit') return renderBooleanCell(value);
            if (['latest_pub_date_ms', 'earliest_pub_date_ms'].includes(header) && typeof value === 'number') {
                return formatDateCell(new Date(value));
            }
            if (header === 'last_posted_at') return formatDateCell(value);
            if (typeof value === 'number' && ![
                'total_episodes', 'update_frequency_hours', 'itunes_rating_count', 'spotify_rating_count'
                ].includes(header)
            ) return formatNumberCell(value);
            return value !== null && value !== undefined ? String(value) : 'N/A';
          }
        }
      });
    }
    return definedColumns;
  }, [displayDataType, finalState]); // Added finalState for contextual data access in cells

  const dataToRender = useMemo(() => itemsToDisplay || [], [itemsToDisplay]);

  // --- Submit Handler --- //
  const handleRunWorkflow = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setIsLoading(true);
    setError(null);
    setFinalState(null);
    setWorkflowMessage(null);

    // --- Construct CampaignConfiguration Payload --- //
    let campaignConfig: any = {
      campaign_id: uuidv4(), // Generate a unique ID for this run
      search_type: searchType,
      // Vetting Criteria (always include)
      ideal_podcast_description: idealPodcastDescription,
      guest_bio: guestBio,
      guest_talking_points: guestTalkingPoints.split('\n').filter(tp => tp.trim() !== ""),
    };

    if (searchType === 'topic') {
      if (!targetAudience.trim()) {
        setError("Target Audience is required for Topic Search.");
        setIsLoading(false);
        return;
      }
      campaignConfig = {
        ...campaignConfig,
        target_audience: targetAudience,
        key_messages: keyMessages.split('\n').filter(km => km.trim() !== ""),
        num_keywords_to_generate: numKeywords,
        max_results_per_keyword: maxResultsPerKeyword,
      };
    } else { // related search
      if (!seedRssUrl.trim() || !seedRssUrl.startsWith('http')) {
        setError("A valid Seed RSS URL is required for Related Search.");
        setIsLoading(false);
        return;
      }
      campaignConfig = {
        ...campaignConfig,
        seed_rss_url: seedRssUrl,
        max_depth: maxDepth,
        max_total_results: maxTotalResults,
      };
    }

    // Validate vetting criteria presence
    if (!campaignConfig.ideal_podcast_description || !campaignConfig.guest_bio || campaignConfig.guest_talking_points.length === 0) {
        setError("Ideal Podcast Description, Guest Bio, and Guest Talking Points are required to run the workflow.");
        setIsLoading(false);
        return;
    }

    try {
      console.log("Sending Campaign Config:", campaignConfig);
      const response = await fetch('/campaigns/run', { // Use relative path
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(campaignConfig), // Send the full config
      });

      const data: WorkflowResponse = await response.json();
      console.log("Received Workflow Response:", data);

      if (!response.ok) {
        if (response.status === 401) {
          localStorage.removeItem('isLoggedInPGL');
          navigate('/login', { replace: true });
          throw new Error("Session invalid. Please log in again.");
        }
        let errorDetail = `Request failed with status ${response.status}`;
        const errorData: any = data;
        if (errorData && errorData.detail) {
            if (Array.isArray(errorData.detail)) {
                errorDetail = errorData.detail.map((err: any) => `${err.loc?.join('.') || 'validation'} - ${err.msg}`).join('; ');
            } else if (typeof errorData.detail === 'string') {
                errorDetail = errorData.detail;
            }
        } else if (data.error) {
            errorDetail = data.error;
        }
        throw new Error(errorDetail);
      }

      setWorkflowMessage(data.message);
      if (data.error) {
        setError(`Workflow Error: ${data.error}`);
      }
      setFinalState(data.final_state || null); // Store the final AgentState

    } catch (err: any) {
      console.error("Error running workflow:", err);
      setError(err.message || 'An unknown error occurred.');
      setFinalState(null);
    } finally {
      setIsLoading(false);
    }
  };

  // --- UPDATED: CSV Download Handler with RFC 4180 Compliance ---
  const handleDownloadCsv = () => {
    if (!dataToRender || dataToRender.length === 0 || !columns || columns.length === 0) {
      setError("No data available to download.");
      return;
    }

    // Helper function to format a cell value for CSV
    const formatCsvCell = (value: any): string => {
        if (value === null || value === undefined) {
            return ''; // Return empty string for null/undefined
        }

        let stringValue: string;

        // Handle arrays and objects specifically
        if (Array.isArray(value)) {
            stringValue = value.join('; '); // Join with semicolon (or choose another delimiter)
        } else if (typeof value === 'object') {
            try {
                stringValue = JSON.stringify(value);
            } catch (e) {
                console.warn("Could not stringify object for CSV:", value, e);
                stringValue = '[Object Error]';
            }
        } else {
            stringValue = String(value); // Convert other types to string
        }

        // Escape double quotes by doubling them
        const escapedValue = stringValue.replace(/"/g, '""');

        // Check if the value needs to be enclosed in double quotes
        if (escapedValue.includes(',') || escapedValue.includes('"') || escapedValue.includes('\n') || escapedValue.includes('\r')) {
            return `"${escapedValue}"`;
        }

        return escapedValue;
    };

    const headers = columns.map(col => formatCsvCell(col.Header)); // Format headers too
    const rows = dataToRender.map(item => {
      return columns.map(col => {
        const cellValue = item[col.accessor as keyof typeof item];
        return formatCsvCell(cellValue); // Use the formatting helper
      }).join(','); // Join cells with comma
    });

    // Manually construct CSV content ensuring proper line endings (CRLF)
    const csvContent = [headers.join(','), ...rows].join('\r\n'); 
    
    // Add BOM for better Excel compatibility with UTF-8
    const blob = new Blob(["\ufeff", csvContent], { type: 'text/csv;charset=utf-8;' });
    
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    link.setAttribute('href', url);
    const campaignId = finalState?.campaign_config?.campaign_id || 'workflow';
    const dataType = displayDataType || 'results';
    link.setAttribute('download', `${campaignId}_${dataType}_${new Date().toISOString().split('T')[0]}.csv`);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  // --- Render Table Function (Simplified) --- //
  const renderTable = () => {
    if (!dataToRender || dataToRender.length === 0) {
      if (!isLoading) { // Only show "No data" if not loading
          return <p className="mt-4 text-gray-600">No data to display. Run a workflow or check for errors.</p>;
      }
      return null; // Don't show anything if loading and no data yet
    }

    return (
      <div className="mt-6 overflow-x-auto">
        <h2 className="text-xl font-semibold mb-3">
          Workflow Results ({displayDataType})
        </h2>
        <div className="shadow border-b border-gray-200 sm:rounded-lg">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                {columns.map((col) => (
                  <th
                    key={col.Header}
                    scope="col"
                    className={`px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider ${
                      col.accessor === 'description' || col.accessor === 'final_explanation' || col.accessor === 'llm_match_explanation' || col.accessor === 'programmatic_consistency_reason' ? 'w-1/3 min-w-[200px]' : // Ensure long text has min width
                      col.accessor === 'title' ? 'w-1/4 min-w-[150px]' : '' // Title also might need more space
                    }`}
                  >
                    {col.Header}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {dataToRender.map((item, index) => (
                <tr key={(item as any).api_id || (item as any).unified_profile_id || (item as any).podcast_id || index} className={index % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                  {columns.map((col) => (
                    <td
                      key={col.Header + ((item as any).api_id || (item as any).unified_profile_id || (item as any).podcast_id || index)} // Make key more unique
                      className={`px-3 py-3 text-sm text-gray-700 align-top ${
                        col.accessor === 'description' || col.accessor === 'final_explanation' || col.accessor === 'llm_match_explanation' || col.accessor === 'programmatic_consistency_reason' ? 'whitespace-normal break-words' : 'whitespace-nowrap'
                      }`}
                    >
                      {/* Pass the whole row data to Cell if it needs more context than just the value */}
                      {col.Cell ? col.Cell({ value: (item as any)[col.accessor as keyof typeof item], row: { original: item } }) : 
                       ((item as any)[col.accessor as keyof typeof item] !== null && (item as any)[col.accessor as keyof typeof item] !== undefined ? String((item as any)[col.accessor as keyof typeof item]) : 'N/A')}
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

  // --- Render Component --- //
  return (
    <div className="container mx-auto p-4 md:p-8 font-sans">
      <header className="mb-8">
        <h1 className="text-3xl font-bold text-gray-800">Run Full Podcast Workflow</h1>
      </header>

      <section className="bg-white shadow-md rounded-lg p-6 mb-8">
        <form onSubmit={handleRunWorkflow} className="space-y-6">
          {/* --- Search Configuration --- */}
          <fieldset className="border p-4 rounded-md">
            <legend className="text-lg font-semibold px-2">1. Search Configuration</legend>
            <div className="flex items-center space-x-6 my-4">
              {/* Radio buttons for search type */}
               <label className="flex items-center space-x-2 cursor-pointer">
                <input
                  type="radio"
                  name="searchType"
                  value="topic"
                  checked={searchType === "topic"}
                  onChange={() => setSearchType('topic')}
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
                  onChange={() => setSearchType('related')}
                  className="form-radio h-4 w-4 text-blue-600 transition duration-150 ease-in-out"
                />
                <span className="text-gray-700">Related Search</span>
              </label>
            </div>

            {/* Conditional Topic Inputs */}
            {searchType === 'topic' && (
              <div className="space-y-4 pl-4 border-l-2 border-gray-200 ml-2">
                 <div>
                   <label htmlFor="targetAudience" className="block text-sm font-medium text-gray-700 mb-1">Target Audience *</label>
                   <input type="text" id="targetAudience" value={targetAudience} onChange={(e) => setTargetAudience(e.target.value)} required className="input-field" placeholder="e.g., Software developers interested in AI" />
                 </div>
                 <div>
                  <label htmlFor="keyMessages" className="block text-sm font-medium text-gray-700 mb-1">Topic Description *</label>
                  <textarea id="keyMessages" value={keyMessages} onChange={(e) => setKeyMessages(e.target.value)} rows={3} required className="input-field" placeholder="Describe the topic to speak on (one per line if multiple points)" />
                 </div>
                 <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label htmlFor="numKeywords" className="block text-sm font-medium text-gray-700 mb-1">Keywords to Gen (1-30)</label>
                    <input type="number" id="numKeywords" value={numKeywords} onChange={(e) => setNumKeywords(parseInt(e.target.value, 10))} min="1" max="30" required className="input-field" />
                  </div>
                  <div>
                    <label htmlFor="maxResultsPerKeyword" className="block text-sm font-medium text-gray-700 mb-1">Results/Keyword (1-200)</label>
                    <input type="number" id="maxResultsPerKeyword" value={maxResultsPerKeyword} onChange={(e) => setMaxResultsPerKeyword(parseInt(e.target.value, 10))} min="1" max="200" required className="input-field" />
                  </div>
                 </div>
              </div>
            )}

            {/* Conditional Related Inputs */}
            {searchType === 'related' && (
               <div className="space-y-4 pl-4 border-l-2 border-gray-200 ml-2">
                <div>
                  <label htmlFor="seedRssUrl" className="block text-sm font-medium text-gray-700 mb-1">Seed RSS URL *</label>
                  <input type="url" id="seedRssUrl" value={seedRssUrl} onChange={(e) => setSeedRssUrl(e.target.value)} required className="input-field" placeholder="https://example.com/podcast/rss" />
                </div>
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label htmlFor="maxDepth" className="block text-sm font-medium text-gray-700 mb-1">Max Depth (1-3)</label>
                    <input type="number" id="maxDepth" value={maxDepth} onChange={(e) => setMaxDepth(parseInt(e.target.value, 10))} min="1" max="3" required className="input-field" />
                  </div>
                  <div>
                    <label htmlFor="maxTotalResults" className="block text-sm font-medium text-gray-700 mb-1">Max Total Results (1-200)</label>
                    <input type="number" id="maxTotalResults" value={maxTotalResults} onChange={(e) => setMaxTotalResults(parseInt(e.target.value, 10))} min="1" max="200" required className="input-field" />
                  </div>
                </div>
              </div>
            )}
          </fieldset>

          {/* --- Vetting Configuration --- */}
          <fieldset className="border p-4 rounded-md">
            <legend className="text-lg font-semibold px-2">2. Vetting Criteria *</legend>
             <div className="space-y-4 mt-2">
                <div>
                    <label htmlFor="idealPodcastDesc" className="block text-sm font-medium text-gray-700 mb-1">Ideal Podcast Description *</label>
                    <textarea id="idealPodcastDesc" value={idealPodcastDescription} onChange={(e) => setIdealPodcastDescription(e.target.value)} rows={4} required className="input-field" placeholder="Describe the perfect podcast characteristics..." />
                </div>
                <div>
                    <label htmlFor="guestBio" className="block text-sm font-medium text-gray-700 mb-1">Guest Bio *</label>
                    <textarea id="guestBio" value={guestBio} onChange={(e) => setGuestBio(e.target.value)} rows={4} required className="input-field" placeholder="Provide the bio or relevant background of the guest..." />
                </div>
                <div>
                    <label htmlFor="guestTalkingPoints" className="block text-sm font-medium text-gray-700 mb-1">Guest Talking Points (One per line) *</label>
                    <textarea id="guestTalkingPoints" value={guestTalkingPoints} onChange={(e) => setGuestTalkingPoints(e.target.value)} rows={5} required className="input-field" placeholder="List key topics, angles, or stories..." />
                </div>
             </div>
          </fieldset>

          {/* --- Submit Button --- */}
          <div className="mt-6">
            <button
              type="submit"
              disabled={isLoading}
              className="w-full md:w-auto py-2 px-6 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:bg-gray-400"
            >
              {isLoading ? "Running Workflow..." : "Run Full Workflow (Search -> Enrich -> Vet)"}
            </button>
          </div>
        </form>
      </section>

      {/* --- Loading / Error / Message Display --- */}
       {isLoading && (
        <div className="mt-6 flex justify-center items-center py-10">
          <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-blue-500"></div>
          <p className="ml-4 text-lg text-gray-700 font-semibold">Running Workflow...</p>
        </div>
      )}
      {error && (
        <div className="my-4 p-4 bg-red-100 border border-red-400 text-red-700 rounded-md shadow-sm">
          <p><span className="font-semibold">Error:</span> {error}</p>
        </div>
      )}
       {workflowMessage && !error && (
        <div className="my-4 p-4 bg-green-100 border border-green-400 text-green-700 rounded-md shadow-sm">
          <p>{workflowMessage}</p>
        </div>
      )}

      {/* --- Results Table --- */}
      {renderTable()}

      {/* --- Raw State Debug --- */}
       {finalState && (
         <details className="mt-8">
           <summary className="cursor-pointer text-sm text-gray-600">View Raw Final State (Debug)</summary>
           <pre className="mt-2 bg-gray-100 p-4 rounded text-xs overflow-auto"><code>{JSON.stringify(finalState, null, 2)}</code></pre>
         </details>
       )}

      {/* --- NEW: Download CSV Button --- */}
      {dataToRender.length > 0 && !isLoading && (
        <button
          onClick={handleDownloadCsv}
          className="ml-4 py-2 px-4 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-green-600 hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-green-500"
        >
          Download Results CSV
        </button>
      )}

    </div>
  );
};

export default CampaignRunner;

// Simple CSS class definition helper for inputs
const InputField: React.FC<React.InputHTMLAttributes<HTMLInputElement> & { label: string; id: string }> = ({ label, id, className, ...props }) => (
  <div>
    <label htmlFor={id} className="block text-sm font-medium text-gray-700 mb-1">{label}</label>
    <input
      id={id}
      className={`mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 sm:text-sm ${className}`}
      {...props}
    />
  </div>
);

const TextAreaField: React.FC<React.TextareaHTMLAttributes<HTMLTextAreaElement> & { label: string; id: string }> = ({ label, id, className, ...props }) => (
  <div>
    <label htmlFor={id} className="block text-sm font-medium text-gray-700 mb-1">{label}</label>
    <textarea
      id={id}
      className={`mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 sm:text-sm ${className}`}
      {...props}
    />
  </div>
);

// Define input-field class style (can be done globally or via CSS modules)
const styles = `
.input-field {
  margin-top: 0.25rem;
  display: block;
  width: 100%;
  padding: 0.5rem 0.75rem;
  border: 1px solid #cbd5e1; /* gray-300 */
  border-radius: 0.375rem; /* rounded-md */
  box-shadow: 0 1px 2px 0 rgba(0, 0, 0, 0.05); /* shadow-sm */
}
.input-field:focus {
  outline: none;
  border-color: #3b82f6; /* blue-500 */
  box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.3); /* ring-blue-500 */
}
`;

// Inject styles (simple way for this example)
if (typeof document !== 'undefined') {
  const styleSheet = document.createElement("style");
  styleSheet.type = "text/css";
  styleSheet.innerText = styles;
  document.head.appendChild(styleSheet);
} 