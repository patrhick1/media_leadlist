import React, { useState } from 'react';

// Define interfaces for the expected API response structure
interface PodcastProfile {
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

interface VettingResult {
  podcast_id: string;
  quality_tier: string;
  composite_score: number;
  decision?: string;
  reasoning?: string;
  explanation?: string;
  metric_scores?: any;
  error?: string | null;
}

interface EnrichmentResult {
  podcast_id: string;
  enriched_data?: any; // Define more strictly based on actual enrichment data
}

// --- REVISED: Structure matching AgentState from the debug output ---
interface AgentState {
  current_step?: string;
  campaign_config: any; // Use a more specific type if known
  leads?: PodcastProfile[]; // Use the 'leads' key and PodcastProfile type
  vetting_results?: VettingResult[]; // Add the separate vetting_results array
  enriched_profiles?: any[]; // Add other seen keys if needed for display/debug
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
  // --- NEW: State for Search Type ---
  const [searchType, setSearchType] = useState<'topic' | 'related'>('topic');
  const [seedRssUrl, setSeedRssUrl] = useState<string>('');
  // ---------------------------------

  const [query, setQuery] = useState<string>(''); // Kept for potential future use, but not submitted currently
  const [numPodcasts, setNumPodcasts] = useState<number>(5); // Default to 5, also not submitted currently

  // --- State Variables for CampaignConfiguration (Topic Search) ---
  const [targetAudience, setTargetAudience] = useState<string>('');
  const [keyMessages, setKeyMessages] = useState<string>(''); // Input as string, parse later
  const [tonePreferences, setTonePreferences] = useState<string>('');
  const [additionalContext, setAdditionalContext] = useState<string>('');
  // ----------------------------------------------------

  const [isLoading, setIsLoading] = useState<boolean>(false);
  // --- Use the new WorkflowResponse type for results state --- //
  const [results, setResults] = useState<WorkflowResponse | null>(null);
  // ---------------------------------------------------------- //
  const [error, setError] = useState<string>('');

  const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setIsLoading(true);
    setResults(null);
    setError('');

    let campaignPayload: any = {}; // Initialize empty payload

    // --- Construct Payload Based on Search Type ---
    if (searchType === 'topic') {
      const messagesArray = keyMessages.split('\\n').map(msg => msg.trim()).filter(msg => msg.length > 0);
      if (messagesArray.length === 0) {
          setError("Please provide at least one key message for Topic Search.");
          setIsLoading(false);
          return;
      }
      if (!targetAudience.trim()) {
          setError("Please provide a Target Audience for Topic Search.");
          setIsLoading(false);
          return;
      }
      campaignPayload = {
          search_type: 'topic', // Explicitly set search type
          target_audience: targetAudience,
          key_messages: messagesArray,
          tone_preferences: tonePreferences,
          // Include additional_context only if it's not empty
          ...(additionalContext.trim() && { additional_context: additionalContext.trim() })
      };
    } else if (searchType === 'related') {
      if (!seedRssUrl.trim() || !seedRssUrl.startsWith('http')) {
          setError("Please provide a valid Seed RSS URL for Related Search.");
          setIsLoading(false);
          return;
      }
      campaignPayload = {
          search_type: 'related', // Explicitly set search type
          seed_rss_url: seedRssUrl.trim()
          // Other fields are not needed for related search
      };
    } else {
        setError("Invalid search type selected.");
        setIsLoading(false);
        return;
    }
    // ------------------------------------------------

    try {
      // Endpoint remains /campaigns/run
      const response = await fetch('http://127.0.0.1:8000/campaigns/run', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        // Send the constructed payload
        body: JSON.stringify(campaignPayload),
      });

      // --- Type the response data correctly --- //
      const data: WorkflowResponse = await response.json();
      // --------------------------------------- //

      if (!response.ok) {
          // Try to parse FastAPI validation error for better message
          let detail = `HTTP error! status: ${response.status}`;
          if (data && (data as any).detail) {
              if (Array.isArray((data as any).detail)) {
                  detail = (data as any).detail.map((err: any) => `${err.loc.join('.')} - ${err.msg}`).join('; ');
              } else if (typeof (data as any).detail === 'string') {
                  detail = (data as any).detail;
              }
          } else if (data.error) {
              detail = data.error;
          }
        throw new Error(detail);
      }

      console.log("Campaign Run Response:", data);
      setResults(data); // Set the full response

      if (data.error) {
        setError(`Workflow failed: ${data.error}`);
      } else if (!data.final_state) {
        console.log("Workflow finished, but no final state data returned.");
      }
      // -------------------------------------------

    } catch (err: any) {
      console.error("Error running campaign workflow:", err);
      setError(err.message || 'Failed to run campaign workflow. Check console.');
    } finally {
      setIsLoading(false);
    }
  };

  const renderResults = () => {
    if (!results) return null;

    // --- Extract leads and vetting data safely from final_state --- //
    // Adjust based on where the final list of *enriched* profiles are stored
    // Based on our flow stopping after enrichment, they should be in `enriched_profiles`
    // The `leads` array might be empty or contain pre-enrichment data depending on exact Agent logic.
    // Let's prioritize `enriched_profiles` if they exist.
    const finalProfiles = results.final_state?.enriched_profiles || results.final_state?.leads || [];

    // Vetting results won't exist since we stopped the workflow before that step.
    // const vettingResults = results.final_state?.vetting_results || [];
    // const vettingMap = new Map<string, VettingResult>();
    // vettingResults.forEach(vetting => {
    //     vettingMap.set(vetting.podcast_id, vetting);
    // });

    // Display data directly from the enriched profile (or lead if enrichment didn't run/failed)
    const processed_podcasts = finalProfiles.map((profile: any, index: number) => { // Use 'any' for flexibility for now
        // const vetting = vettingMap.get(profile.podcast_id);
        return {
            profile: profile || { podcast_id: `unknown-${index}`, name: 'Unknown Name', description: '' },
            // vetting: vetting || null, // Vetting data won't be present
        };
    });
    // ------------------------------------------------------------ //

    return (
      <div style={{ marginTop: '30px' }}>
        <h3 style={{ borderBottom: '1px solid #eee', paddingBottom: '10px' }}>{results.message || 'Processing Complete'}</h3>
        {/* --- Adjusted Table Columns for Enriched Data (No Vetting) --- */}
        {processed_podcasts.length > 0 ? (
          <table style={{ borderCollapse: 'collapse', width: '100%', marginTop: '15px', fontSize: '14px' }}>
            <thead>
              <tr style={{ backgroundColor: '#f8f8f8', borderBottom: '2px solid #ddd' }}>
                <th style={{ padding: '10px 8px', textAlign: 'left' }}>Name</th>
                <th style={{ padding: '10px 8px', textAlign: 'left' }}>Description</th>
                <th style={{ padding: '10px 8px', textAlign: 'left' }}>Hosts</th>
                <th style={{ padding: '10px 8px', textAlign: 'left' }}>Primary Email</th>
                <th style={{ padding: '10px 8px', textAlign: 'left' }}>Twitter Followers</th>
                <th style={{ padding: '10px 8px', textAlign: 'left' }}>LinkedIn Connections</th>
                <th style={{ padding: '10px 8px', textAlign: 'left' }}>ID</th>
                <th style={{ padding: '10px 8px', textAlign: 'left' }}>RSS</th>
              </tr>
            </thead>
            <tbody>
              {processed_podcasts.map((podcast, index) => {
                const profile = podcast.profile || {};
                return (
                  <tr key={profile.podcast_id || profile.api_id || `unknown-${index}`} style={{ borderBottom: '1px solid #eee', backgroundColor: index % 2 === 0 ? '#fff' : '#fafafa' }}>
                    <td style={{ padding: '10px 8px', verticalAlign: 'top' }}>{profile.title || profile.name || 'N/A'}</td>
                    <td style={{ padding: '10px 8px', verticalAlign: 'top', maxWidth: '300px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={profile.description || ''}>{profile.description || 'N/A'}</td>
                    <td style={{ padding: '10px 8px', verticalAlign: 'top' }}>{(profile.host_names || []).join(', ') || 'N/A'}</td>
                    <td style={{ padding: '10px 8px', verticalAlign: 'top' }}>{profile.primary_email || profile.rss_owner_email || 'N/A'}</td>
                    <td style={{ padding: '10px 8px', verticalAlign: 'top', textAlign: 'right' }}>{profile.twitter_followers !== undefined ? profile.twitter_followers : 'N/A'}</td>
                    <td style={{ padding: '10px 8px', verticalAlign: 'top', textAlign: 'right' }}>{profile.linkedin_connections !== undefined ? profile.linkedin_connections : 'N/A'}</td>
                    <td style={{ padding: '10px 8px', verticalAlign: 'top', fontSize: '12px', color: '#666' }}>{profile.podcast_id || profile.api_id || 'N/A'}</td>
                    <td style={{ padding: '10px 8px', verticalAlign: 'top', fontSize: '12px', color: '#666', maxWidth: '150px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={profile.rss_feed_url || profile.rss_url || ''}>
                      {profile.rss_feed_url || profile.rss_url || 'N/A'}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        ) : (
          // Display message if no error and no leads found in state
          !results.error && <p style={{ fontStyle: 'italic', color: '#777' }}>Workflow completed. No enriched profiles found in the final state.</p>
        )}
        {/* ----------------------------------------------------------- */}
        <details style={{ marginTop: '15px' }}>
          <summary>View Raw Workflow State (Debug)</summary>
          <pre style={{backgroundColor: '#f0f0f0', padding: '10px', borderRadius: '4px', maxHeight: '300px', overflow: 'auto', fontSize: '12px'}}><code>{JSON.stringify(results.final_state || results, null, 2)}</code></pre>
        </details>
      </div>
    );
  };

  return (
    <div style={{ margin: '20px', padding: '20px', border: '1px solid #eee', borderRadius: '8px', boxShadow: '0 2px 4px rgba(0,0,0,0.1)', fontFamily: 'sans-serif' }}>
      <h2 style={{ marginTop: '0', borderBottom: '1px solid #eee', paddingBottom: '10px' }}>Run Podcast Vetting Campaign</h2>
      <p style={{ marginBottom: '25px', color: '#555' }}>Configure and run the podcast vetting workflow.</p>

      {/* --- Search Type Selection --- */}
      <div style={{ marginBottom: '20px', display: 'flex', alignItems: 'center', gap: '15px' }}>
        <label style={{ fontWeight: 'bold', minWidth: '120px' }}>Search Type:</label>
        <div>
          <label style={{ marginRight: '15px' }}>
            <input
              type="radio"
              name="searchType"
              value="topic"
              checked={searchType === 'topic'}
              onChange={() => setSearchType('topic')}
              style={{ marginRight: '5px' }}
            />
            Topic Search
          </label>
          <label>
            <input
              type="radio"
              name="searchType"
              value="related"
              checked={searchType === 'related'}
              onChange={() => setSearchType('related')}
              style={{ marginRight: '5px' }}
            />
            Related Search
          </label>
        </div>
      </div>
      {/* --------------------------- */}

      <form onSubmit={handleSubmit} style={{ marginBottom: '30px' }}>
        {/* --- Conditional Inputs for Topic Search --- */}
        {searchType === 'topic' && (
          <>
            <div style={{ display: 'flex', alignItems: 'center', gap: '15px', marginBottom: '15px', flexWrap: 'wrap' }}>
              <label htmlFor="targetAudience" style={{ fontWeight: 'bold', minWidth: '120px' }}>Target Audience:</label>
              <input
                type="text"
                id="targetAudience"
                value={targetAudience}
                onChange={(e) => setTargetAudience(e.target.value)}
                required={searchType === 'topic'}
                style={{ padding: '8px 10px', border: '1px solid #ccc', borderRadius: '4px', flexGrow: 1, minWidth: '250px' }}
                placeholder="Describe the ideal listener or podcast theme (e.g., SaaS founders, AI researchers)"
              />
            </div>
            <div style={{ display: 'flex', alignItems: 'flex-start', gap: '15px', marginBottom: '15px', flexWrap: 'wrap' }}>
              <label htmlFor="keyMessages" style={{ fontWeight: 'bold', minWidth: '120px', paddingTop: '8px' }}>Key Messages:</label>
              <textarea
                id="keyMessages"
                value={keyMessages}
                onChange={(e) => setKeyMessages(e.target.value)}
                required={searchType === 'topic'}
                style={{ padding: '8px 10px', border: '1px solid #ccc', borderRadius: '4px', flexGrow: 1, minWidth: '250px', minHeight: '80px', fontFamily: 'inherit' }}
                placeholder="Enter key points or topics, one per line"
                rows={3}
              />
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '15px', marginBottom: '15px', flexWrap: 'wrap' }}>
              <label htmlFor="tonePreferences" style={{ fontWeight: 'bold', minWidth: '120px' }}>Tone:</label>
              <input
                type="text"
                id="tonePreferences"
                value={tonePreferences}
                onChange={(e) => setTonePreferences(e.target.value)}
                required={searchType === 'topic'}
                style={{ padding: '8px 10px', border: '1px solid #ccc', borderRadius: '4px', flexGrow: 1, minWidth: '250px' }}
                placeholder="e.g., Professional, Conversational, Humorous"
              />
            </div>
            <div style={{ display: 'flex', alignItems: 'flex-start', gap: '15px', marginBottom: '20px', flexWrap: 'wrap' }}>
              <label htmlFor="additionalContext" style={{ fontWeight: 'bold', minWidth: '120px', paddingTop: '8px' }}>Additional Context:</label>
              <textarea
                id="additionalContext"
                value={additionalContext}
                onChange={(e) => setAdditionalContext(e.target.value)}
                style={{ padding: '8px 10px', border: '1px solid #ccc', borderRadius: '4px', flexGrow: 1, minWidth: '250px', minHeight: '60px', fontFamily: 'inherit' }}
                placeholder="(Optional) Any other relevant info, like campaign goals or specific angles"
                rows={2}
              />
            </div>
          </>
        )}
        {/* ------------------------------------------ */}

        {/* --- Conditional Input for Related Search --- */}
        {searchType === 'related' && (
          <div style={{ display: 'flex', alignItems: 'center', gap: '15px', marginBottom: '20px', flexWrap: 'wrap' }}>
            <label htmlFor="seedRssUrl" style={{ fontWeight: 'bold', minWidth: '120px' }}>Seed RSS URL:</label>
            <input
              type="url"
              id="seedRssUrl"
              value={seedRssUrl}
              onChange={(e) => setSeedRssUrl(e.target.value)}
              required={searchType === 'related'}
              style={{ padding: '8px 10px', border: '1px solid #ccc', borderRadius: '4px', flexGrow: 1, minWidth: '250px' }}
              placeholder="Enter the exact RSS feed URL of the seed podcast"
            />
          </div>
        )}
        {/* ----------------------------------------- */}

        <button
          type="submit"
          disabled={isLoading}
          style={{
            padding: '10px 20px',
            backgroundColor: isLoading ? '#ccc' : '#007bff',
            color: 'white',
            border: 'none',
            borderRadius: '4px',
            cursor: isLoading ? 'not-allowed' : 'pointer',
            fontSize: '16px',
            transition: 'background-color 0.2s'
          }}
        >
          {isLoading ? 'Running Workflow...' : `Run ${searchType === 'topic' ? 'Topic' : 'Related'} Search Workflow`}
        </button>
      </form>

      {isLoading && (
        <div style={{ padding: '15px', backgroundColor: '#eef', border: '1px solid #ccd', borderRadius: '4px', textAlign: 'center' }}>
          Running campaign workflow... This might take some time. Please wait.
        </div>
      )}
      {error && (
        <div style={{ padding: '15px', backgroundColor: '#fdd', border: '1px solid #fbb', color: '#800', borderRadius: '4px' }}>
          <strong>Error:</strong> {error}
        </div>
      )}

      {renderResults()}
    </div>
  );
};

export default CampaignRunner; 